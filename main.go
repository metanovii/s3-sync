package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/metanovii/s3-sync/internal/config"
	"github.com/metanovii/s3-sync/internal/metrics"
	"github.com/metanovii/s3-sync/internal/state"
	"github.com/metanovii/s3-sync/internal/storage"
	"github.com/metanovii/s3-sync/internal/syncer"
)

var version = "dev"

func main() {
	app := kingpin.New("s3-sync", "One-way 1:1 mirror between S3-compatible buckets.")
	app.Version(version)
	app.HelpFlag.Short('h')
	configPath := app.Flag("config", "Path to the configuration file.").Short('c').Default("config.yaml").String()
	debug := app.Flag("debug", "Show debug logs.").Bool()
	logFormat := app.Flag("logFormat", "Log format to use.").Default("console").Enum("json", "console")

	runCmd := app.Command("run", "Run all syncs until stopped.")
	dryRun := runCmd.Flag("dry-run", "Run one pass of every sync on a copy of the state, log what would change, change nothing.").Bool()

	validateCmd := app.Command("validate", "Check the configuration and exit.")
	noEnv := validateCmd.Flag("no-env", `Treat unset environment variables as "0", to check a configuration without its secrets (in CI).`).Bool()

	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if *logFormat == "console" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	lookupEnv := os.LookupEnv
	if cmd == validateCmd.FullCommand() && *noEnv {
		lookupEnv = config.LookupEnvOrZero
	}
	cfg, err := config.Load(*configPath, lookupEnv)
	if err != nil {
		log.Fatal().Err(err).Msg("invalid configuration")
	}

	switch cmd {
	case validateCmd.FullCommand():
		log.Info().Int("providers", len(cfg.Providers)).Int("syncs", len(cfg.Syncs)).Msg("configuration is valid")
	case runCmd.FullCommand():
		if err := run(cfg, *dryRun); err != nil {
			log.Fatal().Err(err).Msg("run failed")
		}
	}
}

func run(cfg *config.Config, dryRun bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var db *state.DB
	var err error
	if dryRun {
		db, err = state.OpenCopy(cfg.State.Path)
	} else {
		db, err = state.Open(cfg.State.Path)
	}
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Error().Err(err).Msg("cannot close state database")
		}
	}()

	limits := storage.NewLimits()
	clients := make(map[string]*storage.Client)
	client := func(name string) (*storage.Client, error) {
		if c, ok := clients[name]; ok {
			return c, nil
		}
		c, err := storage.New(ctx, name, cfg.Providers[name], 2*cfg.Workers, limits)
		if err != nil {
			return nil, err
		}
		clients[name] = c
		return c, nil
	}

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	pool := syncer.NewPool(cfg.Workers)
	syncers := make([]*syncer.Syncer, 0, len(cfg.Syncs))
	for _, sc := range cfg.Syncs {
		src, err := client(sc.Source.Provider)
		if err != nil {
			return err
		}
		dst, err := client(sc.Target.Provider)
		if err != nil {
			return err
		}
		syncers = append(syncers, syncer.New(sc, src, dst, db, pool, m, dryRun))
	}

	var srv *metrics.Server
	if !dryRun {
		srv = metrics.NewServer(cfg.Metrics.Listen, reg)
		go func() {
			if err := srv.ListenAndServe(); err != nil {
				log.Error().Err(err).Str("listen", cfg.Metrics.Listen).Msg("metrics server failed")
				stop()
			}
		}()
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		}()
	}

	for i, s := range syncers {
		sc := cfg.Syncs[i]
		err := s.Prepare(ctx,
			config.NormalizeEndpoint(cfg.Providers[sc.Source.Provider].Endpoint),
			config.NormalizeEndpoint(cfg.Providers[sc.Target.Provider].Endpoint))
		if err != nil {
			if ctx.Err() != nil {
				return nil // stopped during start-up
			}
			return fmt.Errorf("sync %s: %w", s.ID, err)
		}
	}

	if dryRun {
		return dryRunPasses(ctx, cfg, syncers)
	}

	srv.SetReady()
	log.Info().Int("syncs", len(syncers)).Int("workers", cfg.Workers).Str("version", version).Msg("started")
	var wg sync.WaitGroup
	for _, s := range syncers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Run(ctx)
		}()
	}
	wg.Wait()
	log.Info().Msg("stopped")
	return nil
}

// dryRunPasses runs one pass of every sync and logs what it would do.
func dryRunPasses(ctx context.Context, cfg *config.Config, syncers []*syncer.Syncer) error {
	var wg sync.WaitGroup
	errs := make([]error, len(syncers))
	for i, s := range syncers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Syncs[i].Timeout))
			defer cancel()
			res, err := s.Pass(pctx)
			if err != nil {
				if ctx.Err() != nil {
					log.Warn().Str("sync", s.ID).Msg("dry run interrupted")
					return
				}
				errs[i] = fmt.Errorf("sync %s: %w", s.ID, err)
				return
			}
			log.Info().Str("sync", s.ID).
				Int64("listed", res.Listed).Int64("would_copy", res.Copied).Int64("bytes", res.Bytes).
				Int64("would_adopt", res.Adopted).Int64("would_delete", res.Deleted).Int64("held", res.Held).
				Int64("failed", res.Failed).Msg("dry run finished")
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}
