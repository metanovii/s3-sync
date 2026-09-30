package main

import (
	"errors"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/metanovii/s3-sync/internal/config"
)

var version = "dev"

var errNotImplemented = errors.New("not implemented yet")

func main() {
	app := kingpin.New("s3-sync", "One-way 1:1 mirror between S3-compatible buckets.")
	app.Version(version)
	app.HelpFlag.Short('h')
	configPath := app.Flag("config", "Path to the configuration file.").Short('c').Default("config.yaml").String()
	debug := app.Flag("debug", "Show debug logs.").Bool()
	logFormat := app.Flag("logFormat", "Log format to use.").Default("console").Enum("json", "console")

	runCmd := app.Command("run", "Run all syncs.")
	dryRun := runCmd.Flag("dry-run", "List and compare, log what would change, change nothing.").Bool()

	validateCmd := app.Command("validate", "Check the configuration and exit.")
	noEnv := validateCmd.Flag("no-env", `Treat unset environment variables as "0", to check a configuration without its secrets (in CI).`).Bool()

	confirmCmd := app.Command("confirm-delete", "Execute deletions held by the delete guards.")
	confirmSource := confirmCmd.Arg("source", "Source location, as in the configuration.").Required().String()
	confirmTarget := confirmCmd.Arg("target", "Target location, as in the configuration.").Required().String()
	confirmCount := confirmCmd.Flag("count", "Expected number of held deletions.").Required().Int()

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
		return
	case runCmd.FullCommand():
		_ = dryRun
		err = errNotImplemented
	case confirmCmd.FullCommand():
		_, _, _ = confirmSource, confirmTarget, confirmCount
		err = errNotImplemented
	}
	if err != nil {
		log.Fatal().Err(err).Str("command", cmd).Msg("command failed")
	}
}
