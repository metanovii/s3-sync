// Package syncer runs passes of one sync: list the source, copy what changed,
// delete what vanished.
package syncer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/metanovii/s3-sync/internal/config"
	"github.com/metanovii/s3-sync/internal/metrics"
	"github.com/metanovii/s3-sync/internal/state"
	"github.com/metanovii/s3-sync/internal/storage"
)

// probeDir is where start-up probes are written, inside the target prefix.
const probeDir = ".s3-sync-probe/"

// staleUpload is the age after which an incomplete multipart upload under
// the target prefix is aborted on start.
const staleUpload = 24 * time.Hour

// Pool limits the number of copy and delete operations of the process.
type Pool chan struct{}

// NewPool returns a pool of n workers.
func NewPool(n int) Pool { return make(Pool, n) }

// Syncer runs passes of one sync.
type Syncer struct {
	ID       string
	cfg      config.Sync
	src, dst *storage.Client
	db       *state.DB
	pool     Pool
	dryRun   bool
	log      zerolog.Logger
	now      func() time.Time

	lastSuccess, passDuration, objects, pending, held, unexpected prometheus.Gauge
	lastFailed                                                    prometheus.Gauge
	passOK, passFailed                                            prometheus.Counter
	copied, copiedBytes, adopted, skipped, deleted, aclNotCopied  prometheus.Counter
	recopied                                                      prometheus.Counter
	errors                                                        *prometheus.CounterVec

	// Consecutive 412 answers per key: prev412 of the previous pass,
	// cur412 of the current one. Keys without a 412 drop out.
	mu              sync.Mutex
	prev412, cur412 map[string]int
}

// New builds a syncer. With dryRun it never writes to the target.
func New(cfg config.Sync, src, dst *storage.Client, db *state.DB, pool Pool, m *metrics.Metrics, dryRun bool) *Syncer {
	l := prometheus.Labels{"source": cfg.Source.String(), "target": cfg.Target.String()}
	s := &Syncer{
		ID:     cfg.ID(),
		cfg:    cfg,
		src:    src,
		dst:    dst,
		db:     db,
		pool:   pool,
		dryRun: dryRun,
		log:    log.With().Str("source", cfg.Source.String()).Str("target", cfg.Target.String()).Logger(),
		now:    time.Now,

		lastSuccess:  m.LastSuccess.With(l),
		passDuration: m.PassDuration.With(l),
		objects:      m.Objects.With(l),
		pending:      m.DeletionsPending.With(l),
		held:         m.DeletionsHeld.With(l),
		unexpected:   m.TargetUnexpected.With(l),
		passOK:       m.Passes.MustCurryWith(l).WithLabelValues("success"),
		passFailed:   m.Passes.MustCurryWith(l).WithLabelValues("failure"),
		copied:       m.CopiedObjects.With(l),
		copiedBytes:  m.CopiedBytes.With(l),
		adopted:      m.AdoptedObjects.With(l),
		skipped:      m.SkippedObjects.With(l),
		deleted:      m.DeletedObjects.With(l),
		aclNotCopied: m.ACLNotCopied.With(l),
		recopied:     m.FullCheckRecopied.With(l),
		errors:       m.Errors.MustCurryWith(l),
		lastFailed:   m.LastPassFailed.With(l),

		cur412: make(map[string]int),
	}
	return s
}

func (s *Syncer) targetKey(sourceKey string) string {
	return s.cfg.Target.Prefix + strings.TrimPrefix(sourceKey, s.cfg.Source.Prefix)
}

func (s *Syncer) sourceKey(targetKey string) string {
	return s.cfg.Source.Prefix + strings.TrimPrefix(targetKey, s.cfg.Target.Prefix)
}

// skipKey reports keys that are never copied: the prefix itself (it would map
// to an empty key) and probes.
func skipKey(key, prefix string) bool {
	return key == prefix || strings.HasPrefix(key, prefix+probeDir)
}

// Prepare registers the sync in state, cleans up after crashes and checks
// that the target accepts the configured ACL.
func (s *Syncer) Prepare(ctx context.Context, sourceEndpoint, targetEndpoint string) error {
	reset, err := s.db.EnsureSync(ctx, s.ID, sourceEndpoint, targetEndpoint)
	if err != nil {
		return err
	}
	if reset {
		s.log.Warn().Msg("provider endpoint changed, state of this sync dropped; the next pass starts from scratch")
	}
	if s.dryRun {
		return nil
	}

	var probes []string
	err = s.dst.List(ctx, s.cfg.Target.Bucket, s.cfg.Target.Prefix+probeDir, func(objs []storage.Object) error {
		for _, o := range objs {
			probes = append(probes, o.Key)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, k := range probes {
		if err := s.dst.Delete(ctx, s.cfg.Target.Bucket, k); err != nil {
			return err
		}
	}

	uploads, err := s.dst.ListUploads(ctx, s.cfg.Target.Bucket, s.cfg.Target.Prefix)
	if err != nil {
		// Not every provider implements ListMultipartUploads.
		s.log.Warn().Err(err).Msg("cannot list incomplete multipart uploads, skipping their cleanup")
	}
	for _, u := range uploads {
		if s.now().Sub(u.Initiated) < staleUpload {
			continue
		}
		if err := s.dst.AbortMultipart(ctx, s.cfg.Target.Bucket, u.Key, u.ID); err != nil {
			s.log.Warn().Err(err).Str("key", u.Key).Msg("cannot abort stale multipart upload")
			continue
		}
		s.log.Info().Str("key", u.Key).Time("initiated", u.Initiated).Msg("aborted stale multipart upload")
	}

	if err := s.probeKeys(ctx); err != nil {
		return fmt.Errorf("key probe: %w", err)
	}
	if s.cfg.ACL != config.ACLSkip {
		if err := s.probeACL(ctx); err != nil {
			return fmt.Errorf("ACL probe: %w", err)
		}
	}
	return nil
}

// probeKey is a key suffix with the characters that URL-encoded listings
// are known to get wrong.
const probeKey = " a+b%20c"

// probeKeys writes an object whose key has a space, '+' and '%' to the
// target, and checks that the listing returns the key unchanged: a provider
// that decodes listings differently would make every such key look missing.
func (s *Syncer) probeKeys(ctx context.Context) error {
	key := s.cfg.Target.Prefix + probeDir + randomHex() + probeKey
	if err := s.dst.Put(ctx, s.cfg.Target.Bucket, key, strings.NewReader(""), 0, storage.Metadata{}, ""); err != nil {
		return err
	}
	defer func() {
		if err := s.dst.Delete(context.WithoutCancel(ctx), s.cfg.Target.Bucket, key); err != nil {
			s.log.Warn().Err(err).Str("key", key).Msg("cannot delete probe object")
		}
	}()
	var keys []string
	err := s.dst.List(ctx, s.cfg.Target.Bucket, s.cfg.Target.Prefix+probeDir, func(objs []storage.Object) error {
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, k := range keys {
		if k == key {
			return nil
		}
	}
	return fmt.Errorf("target lists the key %q as one of %q; keys with spaces, '+' or '%%' would not be mirrored correctly", key, keys)
}

func randomHex() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// probeACL writes an object with an ACL to the target, reads the ACL back and
// deletes the object. With acl: copy it also reads one source ACL.
func (s *Syncer) probeACL(ctx context.Context) error {
	want := s.cfg.ACL
	if want == config.ACLCopy {
		want = "public-read"
		var first string
		errStop := errors.New("stop")
		err := s.src.List(ctx, s.cfg.Source.Bucket, s.cfg.Source.Prefix, func(objs []storage.Object) error {
			for _, o := range objs {
				if !skipKey(o.Key, s.cfg.Source.Prefix) {
					first = o.Key
					return errStop
				}
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			return err
		}
		if first != "" {
			if _, _, err := s.src.GetACL(ctx, s.cfg.Source.Bucket, first); err != nil {
				return fmt.Errorf("source does not return object ACL: %w", err)
			}
		}
	}

	key := s.cfg.Target.Prefix + probeDir + randomHex()
	body := strings.NewReader("s3-sync probe")
	if err := s.dst.Put(ctx, s.cfg.Target.Bucket, key, body, body.Size(), storage.Metadata{}, want); err != nil {
		return fmt.Errorf("target rejects object ACL %q: %w", want, err)
	}
	got, ok, err := s.dst.GetACL(ctx, s.cfg.Target.Bucket, key)
	if derr := s.dst.Delete(context.WithoutCancel(ctx), s.cfg.Target.Bucket, key); derr != nil {
		s.log.Warn().Err(derr).Str("key", key).Msg("cannot delete probe object")
	}
	if err != nil {
		return err
	}
	if recognisable(want) && (!ok || got != want) {
		return fmt.Errorf("target ignored object ACL %q (reads back as %q)", want, got)
	}
	return nil
}

// recognisable reports canned ACLs that CannedACL can read back.
func recognisable(acl string) bool {
	switch acl {
	case "private", "public-read", "public-read-write", "authenticated-read":
		return true
	}
	return false
}

// Result sums up one pass.
type Result struct {
	Listed, Changed, Copied, Adopted, Skipped, Failed int64
	Bytes                                             int64
	Due, Deleted, Held                                int64
	ACLNotCopied                                      int64
	// Interrupted counts operations cut by the pass timeout or a stop.
	Interrupted int64
	// StuckETag counts objects answering 412 on 3 passes in a row.
	StuckETag int64
	FullCheck bool
}

// maxErrorLogs is how many object errors a pass logs one by one; the rest
// only count, so that a failing target does not write a line per object.
const maxErrorLogs = 10

func (r *Result) add(p *int64, n int64) { atomic.AddInt64(p, n) }

// Pass runs one pass. A pass fails when listing the source fails; failed
// copies are counted and retried on the next pass.
func (s *Syncer) Pass(ctx context.Context) (res *Result, err error) {
	start := s.now()
	res = &Result{}
	defer func() {
		s.passDuration.Set(s.now().Sub(start).Seconds())
		s.lastFailed.Set(float64(res.Failed))
		if res.ACLNotCopied > 0 {
			s.log.Warn().Int64("objects", res.ACLNotCopied).
				Msg("source ACL of these objects match no canned ACL; copied without ACL (see debug log for keys)")
		}
		if res.StuckETag > 0 {
			s.log.Warn().Int64("objects", res.StuckETag).
				Msg("If-Match fails on 3 passes in a row although these objects are listed with that ETag; " +
					"the provider may compare ETags differently (see debug log for keys)")
		}
		if errors.Is(err, context.Canceled) {
			return // stopped, not failed
		}
		if err != nil {
			s.passFailed.Inc()
			s.errors.WithLabelValues("pass").Inc()
			return
		}
		s.passOK.Inc()
		s.lastSuccess.Set(float64(s.now().Unix()))
	}()

	s.mu.Lock()
	s.prev412, s.cur412 = s.cur412, make(map[string]int)
	s.mu.Unlock()

	info, err := s.db.GetSync(ctx, s.ID)
	if err != nil {
		return res, err
	}
	pass, err := s.db.BeginPass(ctx, s.ID)
	if err != nil {
		return res, err
	}
	// A pass is a first pass until one pass has listed the target to the
	// end, so an interrupted first pass does not turn into copying
	// everything again.
	firstPass := info.LastFullCheck.IsZero()
	res.FullCheck = !firstPass && s.cfg.FullCheck > 0 &&
		s.now().Sub(info.LastFullCheck) >= time.Duration(s.cfg.FullCheck)

	if firstPass || res.FullCheck {
		if err := s.listTarget(ctx); err != nil {
			return res, err
		}
	}
	if res.FullCheck {
		dropped, unexpected, err := s.db.CompareTarget(ctx, s.ID)
		if err != nil {
			return res, err
		}
		s.recopied.Add(float64(dropped))
		s.unexpected.Set(float64(unexpected))
		if dropped > 0 || unexpected > 0 {
			s.log.Warn().Int64("recopy", dropped).Int64("unexpected", unexpected).Msg("full check found differences")
		}
	}

	var wg sync.WaitGroup
	listErr := s.src.List(ctx, s.cfg.Source.Bucket, s.cfg.Source.Prefix, func(objs []storage.Object) error {
		listed := make([]state.Object, 0, len(objs))
		for _, o := range objs {
			if skipKey(o.Key, s.cfg.Source.Prefix) {
				continue
			}
			listed = append(listed, state.Object{Key: o.Key, Size: o.Size, ETag: o.ETag, LastModified: o.LastModified})
		}
		res.add(&res.Listed, int64(len(listed)))
		changed, err := s.db.MarkSeen(ctx, s.ID, pass, listed)
		if err != nil {
			return err
		}
		res.add(&res.Changed, int64(len(changed)))

		var found map[string]state.TargetObject
		if firstPass && len(changed) > 0 {
			keys := make([]string, len(changed))
			for i, o := range changed {
				keys[i] = o.Key
			}
			if found, err = s.db.TargetObjects(ctx, s.ID, keys); err != nil {
				return err
			}
		}
		for _, o := range changed {
			// A target copy counts as this version when it has the same size
			// and was written after the source object.
			t, ok := found[o.Key]
			adopt := ok && t.Size == o.Size && t.LastModified.Unix() >= o.LastModified.Unix()
			if err := s.submit(ctx, &wg, func() { s.handle(ctx, pass, o, adopt, res) }); err != nil {
				return err
			}
		}
		return nil
	})
	wg.Wait()
	if listErr != nil {
		return res, listErr
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}

	if err := s.db.MarkMissing(ctx, s.ID, pass, s.now()); err != nil {
		return res, err
	}
	if err := s.deleteMissing(ctx, res); err != nil {
		return res, err
	}
	if res.FullCheck && s.cfg.ACL == config.ACLCopy {
		if err := s.recheckACL(ctx, res); err != nil {
			return res, err
		}
	}
	if (firstPass || res.FullCheck) && !s.dryRun {
		if err := s.db.ClearTargetKeys(ctx, s.ID); err != nil {
			return res, err
		}
		if err := s.db.SetLastFullCheck(ctx, s.ID, start); err != nil {
			return res, err
		}
	}
	if !s.dryRun {
		if err := s.db.SetLastSuccess(ctx, s.ID, s.now()); err != nil {
			return res, err
		}
	}
	return res, nil
}

// submit runs fn on the pool, blocking while the pool is full.
func (s *Syncer) submit(ctx context.Context, wg *sync.WaitGroup, fn func()) error {
	select {
	case s.pool <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	wg.Add(1)
	go func() {
		defer func() {
			<-s.pool
			wg.Done()
		}()
		fn()
	}()
	return nil
}

// listTarget stores the target listing, with keys mapped to source keys.
func (s *Syncer) listTarget(ctx context.Context) error {
	if err := s.db.ClearTargetKeys(ctx, s.ID); err != nil {
		return err
	}
	return s.dst.List(ctx, s.cfg.Target.Bucket, s.cfg.Target.Prefix, func(objs []storage.Object) error {
		keys := make([]state.TargetObject, 0, len(objs))
		for _, o := range objs {
			if skipKey(o.Key, s.cfg.Target.Prefix) {
				continue
			}
			keys = append(keys, state.TargetObject{Key: s.sourceKey(o.Key), Size: o.Size, LastModified: o.LastModified})
		}
		return s.db.AddTargetKeys(ctx, s.ID, keys)
	})
}

// handle copies one changed object, or records it without copying when the
// first pass found it already in the target.
func (s *Syncer) handle(ctx context.Context, pass int64, o state.Object, adopt bool, res *Result) {
	l := s.log.With().Str("key", o.Key).Logger()
	acl, known, err := s.objectACL(ctx, o.Key)
	if err != nil {
		s.fail(ctx, res, l, "get_acl", err, "cannot read source ACL")
		return
	}
	if !known {
		res.add(&res.ACLNotCopied, 1)
		s.aclNotCopied.Inc()
	}

	if s.dryRun {
		if adopt {
			res.add(&res.Adopted, 1)
			l.Info().Msg("dry run: would record as already copied")
		} else {
			res.add(&res.Copied, 1)
			res.add(&res.Bytes, o.Size)
			l.Info().Int64("size", o.Size).Msg("dry run: would copy")
		}
		return
	}

	if adopt {
		if acl != "" {
			if err := s.dst.PutACL(ctx, s.cfg.Target.Bucket, s.targetKey(o.Key), acl); err != nil {
				s.fail(ctx, res, l, "put_acl", err, "cannot set target ACL")
				return
			}
		}
		o.ACL = acl
		if err := s.db.PutObject(ctx, s.ID, pass, o, s.now()); err != nil {
			s.fail(ctx, res, l, "state", err, "cannot record object")
			return
		}
		res.add(&res.Adopted, 1)
		s.adopted.Inc()
		return
	}

	n, err := s.copyObject(ctx, o, acl)
	switch {
	case errors.Is(err, storage.ErrPreconditionFailed):
		res.add(&res.Skipped, 1)
		s.skipped.Inc()
		n := s.count412(o.Key)
		if n >= 3 {
			res.add(&res.StuckETag, 1)
		}
		l.Debug().Int("passes", n).Str("etag", o.ETag).Msg("object changed during copying, left for the next pass")
		return
	case err != nil:
		cut := errors.Is(ctx.Err(), context.DeadlineExceeded) || (ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded))
		if cut && o.Size >= multipartThreshold {
			l.Warn().Int64("size", o.Size).Dur("timeout", time.Duration(s.cfg.Timeout)).
				Msg("object did not fit into the pass timeout and will start over next pass; raise timeout")
		}
		msg := "copy failed"
		if storage.StatusCode(err) == http.StatusNotFound {
			msg = "listed object not found in the source; if this repeats, the provider may encode keys in listings differently"
		}
		s.fail(ctx, res, l, "copy", err, msg)
		return
	}
	o.ACL = acl
	if err := s.db.PutObject(ctx, s.ID, pass, o, s.now()); err != nil {
		s.fail(ctx, res, l, "state", err, "cannot record copied object")
		return
	}
	res.add(&res.Copied, 1)
	res.add(&res.Bytes, n)
	s.copied.Inc()
	s.copiedBytes.Add(float64(n))
	l.Debug().Int64("size", n).Msg("copied")
}

// objectACL returns the canned ACL to set on the target copy, "" for none.
// known is false when the source grants match no canned ACL.
func (s *Syncer) objectACL(ctx context.Context, key string) (acl string, known bool, err error) {
	switch s.cfg.ACL {
	case config.ACLSkip:
		return "", true, nil
	case config.ACLCopy:
		acl, ok, err := s.src.GetACL(ctx, s.cfg.Source.Bucket, key)
		if err != nil {
			return "", false, err
		}
		if !ok {
			s.log.Debug().Str("key", key).Msg("source ACL matches no canned ACL")
			return "", false, nil
		}
		return acl, true, nil
	default:
		return s.cfg.ACL, true, nil
	}
}

// fail counts a failed object and logs it, up to maxErrorLogs per pass. An
// operation cut by the pass timeout or a stop is counted as interrupted.
func (s *Syncer) fail(ctx context.Context, res *Result, l zerolog.Logger, operation string, err error, msg string) {
	if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
		atomic.AddInt64(&res.Interrupted, 1)
		return
	}
	n := atomic.AddInt64(&res.Failed, 1)
	s.errors.WithLabelValues(operation).Inc()
	switch {
	case n <= maxErrorLogs:
		l.Error().Err(err).Str("operation", operation).Msg(msg)
	case n == maxErrorLogs+1:
		s.log.Error().Msg("more objects fail in this pass; only their number is logged (failed in \"pass finished\")")
	}
}

// count412 records a 412 answer and returns on how many consecutive passes
// the key got one.
func (s *Syncer) count412(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.prev412[key] + 1
	s.cur412[key] = n
	return n
}

// deleteMissing deletes target copies of keys missing for delete.delay,
// unless there are more of them than the guards allow.
func (s *Syncer) deleteMissing(ctx context.Context, res *Result) error {
	before := s.now().Add(-time.Duration(s.cfg.Delete.Delay))
	pending, err := s.db.CountMissing(ctx, s.ID, before)
	if err != nil {
		return err
	}
	s.pending.Set(float64(pending))
	total, err := s.db.CountObjects(ctx, s.ID)
	if err != nil {
		return err
	}
	defer func() { s.objects.Set(float64(total - atomic.LoadInt64(&res.Deleted))) }()
	if !s.cfg.Delete.Enabled {
		s.held.Set(0)
		return nil
	}
	due, err := s.db.Due(ctx, s.ID, before)
	if err != nil {
		return err
	}
	res.Due = int64(len(due))
	if Held(res.Due, total, s.cfg.Delete) {
		res.Held = res.Due
		s.held.Set(float64(res.Due))
		s.log.Warn().Int64("due", res.Due).Int64("objects", total).
			Int("max_count", s.cfg.Delete.MaxCount).Float64("max_fraction", s.cfg.Delete.MaxFraction).
			Msg("deletions held: more objects vanished from the source than the delete limits allow; " +
				"check the source, then raise delete.max_count or delete.max_fraction to let them run")
		return nil
	}
	s.held.Set(0)

	var wg sync.WaitGroup
	for _, key := range due {
		if s.dryRun {
			res.Deleted++
			s.log.Info().Str("key", key).Msg("dry run: would delete")
			continue
		}
		err := s.submit(ctx, &wg, func() {
			l := s.log.With().Str("key", key).Logger()
			if err := s.dst.Delete(ctx, s.cfg.Target.Bucket, s.targetKey(key)); err != nil {
				s.fail(ctx, res, l, "delete", err, "delete failed")
				return
			}
			if err := s.db.DeleteObject(ctx, s.ID, key); err != nil {
				s.fail(ctx, res, l, "state", err, "cannot drop record of deleted object")
				return
			}
			res.add(&res.Deleted, 1)
			s.deleted.Inc()
		})
		if err != nil {
			break
		}
	}
	wg.Wait()
	return ctx.Err()
}

// Held reports whether due deletions exceed the limits: more than max_count,
// or, for syncs with at least min_count objects, more than max_fraction of
// them. A zero limit is no limit.
func Held(due, total int64, d config.Delete) bool {
	if due == 0 {
		return false
	}
	if d.MaxCount > 0 && due > int64(d.MaxCount) {
		return true
	}
	return d.MaxFraction > 0 && total >= int64(d.MinCount) && float64(due) > d.MaxFraction*float64(total)
}

// recheckACL reads every source ACL again and applies changes.
func (s *Syncer) recheckACL(ctx context.Context, res *Result) error {
	var wg sync.WaitGroup
	after := ""
	for {
		objs, err := s.db.ListObjects(ctx, s.ID, after, 1000)
		if err != nil {
			wg.Wait()
			return err
		}
		if len(objs) == 0 {
			break
		}
		for _, o := range objs {
			err := s.submit(ctx, &wg, func() {
				l := s.log.With().Str("key", o.Key).Logger()
				acl, known, err := s.objectACL(ctx, o.Key)
				if err != nil {
					s.fail(ctx, res, l, "get_acl", err, "cannot read source ACL")
					return
				}
				if !known {
					res.add(&res.ACLNotCopied, 1)
				}
				if acl == "" || acl == o.ACL {
					return
				}
				if s.dryRun {
					s.log.Info().Str("key", o.Key).Str("acl", acl).Msg("dry run: would change target ACL")
					return
				}
				if err := s.dst.PutACL(ctx, s.cfg.Target.Bucket, s.targetKey(o.Key), acl); err != nil {
					s.fail(ctx, res, l, "put_acl", err, "cannot set target ACL")
					return
				}
				if err := s.db.SetACL(ctx, s.ID, o.Key, acl); err != nil {
					s.fail(ctx, res, l, "state", err, "cannot record ACL")
				}
			})
			if err != nil {
				wg.Wait()
				return err
			}
		}
		after = objs[len(objs)-1].Key
	}
	wg.Wait()
	return nil
}

// Run runs passes until ctx is cancelled: each pass is limited by timeout
// and the next starts interval after the previous one ended.
func (s *Syncer) Run(ctx context.Context) {
	for {
		pctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout))
		res, err := s.Pass(pctx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		ev := s.log.Info()
		if err != nil {
			ev = s.log.Error().Err(err)
		}
		ev.Int64("listed", res.Listed).Int64("copied", res.Copied).Int64("bytes", res.Bytes).
			Int64("adopted", res.Adopted).Int64("skipped", res.Skipped).Int64("failed", res.Failed).
			Int64("interrupted", res.Interrupted).
			Int64("deleted", res.Deleted).Int64("held", res.Held).Bool("full_check", res.FullCheck).
			Msg("pass finished")
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(s.cfg.Interval)):
		}
	}
}
