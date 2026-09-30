package syncer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/metanovii/s3-sync/internal/state"
	"github.com/metanovii/s3-sync/internal/storage"
)

const (
	// multipartThreshold is the size from which objects are uploaded in parts.
	multipartThreshold = 64 << 20
	minPartSize        = 64 << 20
	maxParts           = 10000
	// attempts is how many times one object or part is tried.
	attempts = 3
)

// partSize returns the part size for an object: 64 MiB, or more when the
// object would need more than 10000 parts, rounded up to whole MiB.
func partSize(size int64) int64 {
	p := (size + maxParts - 1) / maxParts
	p = (p + 1<<20 - 1) &^ (1<<20 - 1)
	return max(p, minPartSize)
}

// retry runs fn up to attempts times. ErrPreconditionFailed and a cancelled
// context are not retried.
func retry(ctx context.Context, fn func() error) error {
	var err error
	for i := range attempts {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(i) * time.Second):
			}
		}
		err = fn()
		if err == nil || errors.Is(err, storage.ErrPreconditionFailed) || ctx.Err() != nil {
			return err
		}
	}
	return err
}

// copyObject copies one object with its metadata and returns the number of
// bytes written.
func (s *Syncer) copyObject(ctx context.Context, o state.Object, acl string) (int64, error) {
	if o.Size >= multipartThreshold {
		return s.copyMultipart(ctx, o, acl)
	}
	var n int64
	err := retry(ctx, func() error {
		r, err := s.src.Get(ctx, s.cfg.Source.Bucket, o.Key, o.ETag, "")
		if err != nil {
			return err
		}
		defer func() { _ = r.Body.Close() }()
		if err := s.dst.Put(ctx, s.cfg.Target.Bucket, s.targetKey(o.Key), r.Body, r.Size, r.Meta, acl); err != nil {
			return err
		}
		n = r.Size
		return nil
	})
	return n, err
}

// copyMultipart copies a large object part by part. Every part is a ranged
// GET with If-Match, so a part can be retried alone and all parts come from
// the same version. The upload is aborted on any failure.
func (s *Syncer) copyMultipart(ctx context.Context, o state.Object, acl string) (n int64, err error) {
	bucket, key := s.cfg.Target.Bucket, s.targetKey(o.Key)
	size := partSize(o.Size)
	count := int((o.Size + size - 1) / size)

	var uploadID string
	defer func() {
		if err == nil || uploadID == "" {
			return
		}
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if aerr := s.dst.AbortMultipart(actx, bucket, key, uploadID); aerr != nil {
			s.log.Warn().Err(aerr).Str("key", o.Key).Msg("cannot abort multipart upload")
		}
	}()

	etags := make([]string, 0, count)
	for i := range count {
		start := int64(i) * size
		end := min(start+size, o.Size) - 1
		length := end - start + 1
		byteRange := fmt.Sprintf("bytes=%d-%d", start, end)

		var etag string
		err := retry(ctx, func() error {
			r, err := s.src.Get(ctx, s.cfg.Source.Bucket, o.Key, o.ETag, byteRange)
			if err != nil {
				return err
			}
			defer func() { _ = r.Body.Close() }()
			if r.Size != length {
				return fmt.Errorf("range %s returned %d bytes, want %d", byteRange, r.Size, length)
			}
			if uploadID == "" {
				// Metadata comes with the first part.
				id, err := s.dst.CreateMultipart(ctx, bucket, key, r.Meta, acl)
				if err != nil {
					return err
				}
				uploadID = id
			}
			etag, err = s.dst.UploadPart(ctx, bucket, key, uploadID, int32(i+1), r.Body, length)
			return err
		})
		if err != nil {
			return 0, err
		}
		etags = append(etags, etag)
	}
	if err := retry(ctx, func() error {
		return s.dst.CompleteMultipart(ctx, bucket, key, uploadID, etags)
	}); err != nil {
		return 0, err
	}
	return o.Size, nil
}
