// Package storage is a thin S3 client for one provider, with the provider's
// rate limits applied to every request and body.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"

	"github.com/metanovii/s3-sync/internal/config"
)

// ErrPreconditionFailed means the object changed since it was listed.
var ErrPreconditionFailed = errors.New("object changed since listing")

// Object is one listed object.
type Object struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// Metadata is what is copied along with the object body.
type Metadata struct {
	ContentType        *string
	CacheControl       *string
	ContentEncoding    *string
	ContentDisposition *string
	ContentLanguage    *string
	Expires            *time.Time
	User               map[string]string
}

// Limits holds the rate limiters of the process. They are keyed by endpoint,
// not by provider name, so that two providers pointing at the same storage
// share one limit; the smaller configured limit wins.
type Limits struct {
	mu        sync.Mutex
	buckets   map[string]*rate.Limiter // endpoint + "/" + bucket
	bandwidth map[string]*rate.Limiter // endpoint
	rps       map[string]float64       // endpoint: requests per bucket, 0 unlimited
}

// NewLimits returns an empty set of limiters.
func NewLimits() *Limits {
	return &Limits{
		buckets:   make(map[string]*rate.Limiter),
		bandwidth: make(map[string]*rate.Limiter),
		rps:       make(map[string]float64),
	}
}

// register records the limits of a provider and returns its bandwidth
// limiter (nil: unlimited).
func (l *Limits) register(endpoint string, rl config.RateLimit) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r := rl.RequestsPerBucket; r > 0 {
		if cur, ok := l.rps[endpoint]; !ok || cur == 0 || r < cur {
			l.rps[endpoint] = r
		}
	} else if _, ok := l.rps[endpoint]; !ok {
		l.rps[endpoint] = 0
	}
	bw := int64(rl.BandwidthTotal)
	cur := l.bandwidth[endpoint]
	if bw > 0 && (cur == nil || float64(bw) < float64(cur.Limit())) {
		cur = rate.NewLimiter(rate.Limit(bw), int(min(bw, math.MaxInt32)))
		l.bandwidth[endpoint] = cur
	}
	return cur
}

func (l *Limits) bucket(endpoint, bucket string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.rps[endpoint]
	if r <= 0 {
		return nil
	}
	key := endpoint + "/" + bucket
	lim, ok := l.buckets[key]
	if !ok {
		lim = rate.NewLimiter(rate.Limit(r), max(1, int(r)))
		l.buckets[key] = lim
	}
	return lim
}

// Client talks to one provider.
type Client struct {
	Name     string
	s3       *s3.Client
	endpoint string
	limits   *Limits
	// bandwidth is looked up once all providers are registered.
	bwOnce    sync.Once
	bandwidth *rate.Limiter
}

// sdkLogger sends the AWS SDK's own messages to the process log; by default
// the SDK prints them to stderr, some once per request.
// Everything goes to debug: some SDK warnings repeat for every object.
var sdkLogger = logging.LoggerFunc(func(_ logging.Classification, format string, v ...any) {
	log.Debug().Str("component", "aws-sdk").Msgf(format, v...)
})

// Timeouts of one HTTP exchange. A pass has its own, much longer timeout.
const (
	responseHeaderTimeout = time.Minute
	readTimeout           = 2 * time.Minute
)

// New builds a client for the provider. maxConns is the number of parallel
// connections to keep open.
func New(ctx context.Context, name string, p config.Provider, maxConns int, limits *Limits) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(p.Region),
		awsconfig.WithRetryMaxAttempts(5),
		awsconfig.WithLogger(sdkLogger),
		awsconfig.WithHTTPClient(awshttp.NewBuildableClient().
			WithReadTimeout(readTimeout).
			WithTransportOptions(func(t *http.Transport) {
				t.MaxIdleConns = maxConns
				t.MaxIdleConnsPerHost = maxConns
				t.ResponseHeaderTimeout = responseHeaderTimeout
			})),
	}
	if p.Checksum == config.ChecksumWhenSupported {
		opts = append(opts,
			awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenSupported),
			awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenSupported))
	} else {
		opts = append(opts,
			awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
			awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired))
	}
	if !p.DefaultChain() {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(p.AccessKey, p.SecretKey, "")))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("provider %s: %w", name, err)
	}
	endpoint := config.NormalizeEndpoint(p.Endpoint)
	limits.register(endpoint, p.RateLimit)
	return &Client{
		Name: name,
		s3: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(p.Endpoint)
			o.UsePathStyle = p.PathStyle
		}),
		endpoint: endpoint,
		limits:   limits,
	}, nil
}

// wait blocks until the request limit of the bucket allows one more request.
func (c *Client) wait(ctx context.Context, bucket string) error {
	l := c.limits.bucket(c.endpoint, bucket)
	if l == nil {
		return ctx.Err()
	}
	return limiterErr(ctx, l.Wait(ctx))
}

// limiterErr reports a limiter that refuses to wait past the context
// deadline as the deadline itself: the operation was cut by the pass
// timeout, it did not fail.
func limiterErr(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil {
		return err
	}
	if _, ok := ctx.Deadline(); ok {
		return fmt.Errorf("%w: %v", context.DeadlineExceeded, err)
	}
	return err
}

// ErrBadListing means the provider returned a listing that cannot be
// continued. The listing must not be treated as complete: keys after it
// would count as deleted.
var ErrBadListing = errors.New("truncated listing without a usable continuation token")

// List calls fn for every page of objects under prefix. Keys are requested
// URL-encoded, so that keys with control characters survive the XML
// response.
func (c *Client) List(ctx context.Context, bucket, prefix string, fn func([]Object) error) error {
	in := &s3.ListObjectsV2Input{
		Bucket:       aws.String(bucket),
		Prefix:       aws.String(prefix),
		EncodingType: types.EncodingTypeUrl,
	}
	for {
		if err := c.wait(ctx, bucket); err != nil {
			return err
		}
		page, err := c.s3.ListObjectsV2(ctx, in)
		if err != nil {
			return fmt.Errorf("list %s/%s: %w", bucket, prefix, err)
		}
		encoded := page.EncodingType == types.EncodingTypeUrl
		objs := make([]Object, 0, len(page.Contents))
		for _, o := range page.Contents {
			key, err := decodeKey(aws.ToString(o.Key), encoded)
			if err != nil {
				return fmt.Errorf("list %s/%s: %w", bucket, prefix, err)
			}
			objs = append(objs, Object{
				Key:          key,
				Size:         aws.ToInt64(o.Size),
				ETag:         aws.ToString(o.ETag),
				LastModified: aws.ToTime(o.LastModified),
			})
		}
		if err := fn(objs); err != nil {
			return err
		}
		if page.IsTruncated == nil {
			return fmt.Errorf("list %s/%s: %w (IsTruncated missing)", bucket, prefix, ErrBadListing)
		}
		if !*page.IsTruncated {
			return nil
		}
		next := aws.ToString(page.NextContinuationToken)
		if next == "" || next == aws.ToString(in.ContinuationToken) {
			return fmt.Errorf("list %s/%s: %w", bucket, prefix, ErrBadListing)
		}
		in.ContinuationToken = aws.String(next)
	}
}

// decodeKey undoes EncodingType=url. Providers that ignore the parameter
// leave EncodingType empty in the response, and their keys are used as they
// are.
func decodeKey(key string, encoded bool) (string, error) {
	if !encoded {
		return key, nil
	}
	k, err := url.QueryUnescape(key)
	if err != nil {
		return "", fmt.Errorf("decode key %q: %w", key, err)
	}
	return k, nil
}

// GetResult is an object body with its metadata. Body is rate limited.
type GetResult struct {
	Body io.ReadCloser
	Size int64
	Meta Metadata
}

// Get reads an object, or the byte range "bytes=a-b" of it. ifMatch, when
// set, makes a changed object fail with ErrPreconditionFailed.
func (c *Client) Get(ctx context.Context, bucket, key, ifMatch, byteRange string) (*GetResult, error) {
	if err := c.wait(ctx, bucket); err != nil {
		return nil, err
	}
	in := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if ifMatch != "" {
		in.IfMatch = aws.String(ifMatch)
	}
	if byteRange != "" {
		in.Range = aws.String(byteRange)
	}
	out, err := c.s3.GetObject(ctx, in)
	if err != nil {
		if StatusCode(err) == http.StatusPreconditionFailed {
			return nil, ErrPreconditionFailed
		}
		return nil, fmt.Errorf("get %s/%s: %w", bucket, key, err)
	}
	meta := Metadata{
		ContentType:        out.ContentType,
		CacheControl:       out.CacheControl,
		ContentEncoding:    out.ContentEncoding,
		ContentDisposition: out.ContentDisposition,
		ContentLanguage:    out.ContentLanguage,
		User:               out.Metadata,
	}
	if s := aws.ToString(out.ExpiresString); s != "" {
		if t, err := http.ParseTime(s); err == nil {
			meta.Expires = &t
		} else {
			log.Warn().Str("bucket", bucket).Str("key", key).Str("expires", s).
				Msg("Expires is not an HTTP date and is not copied")
		}
	}
	return &GetResult{
		Body: readCloser{c.limit(ctx, out.Body), out.Body},
		Size: aws.ToInt64(out.ContentLength),
		Meta: meta,
	}, nil
}

// unsignedPayload lets a streamed body be sent without hashing it first; the
// SDK does this by itself only over HTTPS.
var unsignedPayload = s3.WithAPIOptions(v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)

// Put writes an object of the given size from body.
func (c *Client) Put(ctx context.Context, bucket, key string, body io.Reader, size int64, meta Metadata, acl string) error {
	if err := c.wait(ctx, bucket); err != nil {
		return err
	}
	in := &s3.PutObjectInput{
		Bucket:             aws.String(bucket),
		Key:                aws.String(key),
		Body:               c.limit(ctx, body),
		ContentLength:      aws.Int64(size),
		ContentType:        meta.ContentType,
		CacheControl:       meta.CacheControl,
		ContentEncoding:    meta.ContentEncoding,
		ContentDisposition: meta.ContentDisposition,
		ContentLanguage:    meta.ContentLanguage,
		Expires:            meta.Expires,
		Metadata:           meta.User,
	}
	if acl != "" {
		in.ACL = types.ObjectCannedACL(acl)
	}
	if _, err := c.s3.PutObject(ctx, in, unsignedPayload); err != nil {
		return fmt.Errorf("put %s/%s: %w", bucket, key, err)
	}
	return nil
}

// CreateMultipart starts a multipart upload and returns its id.
func (c *Client) CreateMultipart(ctx context.Context, bucket, key string, meta Metadata, acl string) (string, error) {
	if err := c.wait(ctx, bucket); err != nil {
		return "", err
	}
	in := &s3.CreateMultipartUploadInput{
		Bucket:             aws.String(bucket),
		Key:                aws.String(key),
		ContentType:        meta.ContentType,
		CacheControl:       meta.CacheControl,
		ContentEncoding:    meta.ContentEncoding,
		ContentDisposition: meta.ContentDisposition,
		ContentLanguage:    meta.ContentLanguage,
		Expires:            meta.Expires,
		Metadata:           meta.User,
	}
	if acl != "" {
		in.ACL = types.ObjectCannedACL(acl)
	}
	out, err := c.s3.CreateMultipartUpload(ctx, in)
	if err != nil {
		return "", fmt.Errorf("create multipart upload %s/%s: %w", bucket, key, err)
	}
	return aws.ToString(out.UploadId), nil
}

// UploadPart uploads one part and returns its ETag.
func (c *Client) UploadPart(ctx context.Context, bucket, key, uploadID string, part int32, body io.Reader, size int64) (string, error) {
	if err := c.wait(ctx, bucket); err != nil {
		return "", err
	}
	out, err := c.s3.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(part),
		Body:          c.limit(ctx, body),
		ContentLength: aws.Int64(size),
	}, unsignedPayload)
	if err != nil {
		return "", fmt.Errorf("upload part %d of %s/%s: %w", part, bucket, key, err)
	}
	return aws.ToString(out.ETag), nil
}

// CompleteMultipart finishes an upload; etags are the part ETags in order.
func (c *Client) CompleteMultipart(ctx context.Context, bucket, key, uploadID string, etags []string) error {
	if err := c.wait(ctx, bucket); err != nil {
		return err
	}
	parts := make([]types.CompletedPart, len(etags))
	for i, e := range etags {
		parts[i] = types.CompletedPart{ETag: aws.String(e), PartNumber: aws.Int32(int32(i + 1))}
	}
	_, err := c.s3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		return fmt.Errorf("complete multipart upload %s/%s: %w", bucket, key, err)
	}
	return nil
}

// AbortMultipart cancels an upload.
func (c *Client) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	if err := c.wait(ctx, bucket); err != nil {
		return err
	}
	_, err := c.s3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
	})
	if err != nil {
		return fmt.Errorf("abort multipart upload %s/%s: %w", bucket, key, err)
	}
	return nil
}

// Upload is an incomplete multipart upload.
type Upload struct {
	Key       string
	ID        string
	Initiated time.Time
}

// ListUploads returns incomplete multipart uploads under prefix.
func (c *Client) ListUploads(ctx context.Context, bucket, prefix string) ([]Upload, error) {
	var out []Upload
	in := &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix), EncodingType: types.EncodingTypeUrl}
	for {
		if err := c.wait(ctx, bucket); err != nil {
			return nil, err
		}
		page, err := c.s3.ListMultipartUploads(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("list multipart uploads %s/%s: %w", bucket, prefix, err)
		}
		encoded := page.EncodingType == types.EncodingTypeUrl
		for _, u := range page.Uploads {
			key, err := decodeKey(aws.ToString(u.Key), encoded)
			if err != nil {
				return nil, err
			}
			out = append(out, Upload{Key: key, ID: aws.ToString(u.UploadId), Initiated: aws.ToTime(u.Initiated)})
		}
		if !aws.ToBool(page.IsTruncated) {
			return out, nil
		}
		// The key marker comes back encoded too; it is sent back decoded.
		nextKey, err := decodeKey(aws.ToString(page.NextKeyMarker), encoded)
		if err != nil {
			return nil, err
		}
		nextID := aws.ToString(page.NextUploadIdMarker)
		if (nextKey == "" && nextID == "") ||
			(nextKey == aws.ToString(in.KeyMarker) && nextID == aws.ToString(in.UploadIdMarker)) {
			return nil, fmt.Errorf("list multipart uploads %s/%s: %w", bucket, prefix, ErrBadListing)
		}
		in.KeyMarker, in.UploadIdMarker = aws.String(nextKey), aws.String(nextID)
	}
}

// Delete removes an object; a missing object is not an error.
func (c *Client) Delete(ctx context.Context, bucket, key string) error {
	if err := c.wait(ctx, bucket); err != nil {
		return err
	}
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil && StatusCode(err) != http.StatusNotFound {
		return fmt.Errorf("delete %s/%s: %w", bucket, key, err)
	}
	return nil
}

// GetACL returns the canned ACL equivalent to the object's grants, or ok ==
// false when the grants do not match any canned ACL.
func (c *Client) GetACL(ctx context.Context, bucket, key string) (acl string, ok bool, err error) {
	if err := c.wait(ctx, bucket); err != nil {
		return "", false, err
	}
	out, err := c.s3.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return "", false, fmt.Errorf("get ACL %s/%s: %w", bucket, key, err)
	}
	var owner string
	if out.Owner != nil {
		owner = aws.ToString(out.Owner.ID)
	}
	acl, ok = CannedACL(owner, out.Grants)
	return acl, ok, nil
}

// PutACL sets a canned ACL on an object.
func (c *Client) PutACL(ctx context.Context, bucket, key, acl string) error {
	if err := c.wait(ctx, bucket); err != nil {
		return err
	}
	_, err := c.s3.PutObjectAcl(ctx, &s3.PutObjectAclInput{
		Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACL(acl),
	})
	if err != nil {
		return fmt.Errorf("put ACL %s/%s: %w", bucket, key, err)
	}
	return nil
}

// StatusCode returns the HTTP status of an S3 error, or 0.
func StatusCode(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// limit wraps r in the provider bandwidth limit.
func (c *Client) limit(ctx context.Context, r io.Reader) io.Reader {
	c.bwOnce.Do(func() {
		c.limits.mu.Lock()
		c.bandwidth = c.limits.bandwidth[c.endpoint]
		c.limits.mu.Unlock()
	})
	if c.bandwidth == nil || r == nil {
		return r
	}
	return &limitedReader{ctx: ctx, r: r, l: c.bandwidth}
}

type limitedReader struct {
	ctx context.Context
	r   io.Reader
	l   *rate.Limiter
}

func (lr *limitedReader) Read(p []byte) (int, error) {
	if b := lr.l.Burst(); len(p) > b {
		p = p[:b]
	}
	n, err := lr.r.Read(p)
	if n > 0 {
		if werr := lr.l.WaitN(lr.ctx, n); werr != nil {
			return n, limiterErr(lr.ctx, werr)
		}
	}
	return n, err
}

type readCloser struct {
	io.Reader
	io.Closer
}
