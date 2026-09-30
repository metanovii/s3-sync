package storage

import (
	"context"
	"time"

	"golang.org/x/time/rate"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metanovii/s3-sync/internal/config"
)

// listServer answers every ListObjectsV2 with body.
func listServer(t *testing.T, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	c, err := New(context.Background(), "test", config.Provider{
		Endpoint: srv.URL, Region: "us-east-1", PathStyle: true,
		Checksum: config.ChecksumWhenRequired, AccessKey: "a", SecretKey: "b",
	}, 2, NewLimits())
	require.NoError(t, err)
	return c
}

func page(truncated, token, key string) string {
	s := `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>b</Name><EncodingType>url</EncodingType>`
	if truncated != "" {
		s += "<IsTruncated>" + truncated + "</IsTruncated>"
	}
	if token != "" {
		s += "<NextContinuationToken>" + token + "</NextContinuationToken>"
	}
	return s + "<Contents><Key>" + key + "</Key><Size>1</Size><ETag>&quot;e&quot;</ETag></Contents></ListBucketResult>"
}

func TestListRejectsBrokenPagination(t *testing.T) {
	for name, body := range map[string]string{
		"truncated without token": page("true", "", "k"),
		"no IsTruncated":          page("", "", "k"),
		"same token again":        page("true", "t1", "k"),
	} {
		t.Run(name, func(t *testing.T) {
			c := listServer(t, body)
			err := c.List(context.Background(), "b", "", func([]Object) error { return nil })
			require.ErrorIs(t, err, ErrBadListing)
		})
	}
}

func TestListDecodesKeys(t *testing.T) {
	c := listServer(t, page("false", "", "bad%01key+with%2Bplus"))
	var keys []string
	err := c.List(context.Background(), "b", "", func(objs []Object) error {
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"bad\x01key with+plus"}, keys)
}

func TestListUploadsRejectsRepeatedMarker(t *testing.T) {
	body := `<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult><Bucket>b</Bucket>` +
		`<EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>` +
		`<NextKeyMarker>%D1%8E</NextKeyMarker><NextUploadIdMarker>u1</NextUploadIdMarker>` +
		`<Upload><Key>%D1%8E</Key><UploadId>u1</UploadId></Upload></ListMultipartUploadsResult>`
	c := listServer(t, body)
	_, err := c.ListUploads(context.Background(), "b", "")
	require.ErrorIs(t, err, ErrBadListing)
}

func TestLimiterErrIsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	l := rate.NewLimiter(rate.Limit(0.0001), 1)
	require.True(t, l.Allow())
	err := limiterErr(ctx, l.Wait(ctx)) // would exceed the deadline
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, ctx.Err())
}
