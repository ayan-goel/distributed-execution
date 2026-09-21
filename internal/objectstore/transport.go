package objectstore

import (
	"io"
	"net/http"
	"net/url"
)

type guardedTransport struct {
	base     http.RoundTripper
	origin   url.URL
	maxBytes int64
}

func (t guardedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	// SDK endpoint resolution must stay on the configured origin. This includes
	// region redirects and prevents signed server requests reaching another host.
	if request.URL.Scheme != t.origin.Scheme || request.URL.Host != t.origin.Host || request.URL.User != nil {
		return nil, ErrUnavailable
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	limit := int64(64 << 10)
	if request.Method == http.MethodGet && request.URL.Query().Has("versionId") && response.StatusCode == http.StatusOK {
		limit = t.maxBytes + 1
	}
	// Bound XML/error bodies before SDK decoding and object streams before any
	// caller reads them. A bad backend cannot allocate unbounded response memory.
	response.Body = &boundedBody{Reader: io.LimitReader(response.Body, limit), Closer: response.Body}
	return response, nil
}

type boundedBody struct {
	io.Reader
	io.Closer
}
