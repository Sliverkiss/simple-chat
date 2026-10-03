package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"simple-chat/internal/sse"
	"simple-chat/internal/upstream"
)

// consoleErrorClass records bounded diagnostics without formatting upstream
// messages, response bodies, request URLs, or locally supplied credentials.
func consoleErrorClass(err error) string {
	var biz *upstream.BizError
	if errors.As(err, &biz) {
		return fmt.Sprintf("biz_code=%d", biz.BizCode)
	}
	var httpErr *upstream.HTTPStatusError
	if errors.As(err, &httpErr) {
		return fmt.Sprintf("http_status=%d", httpErr.Status)
	}
	if errors.Is(err, sse.ErrContentFilter) {
		return "content_filter"
	}
	var stream *sse.StreamError
	if errors.As(err, &stream) {
		return "stream_error"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "transport"
}
