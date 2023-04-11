package sordini

import (
	"io"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Context struct {
	mu            sync.Mutex
	conn          io.ReadWriter
	err           error
	correlationID int32
	clientID      *string
	req           kmsg.Request
	res           kmsg.Response
	vals          map[interface{}]interface{}
}

func (ctx *Context) Request() interface{} {
	return ctx.req
}

func (ctx *Context) Response() interface{} {
	return ctx.res
}

func (ctx *Context) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (ctx *Context) Done() <-chan struct{} {
	return nil
}

func (ctx *Context) Err() error {
	return ctx.err
}

func (ctx *Context) Value(key interface{}) interface{} {
	ctx.mu.Lock()
	if ctx.vals == nil {
		ctx.vals = make(map[interface{}]interface{})
	}
	val := ctx.vals[key]
	ctx.mu.Unlock()
	return val
}

func (ctx *Context) ResponseToBytes() ([]byte, error) {
	var dst []byte
	dst = append(dst, 0, 0, 0, 0) // reserve length
	dst = kbin.AppendInt32(dst, ctx.correlationID)

	// The flexible tags end the request header, and then begins the
	// request body.
	if ctx.res.IsFlexible() {
		var numTags uint8
		dst = append(dst, numTags)
		if numTags != 0 {
			// TODO when tags are added
		}
	}

	// Now the request body.
	dst = ctx.res.AppendTo(dst)

	// TODO: use better semantics to do this
	kbin.AppendInt32(dst[:0], int32(len(dst[4:])))
	return dst, nil
}
