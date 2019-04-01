package ipfs

import (
	"context"
	"errors"
	routinghelpers "gx/ipfs/QmX3syBjwRd12qJGaKbFBWFfrBinKsaTC43ry3PsgiXCLK/go-libp2p-routing-helpers"
	routing "gx/ipfs/QmcQ81jSyWCp1jpkQ8CMbtpXT3jK7Wg6ZtYmoyWFgBoF9c/go-libp2p-routing"
	ropts "gx/ipfs/QmcQ81jSyWCp1jpkQ8CMbtpXT3jK7Wg6ZtYmoyWFgBoF9c/go-libp2p-routing/options"
	"gx/ipfs/QmfGQp6VVqdPCDyzEM6EGwMY74YPabTSEoQWHUxZuCSWj3/go-multierror"

	record "github.com/libp2p/go-libp2p-record"
)

var (
	ErrCachingRouterValueIsNotByteSlice = errors.New("Value is not byte slice")
)

type CachingRouter struct {
	cachingRouter routing.ValueStore
	Routers       []routing.IpfsRouting
	Validator     record.Validator
}

func (r CachingRouter) PutValue(ctx context.Context, key string, value []byte, opts ...ropts.Option) error {
	go routinghelpers.Parallel{Routers: r.Routers}.PutValue(ctx, key, value, opts...)
	return r.cachingRouter.PutValue(ctx, key, value, opts...)
}

func (r CachingRouter) GetValue(ctx context.Context, key string, opts ...ropts.Option) ([]byte, error) {
	// First check the cache router. If it's successful return the value otherwise
	// continue on to check the other routers.
	val, err := r.cachingRouter.GetValue(ctx, key, opts...)
	if err == nil {
		return val, nil
	}

	// Cache miss; Check other routers
	valInt, err := r.get(ctx, func(ri routing.IpfsRouting) (interface{}, error) {
		return ri.GetValue(ctx, key, opts...)
	})
	val, ok := valInt.([]byte)
	if !ok {
		return nil, ErrCachingRouterValueIsNotByteSlice
	}

	// Write value back to caching router so it can hit next time.
	return val, r.cachingRouter.PutValue(ctx, key, val, opts...)
}

func (r CachingRouter) SearchValue(ctx context.Context, key string, opts ...ropts.Option) (<-chan []byte, error) {
	// Check caching router for value. If it's found return a closed channel with
	// just that value in it. If it's not found check other routers.
	val, err := r.cachingRouter.GetValue(ctx, key, opts...)
	if err == nil {
		valuesCh := make(chan ([]byte), 1)
		valuesCh <- val
		close(valuesCh)
		return valuesCh, nil
	}

	// Cache miss; check other routers
	return routinghelpers.Parallel{Routers: r.Routers, Validator: r.Validator}.SearchValue(ctx, key, opts...)
}

func (r CachingRouter) get(ctx context.Context, do func(routing.IpfsRouting) (interface{}, error)) (interface{}, error) {
	var errs []error
	for _, ri := range r.Routers {
		val, err := do(ri)
		switch err {
		case nil:
			return val, nil
		case routing.ErrNotFound, routing.ErrNotSupported:
			continue
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		errs = append(errs, err)
	}
	switch len(errs) {
	case 0:
		return nil, routing.ErrNotFound
	case 1:
		return nil, errs[0]
	default:
		return nil, &multierror.Error{Errors: errs}
	}
}
