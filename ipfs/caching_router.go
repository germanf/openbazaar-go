package ipfs

import (
	"context"
	"errors"
	"gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	ci "gx/ipfs/QmPvyPwuCgJ7pDmrKDxRtsScJgBaM5h4EpRL2qQJsmXf4n/go-libp2p-crypto"
	peer "gx/ipfs/QmTRhk7cgjUf2gfQ3p2M9KPECNZEW9XUrmHcFCgog4cPgB/go-libp2p-peer"
	pstore "gx/ipfs/QmTTJcDL3gsnGDALjh2fDGg1onGRUdVgNL2hU2WEZcVrMX/go-libp2p-peerstore"
	routinghelpers "gx/ipfs/QmX3syBjwRd12qJGaKbFBWFfrBinKsaTC43ry3PsgiXCLK/go-libp2p-routing-helpers"
	routing "gx/ipfs/QmcQ81jSyWCp1jpkQ8CMbtpXT3jK7Wg6ZtYmoyWFgBoF9c/go-libp2p-routing"
	ropts "gx/ipfs/QmcQ81jSyWCp1jpkQ8CMbtpXT3jK7Wg6ZtYmoyWFgBoF9c/go-libp2p-routing/options"
	"gx/ipfs/QmfGQp6VVqdPCDyzEM6EGwMY74YPabTSEoQWHUxZuCSWj3/go-multierror"
)

var (
	ErrCachingRouterValueIsNotByteSlice = errors.New("Value is not byte slice")
)

type CachingRouter struct {
	cachingRouter routing.ValueStore
	routinghelpers.Tiered
}

func NewCachingRouter(cachingRouter routing.ValueStore, tiered routinghelpers.Tiered) routing.IpfsRouting {
	return CachingRouter{
		cachingRouter: cachingRouter,
		Tiered:        tiered,
	}
}

func (r CachingRouter) PutValue(ctx context.Context, key string, value []byte, opts ...ropts.Option) error {
	// Write to the tiered router in the background then write to the caching
	// router and return
	go r.Tiered.PutValue(ctx, key, value, opts...)
	return r.cachingRouter.PutValue(ctx, key, value, opts...)
}

func (r CachingRouter) GetValue(ctx context.Context, key string, opts ...ropts.Option) ([]byte, error) {
	// First check the cache router. If it's successful return the value otherwise
	// continue on to check the other routers.
	val, err := r.cachingRouter.GetValue(ctx, key, opts...)
	if err == nil {
		return val, nil
	}

	// Cache miss; Check tiered router
	val, err = r.Tiered.GetValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	// Write value back to caching router so it can hit next time.
	return val, r.cachingRouter.PutValue(ctx, key, val, opts...)
}

func (r CachingRouter) SearchValue(ctx context.Context, key string, opts ...ropts.Option) (<-chan []byte, error) {
	// Check caching router for value. If it's found return a closed channel with
	// just that value in it. If it's not found check the tiered router.
	val, err := r.cachingRouter.GetValue(ctx, key, opts...)
	if err == nil {
		valuesCh := make(chan ([]byte), 1)
		valuesCh <- val
		close(valuesCh)
		return valuesCh, nil
	}

	// Cache miss; check tiered router
	return r.Tiered.SearchValue(ctx, key, opts...)
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

// Delegated methods
func (r CachingRouter) GetPublicKey(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	return r.Tiered.GetPublicKey(ctx, p)
}

func (r CachingRouter) Provide(ctx context.Context, c cid.Cid, local bool) error {
	return r.Tiered.Provide(ctx, c, local)
}

func (r CachingRouter) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan pstore.PeerInfo {
	return r.Tiered.FindProvidersAsync(ctx, c, count)
}

func (r CachingRouter) FindPeer(ctx context.Context, p peer.ID) (pstore.PeerInfo, error) {
	return r.Tiered.FindPeer(ctx, p)
}
