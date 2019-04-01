package ipfs

import (
	"bytes"
	"context"
	"encoding/base64"
	"io/ioutil"
	"net/http"
	"time"

	ropts "gx/ipfs/QmcQ81jSyWCp1jpkQ8CMbtpXT3jK7Wg6ZtYmoyWFgBoF9c/go-libp2p-routing/options"
)

var apiRouterHTTPClient = &http.Client{
	Timeout: time.Second * 30,
}

type APIRouter struct {
	URI string
}

func (r APIRouter) PutValue(ctx context.Context, key string, value []byte, opts ...ropts.Option) error {
	req, err := http.NewRequest("PUT", r.pathForKey(key), bytes.NewBuffer(value))
	if err != nil {
		return err
	}

	_, err = apiRouterHTTPClient.Do(req)
	return err
}

func (r APIRouter) GetValue(ctx context.Context, key string, opts ...ropts.Option) ([]byte, error) {
	resp, err := apiRouterHTTPClient.Get(r.pathForKey(key))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func (r APIRouter) pathForKey(key string) string {
	return r.uri + "/" + base64.URLEncoding.EncodeToString([]byte(key))
}
