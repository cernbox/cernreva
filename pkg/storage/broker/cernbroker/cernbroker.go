package cernbroker

import (
	"context"
	"strings"

	"github.com/cernbox/reva/pkg/log"
	"github.com/cernbox/reva/pkg/storage/broker/registry"
	"github.com/cernbox/reva/pkg/user"

	"github.com/cernbox/reva/pkg/storage"
	"github.com/mitchellh/mapstructure"
)

func init() {
	registry.Register("cernbroker", New)
}

var logger = log.New("cernbroker")

type config struct {
	Rules      map[string]string `mapstructure:"rules"`
	HomeMap    map[string]string `mapstructure:"home_map"`
	ProjectMap map[string]string `mapstructure:"project_map"`
}

type broker struct {
	conf *config
}

// New returns an implementation to of the storage.FS interface that talk to
// a local filesystem.
func New(m map[string]interface{}) (storage.Broker, error) {
	c := &config{}
	if err := mapstructure.Decode(m, c); err != nil {
		return nil, err
	}
	return &broker{conf: c}, nil
}

func (b *broker) getAllProviders(ctx context.Context) ([]*storage.ProviderInfo, error) {
	u, ok := user.ContextGetUser(ctx)
	if !ok {
		return nil, userContextRequiredError("no user context")
	}

	// load direct addressable providers.
	providers := []*storage.ProviderInfo{}
	for k, v := range b.conf.Rules {
		providers = append(providers, &storage.ProviderInfo{
			Endpoint:  v,
			MountPath: k,
		})
	}

	// load user-based /home providers.
	letter := string(u.Username[0])
	endpoint, ok := b.conf.HomeMap[letter]
	provider := &storage.ProviderInfo{
		Endpoint:  endpoint,
		MountPath: "/home",
	}
	providers = append(providers, provider)
	return providers, nil

}
func (b *broker) ListProviders(ctx context.Context) ([]*storage.ProviderInfo, error) {
	return b.getAllProviders(ctx)
}

func (b *broker) FindProvider(ctx context.Context, fn string) (*storage.ProviderInfo, error) {
	providers, err := b.getAllProviders(ctx)
	if err != nil {
		return nil, err
	}

	// find longest match
	var match string
	var provider *storage.ProviderInfo
	for _, p := range providers {
		prefix := p.MountPath
		if strings.HasPrefix(fn, prefix) && len(prefix) > len(match) {
			match = prefix
			provider = p
		}
	}

	if match == "" {
		return nil, notFoundError("storage provider not found for path: " + fn)
	}

	return provider, nil
}

type notFoundError string

func (e notFoundError) Error() string { return string(e) }
func (e notFoundError) IsNotFound()   {}

type userContextRequiredError string

func (e userContextRequiredError) Error() string        { return string(e) }
func (e userContextRequiredError) UserContextRequired() {}
