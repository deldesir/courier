package runtime

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/gomodule/redigo/redis"
	_ "github.com/lib/pq" // postgres driver
	"github.com/nyaruka/gocommon/aws/cwatch"
	"github.com/nyaruka/gocommon/aws/dynamo"
	"github.com/nyaruka/gocommon/aws/s3x"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/vkutil"
	"github.com/vinovest/sqlx"
)

type Runtime struct {
	Config *Config
	DB     *sqlx.DB
	Dynamo *dynamodb.Client
	VK     *redis.Pool
	S3     *s3x.Service
	CW     *cwatch.Service

	HTTP       *http.Client
	HTTPAccess *httpx.AccessConfig

	// HTTPProxied is the HTTP client used by handlers that send to user-configured URLs. When
	// SendProxyURL is set, it routes through that forward proxy. Otherwise it's the same as HTTP.
	HTTPProxied *http.Client

	Writers *Writers
	Spool   *dynamo.Spool
}

func NewRuntime(cfg *Config) (*Runtime, error) {
	rt := &Runtime{Config: cfg}

	var err error

	rt.DB, err = sqlx.Open("postgres", cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("error creating Postgres connection pool: %w", err)
	}
	rt.DB.SetMaxIdleConns(4)
	rt.DB.SetMaxOpenConns(16)

	// DynamoDB: optional — skip if no table prefix configured (nanoRP mode)
	if cfg.DynamoTablePrefix != "" {
		rt.Dynamo, err = dynamo.NewClient(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, cfg.DynamoEndpoint)
		if err != nil {
			return nil, fmt.Errorf("error creating DynamoDB client: %w", err)
		}
	} else {
		slog.Info("DynamoDB disabled (COURIER_DYNAMO_TABLE_PREFIX is empty)")
	}

	rt.VK, err = vkutil.NewPool(cfg.Valkey, vkutil.WithMaxActive(cfg.MaxWorkers*2))
	if err != nil {
		return nil, fmt.Errorf("error creating Valkey pool: %w", err)
	}

	// S3: optional — skip if no access key configured (nanoRP mode)
	if cfg.AWSAccessKeyID != "" {
		rt.S3, err = s3x.NewService(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, cfg.S3Endpoint, cfg.S3PathStyle)
		if err != nil {
			return nil, fmt.Errorf("error creating S3 service: %w", err)
		}
	} else {
		slog.Info("S3 disabled (COURIER_AWS_ACCESS_KEY_ID is empty)")
	}

	rt.CW, err = cwatch.NewService(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, cfg.CloudwatchNamespace, cfg.DeploymentID)
	if err != nil {
		return nil, fmt.Errorf("error creating Cloudwatch service: %w", err)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 8
	transport.IdleConnTimeout = 15 * time.Second
	rt.HTTP = &http.Client{Transport: transport, Timeout: 30 * time.Second}

	// build a proxied variant when SendProxyURL is configured; otherwise reuse the regular client
	// so handlers can always go through HTTPProxied without behavior change.
	proxyURL, err := cfg.ParseSendProxyURL()
	if err != nil {
		return nil, fmt.Errorf("error parsing send proxy URL: %w", err)
	}
	rt.HTTPProxied = rt.HTTP
	if proxyURL != nil {
		proxiedTransport := transport.Clone()
		proxiedTransport.Proxy = http.ProxyURL(proxyURL)
		rt.HTTPProxied = &http.Client{Transport: proxiedTransport, Timeout: 30 * time.Second}
	}

	disallowedIPs, disallowedNets, err := cfg.ParseDisallowedNetworks()
	if err != nil {
		return nil, fmt.Errorf("error parsing disallowed networks: %w", err)
	}
	rt.HTTPAccess = httpx.NewAccessConfig(10*time.Second, disallowedIPs, disallowedNets)

	// DynamoDB spool: only create if DynamoDB is enabled (nanorp mode)
	if rt.Dynamo != nil {
		rt.Spool = dynamo.NewSpool(rt.Dynamo, rt.Config.SpoolDir+"/dynamo", 30*time.Second)
	}
	rt.Writers = newWriters(cfg, rt.Dynamo, rt.Spool)

	return rt, nil
}

// NewTestRuntime returns a minimal Runtime wrapping the given config, suitable for tests that need a
// Runtime but don't bring up real backing services. It populates HTTP with http.DefaultClient so
// code paths that issue outbound HTTP requests work against test servers.
func NewTestRuntime(cfg *Config) *Runtime {
	return &Runtime{Config: cfg, HTTP: http.DefaultClient, HTTPProxied: http.DefaultClient}
}

func (r *Runtime) Start() error {
	if r.Spool != nil {
		if err := r.Spool.Start(); err != nil {
			return fmt.Errorf("error starting dynamo spool: %w", err)
		}
	}

	r.Writers.start()
	return nil
}

func (r *Runtime) Stop() {
	r.Writers.stop()
	if r.Spool != nil {
		r.Spool.Stop()
	}
}
