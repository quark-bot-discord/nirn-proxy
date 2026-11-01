# Config
All variables are optional unless stated otherwise

##### LOG_LEVEL
Logrus log level. Passed directly to [ParseLevel](https://github.com/sirupsen/logrus/blob/master/logrus.go#L25-L45)

##### PORT
The port to listen for requests on

##### METRICS_PORT 
The port to listen for metrics requests on

##### ENABLE_METRICS
Toggle to enable and register metrics. Disabling may improve resource usage

##### ENABLE_PPROF
Enables the performance profiling handler. Read more [here](https://github.com/google/pprof/blob/master/doc/README.md)

##### BUFFER_SIZE
Size for the internal proxy go channels. Channels are used to synchronize and order requests. As each request comes in, it gets pushed to a channel. In go, channels can be buffered, this var defines the size of this buffer.
Decreasing this will improve memory usage, but beware that once a channel buffer is full, requests will fight to be added to the channel on the next free spot. This means that during high usage periods, a part of the requests will be unordered if this value is set too low.

##### OUTBOUND_IP
The local address to use when firing requests to discord.

Example: `120.121.122.123`

##### BIND_IP
The IP to bind the HTTP server on (both for requests and metrics). 127.0.0.1 will only allow requests coming from the loopback interface. Useful for preventing the proxy from being accessed from outside of LAN, for example.

Example: `10.0.0.42` - Would only listen on LAN

##### REQUEST_TIMEOUT
Defines the amount of time the proxy will wait for a response from discord. Does not include time waiting for ratelimits to clear.

##### CLUSTER_PORT
Sets the port that's going to be used to communicate with other cluster members. Default 7946

##### CLUSTER_MEMBERS
Comma separated list of stable/known members of the cluster. Does not need to include all members, a gossip protocol is used for discovery. You may include a port along with the address and if you don't, CLUSTER_PORT is used. This variable overrides CLUSTER_DNS.

Example: `10.0.0.2,10.0.0.3:7244`

##### CLUSTER_DNS
DNS address that will resolve to multiple members of the cluster. Does not need to include all members, a gossip protocol is used for discovery. While this is the recommended method of discovery for Kubernetes or similar, it does come with a limitation, which is that all nodes must use the same port for communication since DNS can't return port information. The port used by the proxy for requests is broadcasted automatically and doesn't need to be the same for nodes.

If using Kubernetes, create a headless service and use it here for easy clustering.

Example: `nirn-headless.default.svc.cluster.local` or `nirn.mydomain.com`

##### MAX_BEARER_COUNT
Bearer token queues max size. Internally, bearer queues are put in an LRU map, this env var represents the max amount of items for this map.
Requests are never interrupted midway, even when an entry is evicted. A low LRU size may cause increased 429s if a bearer token has too many requests queued and fires another one after eviction.
Default: 1024

##### DISABLE_HTTP_2
Flag to disable HTTP/2 requests on the client that makes discord requests. Does not impact the http server.
This flag defaults to true due to go http2 support having a few minor issues that can cause requests to fail.

Default: true

##### BOT_RATELIMIT_OVERRIDES
Allows you to define custom global request limits for one or multiple bots. The default is 50 for bots with concurrency = 1 (/gateway/bot -> session_start_limit.max_concurrency field), 500 for concurrency 16 and based on a formula for higher concurrency values. This does not always represents the correct REST limit though, in those cases, you can manually set it using this flag.

Format: Command separated list of **user id** and limit combo, separated by `:` and with no spaces at all. Don't use application ids.
Example: `392827169497284619:100,227115752396685313:80`


##### DISABLE_GLOBAL_RATELIMIT_DETECTION
Disables the optimistic global rest limit detection. This detection uses the /gateway/bot endpoint, which has a low ratelimit and can cause issues with requests being dropped/delayed as cluster size grows.

You probably want to set BOT_RATELIMIT_OVERRIDES if you set this to true.

Default: false

In the future, this will be the only possible behavior.

##### DEFAULT_MAX_WAIT_MS
How long a request may wait for rate limits (its bucket and the global limit) before the proxy drops it, for requests that do not send `x-quark-max-wait-ms`. A dropped request is answered at once with `503`, `x-quark-shed-reason` and `x-quark-not-sent: true`, and is never sent to Discord, so the client can retry it with no risk of a duplicate. `0` disables the limit.

If you set it, keep it below the shortest client request timeout, because past that the client has already gone. Leave it at `0` if some clients cannot handle a 503 or use routes whose rate-limit reset is longer than the limit. Quark's bots send `x-quark-max-wait-ms` themselves, so they don't need this.

Default: 0

## Request headers (Quark fork)
Optional. All `x-quark-*` headers are removed before a request is forwarded to Discord.

- `x-quark-priority`: `high`, `normal` (default) or `low`. When the global limit is exhausted, waiting requests get tokens strictly in this order. Under sustained overload, low-priority requests wait until their deadline and are shed.
- `x-quark-max-wait-ms`: this request's wait budget, overriding `DEFAULT_MAX_WAIT_MS`.

## Behaviour changes in this fork
- The global token is taken immediately before a request is sent, not when it arrives. A request that waits in its bucket and is then abandoned no longer uses up the budget.
- A request whose client has disconnected is dropped without being sent, and without taking a token.
- A global 429 from Discord pauses the whole global limiter until it expires. Before, the lock was recorded and never read.
- Metrics: `nirn_proxy_global_wait_seconds`, `nirn_proxy_queue_wait_seconds`, `nirn_proxy_shed_total`, `nirn_proxy_cancelled_before_send_total`, `nirn_proxy_global_429_total`, `nirn_proxy_global_waiting`. Any increase in `nirn_proxy_global_429_total` means the configured global limit is above Discord's. Lower it with `BOT_RATELIMIT_OVERRIDES`. If several proxy pods run without clustering, divide the limit by the number of pods, because each pod enforces the limit on its own.

## Unstable env vars
Collection of env vars that may be removed at any time, mainly used for Discord introducing new behaviour on their edge api versions

##### DISABLE_401_LOCK
The proxy locks its queue permanently in case a 401 is encountered during normal operation. This env disables this mechanism but not the logging for it.
