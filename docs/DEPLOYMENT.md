# WSE Deployment Guide

Production deployment patterns for WSE, covering single-instance, cluster, and container orchestration setups.

---

## Single Instance

The simplest deployment. One WSE server behind a reverse proxy.

```
Client -> nginx (TLS) -> WSE (ws://0.0.0.0:5007/wse)
```

```python
from wse_server import RustWSEServer

server = RustWSEServer(
    "0.0.0.0", 5007,
    max_connections=50_000,
    jwt_secret=b"replace-with-a-strong-secret-key!",
    jwt_issuer="my-app",
    jwt_audience="my-api",
    jwt_cookie_name="access_token",    # optional, default: "access_token"
    jwt_previous_secret=None,          # optional: old secret for zero-downtime rotation
    jwt_key_id=None,                   # optional: expected kid header claim
    # jwt_algorithm="RS256",           # optional: use RS256 or ES256 instead of HS256
    # jwt_private_key=open("key.pem", "rb").read(),  # optional: private key for token encoding
)
server.enable_drain_mode()
server.start()
```

### Reverse Proxy (nginx)

```nginx
upstream wse_backend {
    server 127.0.0.1:5007;
}

server {
    listen 443 ssl;
    server_name ws.example.com;

    ssl_certificate     /etc/ssl/certs/ws.example.com.pem;
    ssl_certificate_key /etc/ssl/private/ws.example.com.key;

    location /wse {
        proxy_pass http://wse_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }
}
```

Key settings:
- `proxy_http_version 1.1` and `Upgrade/Connection` headers enable WebSocket passthrough.
- `proxy_read_timeout 3600s` prevents nginx from closing idle connections before WSE's 60s zombie timeout.

---

## Cluster Deployment

Multiple WSE instances connected via TCP mesh.

```
                         Load Balancer (nginx/HAProxy)
                        /           |           \
                   Node A        Node B        Node C
                   :5007         :5007         :5007
                      \            |            /
                    TCP Mesh (:9999, full connectivity)
```

### Node Configuration

Each node runs the same code. Only `peers` and `cluster_addr` differ per node.

```python
server = RustWSEServer(
    "0.0.0.0", 5007,
    max_connections=50_000,
    jwt_secret=b"replace-with-a-strong-secret-key!",
    jwt_issuer="my-app",
    jwt_audience="my-api",
    jwt_cookie_name="access_token",    # optional, default: "access_token"
    jwt_previous_secret=None,          # optional: old secret for zero-downtime rotation
    jwt_key_id=None,                   # optional: expected kid header claim
    # jwt_algorithm="RS256",           # optional: RS256 or ES256
    # jwt_private_key=open("key.pem", "rb").read(),
    recovery_enabled=True,
    presence_enabled=True,
)
server.enable_drain_mode()
server.start()

server.connect_cluster(
    peers=["10.0.0.2:9999", "10.0.0.3:9999"],
    tls_ca="/etc/wse/ca.pem",
    tls_cert="/etc/wse/node.pem",
    tls_key="/etc/wse/node.key",
    cluster_port=9999,
)
```

### Gossip Discovery

For dynamic environments where nodes come and go, use gossip instead of a static peer list:

```python
server.connect_cluster(
    peers=[],
    seeds=["10.0.0.2:9999"],        # At least one seed node
    cluster_addr="10.0.0.1:9999",   # This node's cluster address
    cluster_port=9999,
)
```

New nodes only need one reachable seed address. The gossip protocol (PeerAnnounce/PeerList frames) propagates membership to all nodes automatically.

### Load Balancer Configuration

For message recovery to work, clients must reconnect to the same node (recovery buffers are local). Use sticky sessions:

**HAProxy:**

```
frontend websocket
    bind *:443 ssl crt /etc/ssl/ws.example.com.pem
    default_backend wse_nodes

backend wse_nodes
    balance roundrobin
    stick-table type string len 64 size 100k expire 30m
    stick on req.cook(access_token)
    server node-a 10.0.0.1:5007 check
    server node-b 10.0.0.2:5007 check
    server node-c 10.0.0.3:5007 check
```

**nginx (ip_hash):**

```nginx
upstream wse_cluster {
    ip_hash;
    server 10.0.0.1:5007;
    server 10.0.0.2:5007;
    server 10.0.0.3:5007;
}
```

### Certificate Generation

For cluster mTLS, generate a shared CA and per-node certificates:

```bash
# Generate CA
openssl ecparam -genkey -name prime256v1 -out ca.key
openssl req -new -x509 -key ca.key -out ca.pem -days 365 -subj "/CN=WSE CA"

# Generate node certificate
openssl ecparam -genkey -name prime256v1 -out node.key
openssl req -new -key node.key -out node.csr -subj "/CN=wse-node-1"
openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -out node.pem -days 365
```

All nodes use the same CA (`ca.pem`). Each node gets its own certificate and key.

---

## Docker

### Dockerfile

```dockerfile
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir wse-server
COPY app.py .

EXPOSE 5007 9999
CMD ["python", "app.py"]
```

### Docker Compose (3-node cluster)

```yaml
services:
  wse-1:
    build: .
    ports:
      - "5007:5007"
    environment:
      - WSE_PORT=5007
      - WSE_CLUSTER_PORT=9999
      - WSE_PEERS=wse-2:9999,wse-3:9999
    volumes:
      - ./certs:/etc/wse:ro

  wse-2:
    build: .
    ports:
      - "5008:5007"
    environment:
      - WSE_PORT=5007
      - WSE_CLUSTER_PORT=9999
      - WSE_PEERS=wse-1:9999,wse-3:9999
    volumes:
      - ./certs:/etc/wse:ro

  wse-3:
    build: .
    ports:
      - "5009:5007"
    environment:
      - WSE_PORT=5007
      - WSE_CLUSTER_PORT=9999
      - WSE_PEERS=wse-1:9999,wse-2:9999
    volumes:
      - ./certs:/etc/wse:ro
```

---

## Kubernetes

### StatefulSet (with gossip discovery)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: wse
spec:
  serviceName: wse
  replicas: 3
  selector:
    matchLabels:
      app: wse
  template:
    metadata:
      labels:
        app: wse
    spec:
      containers:
        - name: wse
          image: your-registry/wse-server:latest
          ports:
            - containerPort: 5007
              name: websocket
            - containerPort: 9999
              name: cluster
          env:
            - name: WSE_SEEDS
              value: "wse-0.wse.default.svc.cluster.local:9999"
          volumeMounts:
            - name: tls
              mountPath: /etc/wse
              readOnly: true
      volumes:
        - name: tls
          secret:
            secretName: wse-tls
---
apiVersion: v1
kind: Service
metadata:
  name: wse
spec:
  clusterIP: None  # Headless service for StatefulSet DNS
  ports:
    - port: 5007
      name: websocket
    - port: 9999
      name: cluster
  selector:
    app: wse
---
apiVersion: v1
kind: Service
metadata:
  name: wse-public
spec:
  type: LoadBalancer
  ports:
    - port: 443
      targetPort: 5007
  selector:
    app: wse
```

With gossip discovery, each pod uses the first pod (`wse-0`) as a seed. The gossip protocol discovers remaining pods automatically as they join.

---

## Graceful Drain

The `drain()` method enables zero-downtime deployments by sending a WebSocket Close frame to all connected clients and rejecting new connections.

### Basic Usage

```python
server.drain(close_code=4300, close_reason="shutting down", timeout=10)
```

Clients receive close code 4300, which signals a server-initiated drain. Well-behaved clients reconnect to another instance. After `timeout` seconds, any remaining connections are force-closed.

### Kubernetes preStop Hook

Use `drain()` in a Kubernetes preStop hook to give clients time to reconnect before the pod is terminated:

```python
import signal

def handle_sigterm(signum, frame):
    server.drain(close_code=4300, close_reason="pod shutting down", timeout=10)
    server.stop()

signal.signal(signal.SIGTERM, handle_sigterm)
```

Configure the pod spec with sufficient `terminationGracePeriodSeconds` to cover the drain timeout:

```yaml
spec:
  terminationGracePeriodSeconds: 30
  containers:
    - name: wse
      lifecycle:
        preStop:
          exec:
            command: ["kill", "-SIGTERM", "1"]
```

### Rolling Restarts

For rolling deployments, the sequence per pod is:

1. Kubernetes sends SIGTERM
2. preStop hook calls `drain(timeout=10)` -- clients receive close code 4300 and reconnect to other pods
3. Drain wait runs as a separate task; health checks and cluster protocol continue to work during the window
4. After timeout, `server.stop()` cleans up remaining resources
5. Kubernetes replaces the pod

In cluster mode, the draining node notifies peers via `ClusterCommand::Drain`. Peers update their routing tables so new messages are not forwarded to the draining node.

### Client Handling

Configure clients to detect close code 4300 and reconnect immediately:

```python
# Python client -- automatic reconnect handles this by default
async with connect("ws://host/wse", token="...") as client:
    # On close code 4300, the client reconnects to the next endpoint in the pool
    async for event in client:
        process(event)
```

---

## Resource Sizing

| Connections | RAM (approx) | CPU Cores | Recovery Budget |
|-------------|--------------|-----------|-----------------|
| 1,000 | 256 MB | 1 | 64 MB |
| 10,000 | 512 MB | 2 | 128 MB |
| 50,000 | 2 GB | 4 | 256 MB |
| 100,000 | 4 GB | 8 | 512 MB |
| 500,000 | 16 GB | 16+ | 1 GB+ |

Memory usage depends on message sizes, presence data, and recovery buffer configuration. The numbers above assume typical workloads with 1 KB average message size. Per-connection outbound buffers are capped at `max_outbound_queue_bytes` (default 16 MB); slow consumers that exceed this limit have messages dropped (recoverable via reconnect).

Set `max_subscriptions_per_connection` to limit how many topics each connection can subscribe to (default 0 = unlimited). This prevents a single client from creating millions of unique topics and exhausting server memory. Both regular subscriptions and queue group memberships count against the same budget.

---

## Monitoring

### Prometheus Metrics

WSE exposes metrics in Prometheus text exposition format via `prometheus_metrics()`. Wire it to an HTTP endpoint:

```python
from starlette.responses import PlainTextResponse

@app.get("/metrics")
async def metrics():
    return PlainTextResponse(
        server.prometheus_metrics(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
```

Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: wse
    scrape_interval: 15s
    static_configs:
      - targets: ["localhost:8000"]
    metrics_path: /metrics
```

See [INTEGRATION.md Section 15](INTEGRATION.md#15-prometheus-metrics) for the full list of exposed metrics.

### Health Endpoint

For simple health checks (load balancer probes, dashboards), use `health_snapshot()`:

```python
@app.get("/health")
async def health():
    return server.health_snapshot()
```

### Key Metrics

| Metric | Type | Alert Condition |
|--------|------|-----------------|
| `wse_connections` | gauge | Approaching `max_connections` |
| `wse_inbound_queue_depth` | gauge | Sustained growth (drain loop too slow) |
| `wse_inbound_dropped_total` | counter | Any increase (events lost) |
| `wse_auth_failures_total` | counter | Spike (brute force or misconfigured clients) |
| `wse_rate_limited_total` | counter | Sustained growth (publisher too fast) |
| `wse_slow_consumer_drops_total` | counter | Any increase (slow clients, tune `max_outbound_queue_bytes`) |
| `wse_cluster_peers` | gauge | Below expected count (peer down) |
| `wse_recovery_bytes` | gauge | Above 80% of `recovery_memory_budget` |

### Alerts

- `wse_inbound_dropped_total` increasing - drain loop is too slow, increase batch size or add processing capacity.
- `wse_cluster_peers < expected` - peer disconnected, check network and mTLS certificates.
- `wse_recovery_bytes > 80% budget` - consider increasing `recovery_memory_budget` or reducing `recovery_ttl`.
- `wse_auth_failures_total` spike - possible brute force attempt or client misconfiguration.
- `wse_slow_consumer_drops_total` increasing - clients can't consume fast enough. Increase `max_outbound_queue_bytes` or investigate slow subscribers.

---

## Configuration

All configuration is passed programmatically through the `RustWSEServer` constructor and `connect_cluster()` method. Origin validation should be configured in your reverse proxy (see [SECURITY.md](SECURITY.md#origin-validation)).
