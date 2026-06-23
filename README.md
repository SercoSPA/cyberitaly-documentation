# CyberItaly Monitoring

Elastic Stack 8.x to collect **logs and metrics from a Kubernetes cluster**,
deployed with plain Docker Compose across two AlmaLinux VMs.

## Architecture

```
                Kubernetes cluster (apps)
        ┌─────────────────────────────────────────┐
        │  Elastic Agent DaemonSet (Fleet-managed) │
        │  container logs + k8s/system metrics      │
        └───────────────┬──────────────┬───────────┘
            enroll :8220 │              │ ship data :9200
                         ▼              ▼
   ci-mon-dash-01                    ci-mon-db-01
   (4 vCPU / 8 GB / 50 GB /data)     (4 vCPU / 16 GB / 100 GB /data)
   ┌───────────────────────────┐    ┌──────────────────────────┐
   │ Kibana          :5601 https│◀──▶│ Elasticsearch    :9200    │
   │ Fleet Server    :8220 https│◀──▶│ (single node, TLS, heap 8g)│
   │ Heartbeat (uptime)         │───▶│                            │
   └───────────────────────────┘    └──────────────────────────┘
```

- **ci-mon-db-01** — Elasticsearch single node. Heap 8 GB (½ of RAM), data on `/data`.
- **ci-mon-dash-01** — Kibana, Fleet Server, Heartbeat.
- **Kubernetes** — Fleet-managed Elastic Agent DaemonSet (`k8s/`).
- TLS everywhere, signed by a private CA generated in `certs/`.

## Repository layout

```
.
├── certs/                  CA + per-service certificates (generated)
│   └── instances.yml       cert subjects — add your VM IPs here
├── db-01/                  Elasticsearch (deploy on ci-mon-db-01)
│   ├── docker-compose.yml
│   └── .env.example
├── dash-01/                Kibana + Fleet Server + Heartbeat (deploy on ci-mon-dash-01)
│   ├── docker-compose.yml
│   ├── kibana/kibana.yml
│   ├── heartbeat/heartbeat.yml
│   └── .env.example
├── k8s/                    Fleet-managed Elastic Agent DaemonSet + RBAC
│   ├── elastic-agent-managed.yaml
│   └── README.md
└── scripts/                host prep, cert + token bootstrap helpers
```

## Prerequisites

- Two AlmaLinux VMs as specced above, able to reach each other.
- DNS (or `/etc/hosts`) so `ci-mon-db-01` and `ci-mon-dash-01` resolve on both
  VMs and from the Kubernetes nodes.
- Outbound access to `docker.elastic.co`.

---

## Deployment order

Run the numbered steps in order. `$REPO` = this repo checked out on each VM.

### 0. Both VMs — install Docker

```bash
sudo ./scripts/00-install-docker.sh
```

### 1. Generate certificates (once)

Edit `certs/instances.yml` to add the **real IPs** of both VMs, then:

```bash
cp .env.example .env            # set STACK_VERSION
./scripts/01-generate-certs.sh
```

Copy the whole repo (including the now-populated `certs/`) to **both** VMs.
The script also prints the **CA fingerprint** — save it for step 4.

### 2. ci-mon-db-01 — prepare host and start Elasticsearch

```bash
sudo ./scripts/02-prepare-db-01.sh

cd db-01
cp .env.example .env            # set ELASTIC_PASSWORD (openssl rand -base64 24)
docker compose up -d
docker compose logs -f elasticsearch   # wait until healthy
```

### 3. ci-mon-db-01 — create the dash-01 secrets

Still on db-01, with Elasticsearch running:

```bash
./scripts/05-set-kibana-password.sh     # prints KIBANA_PASSWORD
./scripts/06-create-fleet-token.sh      # prints FLEET_SERVER_SERVICE_TOKEN
./scripts/04-ca-fingerprint.sh          # prints CA_TRUSTED_FINGERPRINT
```

### 4. ci-mon-dash-01 — prepare host and start the dashboard stack

```bash
sudo ./scripts/03-prepare-dash-01.sh

cd dash-01
cp .env.example .env
# Fill in .env:
#   STACK_VERSION            (match db-01)
#   ELASTIC_PASSWORD         (same as db-01)
#   KIBANA_PASSWORD          (from step 3)
#   FLEET_SERVER_SERVICE_TOKEN (from step 3)
#   CA_TRUSTED_FINGERPRINT   (from step 3)
#   ENCRYPTION_KEY / SECURITY_ENCRYPTION_KEY / REPORTING_ENCRYPTION_KEY
#     -> three distinct values from: openssl rand -hex 32
docker compose up -d
docker compose logs -f
```

Open **https://ci-mon-dash-01:5601** and log in as `elastic`.
Under **Fleet → Agents** you should see the local Fleet Server agent healthy.

### 5. Kubernetes — deploy the collecting agents

See [k8s/README.md](k8s/README.md): deploy kube-state-metrics, create the CA
secret, grab the enrollment token for **Kubernetes Cluster Policy**, then
`kubectl apply -f k8s/elastic-agent-managed.yaml`.

---

## Operations

- **Logs:**   `docker compose logs -f <service>` in `db-01/` or `dash-01/`.
- **Restart:** `docker compose restart <service>`.
- **Upgrade:** bump `STACK_VERSION` in both `.env` files (db-01 first, then
  dash-01), re-pull and recreate: `docker compose pull && docker compose up -d`.
  Keep the Elastic Agent image tag in `k8s/elastic-agent-managed.yaml` in sync.
- **Backups:** snapshot `/data/elasticsearch` (or configure an ES snapshot
  repository — recommended for production).

## Security notes

- `.env` files and the contents of `certs/` are git-ignored — never commit them.
- All inter-service traffic is TLS. Certificates are signed by the private CA in
  `certs/ca/`; protect `certs/ca/ca.key`.
- This is a single Elasticsearch node (no HA). For production resilience,
  configure snapshots and consider adding nodes later.
