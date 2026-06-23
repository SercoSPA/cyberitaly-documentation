# Configure before deploy

Exactly which values to fill in, and where each one comes from, before you
deploy the stack. 5 files in total.

**Legend**
- 🖊️  **You invent** — generate the value yourself
- 🤖 **Script** — a script prints it; copy-paste the output
- 📌 **Fact** — a known value (IP, version, token from the UI)

---

## 1. `certs/instances.yml` — 📌 *before generating certs*

Uncomment and set the real IPs (lines 16, 26, 36):

| Instance | Set IP of |
|----------|-----------|
| `elasticsearch` | `ci-mon-db-01` |
| `kibana`        | `ci-mon-dash-01` |
| `fleet-server`  | `ci-mon-dash-01` |

> Required if the Kubernetes nodes reach Fleet/Elasticsearch by IP rather than
> hostname — otherwise certificate verification fails.

---

## 2. `.env` (repo root) — used only by the cert script

| Variable | Type | Value |
|----------|------|-------|
| `STACK_VERSION` | 📌 | default `8.18.2`; change only for a different 8.x |

---

## 3. `db-01/.env`

| Variable | Type | How to get it |
|----------|------|---------------|
| `STACK_VERSION` | 📌 | match everywhere |
| `CLUSTER_NAME`  | 📌 | default `cyberitaly-monitoring` is fine |
| `LICENSE`       | 📌 | `basic` (default) or `trial` |
| `ELASTIC_PASSWORD` | 🖊️ | `openssl rand -base64 24` |

---

## 4. `dash-01/.env`

| Variable | Type | How to get it |
|----------|------|---------------|
| `STACK_VERSION` | 📌 | same as db-01 |
| `ELASTIC_PASSWORD` | 📌 | **same value** as `db-01/.env` |
| `KIBANA_PASSWORD` | 🤖 | `scripts/05-set-kibana-password.sh` |
| `FLEET_SERVER_SERVICE_TOKEN` | 🤖 | `scripts/06-create-fleet-token.sh` |
| `CA_TRUSTED_FINGERPRINT` | 🤖 | `scripts/04-ca-fingerprint.sh` |
| `ENCRYPTION_KEY` | 🖊️ | `openssl rand -hex 32` |
| `SECURITY_ENCRYPTION_KEY` | 🖊️ | `openssl rand -hex 32` (different) |
| `REPORTING_ENCRYPTION_KEY` | 🖊️ | `openssl rand -hex 32` (different) |

> The three 🤖 values do not exist until Elasticsearch is running on db-01.

---

## 5. `k8s/elastic-agent-managed.yaml`

| What | Line | Type | How to get it |
|------|------|------|---------------|
| `FLEET_ENROLLMENT_TOKEN` | 54 | 📌 | Kibana → **Fleet → Enrollment tokens → "Kubernetes Cluster Policy"** (exists only after dash-01 is up) |
| image tag `:8.18.2` | 35 | 📌 | bump only if you changed `STACK_VERSION` |

---

## Summary of values you create by hand

| Value | Command | Goes into |
|-------|---------|-----------|
| elastic password | `openssl rand -base64 24` | `db-01/.env` **and** `dash-01/.env` |
| encryption key 1 | `openssl rand -hex 32` | `dash-01/.env` → `ENCRYPTION_KEY` |
| encryption key 2 | `openssl rand -hex 32` | `dash-01/.env` → `SECURITY_ENCRYPTION_KEY` |
| encryption key 3 | `openssl rand -hex 32` | `dash-01/.env` → `REPORTING_ENCRYPTION_KEY` |

Everything else is either printed by a script (04/05/06) or a known fact
(IPs, version, enrollment token).

---

## Order it happens

1. Fill **#1, #2, #3** → run `scripts/01-generate-certs.sh` → start Elasticsearch on db-01.
2. Run `scripts/04`, `05`, `06` on db-01 → paste their output + your generated keys into **#4** → start dash-01.
3. Log into Kibana → copy the enrollment token → fill **#5** → `kubectl apply -f k8s/elastic-agent-managed.yaml`.

See [README.md](README.md) for the full command-by-command walkthrough.
