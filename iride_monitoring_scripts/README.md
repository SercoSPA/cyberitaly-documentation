# IRIDE CyberItaly — Monitoring Collectors (cron-scheduled)

Python collectors that probe IRIDE CyberItaly services and ship JSON documents to
the Serco Elastic instance (`https://ci-mon-db-01:9200`). This README covers **only
the folders shipping a `cron_run.sh` wrapper** — i.e. the units currently scheduled
on the monitoring VM. Folders without a wrapper (`API_Insula`, `API_AAO`, `MongoDB`)
and the scripts not referenced by any wrapper are listed at the end as out of scope.

---

## 1. Common architecture

Every collector follows the same pattern:

| Aspect | Convention |
|---|---|
| Config | Sibling `.ini` file, read via `configparser`, resolved relative to `__file__` |
| Dry-run | `ELASTIC_ENABLED = False` → local file only, no Elastic connection |
| Output | NDJSON `.log` next to the script (one document per line) **plus** Elastic index |
| Doc style | DESP-style events: `@timestamp` first, `event_type`, `hostname`, status field |
| Status model | `OK` / `SLOW` / `FAIL` (KPI probes) or `HEALTHY` / `DEGRADED` / `DOWN` (health probes) |
| Runtime | `source /home/monitoring/venv/bin/activate` inside `cron_run.sh`, then `python3` |
| Auth | Keycloak password grant (Insula/IAM), OVH API keys, SFTPGo JWT/API key, S3 keys |

`cron_run.sh` always resolves its own directory via `readlink -f "$0"`, `cd`s into it,
activates the venv and runs one or more scripts in sequence (with `sleep` spacing where
several run back-to-back).

**Proxy requirement (all units):** `http_proxy`, `https_proxy` and `no_proxy` must be set
**at the top of the crontab**, not in `~/.bashrc` — cron does not read it. Egress to OVH /
Insula goes through Squid; traffic to `ci-mon-db-01` must bypass it via `no_proxy`.

---

## 2. Scheduled units

### `IAM/` — Keycloak IAM metrics
Runs two scripts in sequence (10 s apart).

- **`kpi_iride_keycloak_events_iride_elastic.py`** → `logs-iride-keycloak-events.monitoring-default`
  Pulls `LOGIN` and `LOGIN_ERROR` events from the Keycloak Admin API over a
  `LOOKBACK_MINUTES = 30` window (paginated, with token refresh mid-pagination).
  One doc per event (`keycloak_event`) with a **deterministic SHA-1 `_id`**, so overlapping
  runs are idempotent instead of duplicating; plus one `keycloak_events_summary` doc.
  `@timestamp` is the real event time, `ingest_timestamp` the run time.
  *Requires index-template mapping:* `kc_event_type`, `error`, `ip_address`, `client_id_kc`,
  `user_id`, `session_id` as `keyword`; `details` as `flattened` (Keycloak emits dynamic keys →
  mapping explosion otherwise). Suggested cadence: 5–10 min (`LOOKBACK ≥ 2×` cron interval).
- **`kpi_iride_keycloak_user_statistics_iride_elastic.py`** → `logs-iride-keycloak-users.monitoring-default`
  Full realm user list → global totals, federated vs registered, `DPAD_Services` members,
  active sessions across clients, plus breakdowns by country / userProfile / gender.
  Emits one `keycloak_user_statistics` summary doc + N `keycloak_user_breakdown` docs.
  Suggested cadence: daily (user counts move slowly).

### `KPI_Scripts/Catalogue_Access_Services_Availability/`
- **`collector_insula_kpi_iride_elastic.py`** → `logs-iride-insula-kpi.monitoring-default`
  Contractual KPI *"Availability of data catalogue and access services"*. End-to-end synthetic
  probe over Insula API v2.0, six ordered probes each with OK/NOK + latency: `auth`,
  `catalogue_params`, `catalogue_search` (**the KPI latency measure**), `file_lookup`,
  `file_metadata`, `file_download` (truncated at `MAX_DOWNLOAD_BYTES`, canary file of a few KB).
  Rollup: `catalogue_status` and `access_status` are OK only if *all* probes in their category
  are OK; `overall_status` is the AND of both. Emits `insula_kpi_probe` (per probe, optional)
  and `insula_kpi_availability` (per run, always). Stdlib only, supports `--config` and `--dry-run -v`.
  This is the **newer copy (v1.17.0)** of the collector, with NWD/NWH windowing
  (Mon–Fri, 08:00–17:00 `Europe/Rome`) that the `API_Insula/` copy (v1.15.0) lacks.

### `KPI_Scripts/Processing_Services_Availability/`
- **`collector_insula_processing_iride_elastic.py`** → `logs-iride-insula-processing.monitoring-default`
  Contractual KPI *"Availability of Processing services"*, target **≥ 95% of NWD-NWH**. Deliberately
  separate from the catalogue KPI (separate requirement, separate threshold).
  **Level 1 (free, always on):** `jobs_search`, `jobconfigs_list`, `service_check`.
  **Level 2 (billed, own cadence, `JOB_LAUNCH_ENABLED = True`):** real `job_launch` →
  `job_execution` polling to COMPLETED → `job_outputs` → `cleanup_outputs` / `cleanup_job`.
  The emitted event declares which `kpi_level` backed the measurement. Outcome is always
  recorded **before** cleanup (after the DELETE, the Elastic/NDJSON event is the only evidence),
  and cleanup results never affect the KPI. Uses an overlap lock — mandatory with a frequent cron.
  Docstring also captures hard-won API quirks: only `sort=id,desc` is reliable, no server-side
  time filter works on `parametricFind` (window client-side), `DELETE /jobs/{id}` works (204)
  though undocumented, outputs live in `detailedJob.outputFiles`, and real launch cost was
  2 coins vs the 1 declared in `costingExpression` — cost is measured, not assumed.
  Emits `insula_processing_probe`, `insula_processing_availability`, and
  `insula_wallet_recharge` when a top-up happens.

### `KPI_Scripts/Dashboard_Availability/`
- **`kpi_dashboard_availability_iride_elastic.py`** → `logs-iride-dashboard-availability.monitoring-default`
  KPI *"Availability of visualization and user dashboard services"* for the `perception` app.
  **Level A:** HTTP GET on the dashboard front-end (status + latency). **Level B:** Keycloak login +
  a call to the Insula `/jobs` API that feeds the dashboard. Rationale: the dashboard is an SPA,
  so a 200 on the front-end only proves the static bundle is served, not that data is reachable.
  One doc per run with `frontend_*`, `api_*` and an aggregated `response_status`. Light: 2 GETs + 1 token POST.

### `KPI_Scripts/Data_Access_Availability/`
- **`kpi_data_access_availability_iride_elastic.py`** → `logs-iride-data-access-availability.monitoring-default`
  KPI *"Availability of data retrieval, ingestion and access"* against SFTPGo
  (`ftp.dingest.iride-cyberitaly.space`). **Level A:** `GET /api/v2/token` (Basic Auth → JWT).
  **Level B:** `GET /api/v2/user/dirs` with a user-scope API key (`X-SFTPGO-API-KEY`) — a real
  directory listing, proving retrieval works end-to-end and not just that auth is up.
  One doc per run with `auth_*`, `retrieval_*`, aggregated `response_status`.

### `API_Insula_Awereness/`
- **`kpi_insula_awareness_storage_iride_elastic.py`** → `logs-iride-insula-awareness.monitoring-default`
  D-100 consumption metrics from Insula (ref. CIMS-39): paginated `/wallets` (user list + credit
  balance), paginated `/quotas` filtered on `usageType.name == "FILES_STORAGE_MB"` (per-owner
  override, default 5000 MB), then `/reports/storage/{id}/CSV` per user (no bulk endpoint —
  one call per user), taking the last row in bytes → MB. Emits one `insula_awareness_user` doc
  per user plus one `insula_awareness_aggregate`. Has a `MOCK_MODE` for dry testing.
  Triage note: nginx 500 with an HTML body on `/wallets` is *their* backend being down, not the
  token — 401/403 is the token/scope. The collector logs the status and does not crash.

### `OVH_Quota/` — three OVH read-only collectors in sequence (10 s apart)
- **`kpi_iride_ovh_compute_quota_iride_elastic.py`** → `logs-iride-ovh-compute-quota.monitoring-default`
  `GET /cloud/project/{id}/region/{REGION}/quota` → one snapshot doc: instances used vs max, RAM,
  volumes, network, load balancers, MB/GB normalised to bytes, plus additive `*_usage_pct` fields
  for threshold alerting. Minimal impact (1 API call).
- **`kpi_iride_ovh_bucket_totalsize_iride_elastic.py`** → `logs-iride-ovh-bucket-totalsize.monitoring-default`
  `GET .../region/{REGION}/storage` → one doc per bucket with aggregate size (bytes/MB/GB) and
  object count. Metadata only, no data I/O.
- **`kpi_iride_ovh_k8s_nodepools_iride_elastic.py`** → `logs-iride-ovh-k8s-nodepool.monitoring-default`
  `GET /cloud/project/{id}/kube` → `/kube/{kubeId}` → `/kube/{kubeId}/nodepool`. One doc per nodepool
  (~13 across the 4 CYIT-01 clusters) with cluster metadata, pool detail and derived vCPU/RAM
  (flavor × node count). Built after feedback that tenant-level quota was too coarse: per-nodepool
  granularity shows consumption per component (mgmt, proc, dingest-core, dingest-dret, tools, models).
  Heaviest of the three — it iterates every cluster in the tenant. Suggested cadence: 10 min.
  Requires `pip install ovh elasticsearch`.

### `K8s_health/`
- **`collector_k8s_pod_health_iride_elastic.py`** → `logs-iride-k8s-pod-health.monitoring-default`
  Pod health probe over `kubectl` (subprocess) across `TARGET_NAMESPACES`
  (`adam-catalog, adam-dapapi, adam-dret, adam-ftp, adam-mongo, adam-wxs, platform, logging, argocd`).
  One doc per pod: phase, ready, restart counts, uptime/pod age, container detail, resource
  requests/limits and live CPU/RAM via metrics-server (`KUBECTL_TOP_ENABLED = True`, adds ~1–2 s
  per namespace). Read-only. Wrapper exports `KUBECONFIG=/home/.kube/kubeconfig`. Cadence: 5–15 min.
  Derived `health_status`: `HEALTHY` / `DEGRADED` / `DOWN` / `COMPLETED` / `RUNNING`, refined over
  three revisions: CPU/RAM percentages now fall back to *requests* when no *limit* is set (previously
  patchy nulls broke graphs and alerts); `restart_delta` (via a state file) replaces the monotonic
  cumulative counter so real crashloops surface and old healthy pods stop being flagged forever;
  and classification is **owner-aware** — Job/CronJob pods have a finite lifecycle, so `Pending`/
  `Running` are normal transients (`Failed` = DOWN, `Succeeded` = COMPLETED, otherwise RUNNING),
  which removed false reds on frequently-running CronJobs. Long-running workloads keep the strict
  rule. Docs carry `owner_kind` and `is_job_pod` for Grafana splits.

### `HIS_Central_DT1/`
- **`kpi_his_central_health_iride_elastic.py`** → `logs-iride-his-central-health.monitoring-default`
  Synthetic health + freshness probe on HIS-Central (GeoDAB / MEEO), the aggregated ISPRA
  hydrometric source used by CIMS-49. Endpoint `/om-api/observations?limit=N`; the in-URL token is
  tied to the CyberItaly account and does not expire.
  **Level 2:** reachability — `http_code`, `latency_ms`, `response_size_bytes`.
  **Level 4:** freshness — `max(phenomenonTime.end)` over 50 observations vs now
  (`data_last_timestamp`, `data_freshness_minutes`, `data_freshness_status`).
  `limit=50` rather than `1` because HIS-Central does not guarantee chronological ordering, so a
  single record may not be the newest. Plus station/source metadata samples.

### `S3_Bucket/` — probe then inventory, 60 s apart
- **`kpi_s3_synthetic_iride_elastic_e2e.py`** → `logs-iride-s3-health.monitoring-default`
  Active write/read/delete probe measuring upload, download and delete latency on
  `TARGET_BUCKETS` (OVH endpoint `s3.gra.io.cloud.ovh.net`), emitting `s3_probe_e2e` per bucket.
  Minimal-invasiveness rules: dedicated `__monitoring/` prefix, unique object name
  (timestamp + uuid, never overwrites), ~1 KB identifiable payload, cleanup guaranteed via
  `try/finally`, per-step logging. Note the dry-run flag applies to Elastic only — the probe
  always really writes to S3. Suggested cadence: 15 min.
- **`service_bucket_folders_size_iride_elastic.py`** → `logs-iride-s3-storage.monitoring-default`
  Inventory scan: walks every project bucket and computes total size + object count per
  top-level folder, one doc per folder. Runs second because it is I/O-heavy (paginator over all
  buckets). Suggested cadence: 6 h.
  **Wrapped in `flock -n`** on `.service_bucket_folders_size.lock`: the scan can outlast the cron
  interval, and overlapping runs previously multiplied written documents (~4M junk docs and a 2 GB
  log file). If the previous run is still active this one skips the scan and exits 0 (not an error).

### `FTP_folder/`
- **`collector_sftpgo_folders_iride_elastic.py`** → `logs-iride-sftpgo-inventory.monitoring-default`
  Full folder/file inventory of the MEEO-managed SFTPGo sink (SFTPGo 2.6.6, ref. CIMS-42).
  Flow: `GET /api/v2/token` (admin Basic Auth → JWT) → `GET /api/v2/users` → optional
  `POST /api/v2/quotas/users/{u}/scan` to force quota recalculation → `GET /api/v2/users/{u}` for
  refreshed `used_quota` → `GET /api/v2/user/dirs` for listings.
  **Auth pattern that actually works:** a user-scope API key (`POST /api/v2/apikeys`, `scope=2`,
  `user="<username>"`) sent as `X-SFTPGO-API-KEY` with **no** `.username` suffix. The
  admin-scope + impersonation pattern suggested by the docs does not work on 2.6.6.
  Emits `sftpgo_user_inventory` (per user: quota usage, per-subfolder sizes, top-level structure,
  volume fields) and `sftpgo_folder_node` (per folder, nested included: `path`, `file_count`,
  `total_bytes`, oldest/newest mtime, `freshness_minutes`, `oldest_file_age_days`,
  `retention_breach`). Suggested cadence: 30 min.
  **Volume capacity (added after the 02/08/2026 incident):** `quota_pct_used` is only computed when
  the user has `quota_size > 0`. The `ispra` user is unlimited, so that field stays null and no alert
  can ever fire on it — yet the real constraint is the underlying PVC filesystem, which the SFTPGo
  REST API does not expose. The volume filled to 100%, all ISPRA uploads failed, and nothing alerted.
  Capacity is now measured from outside via a three-level cascade (`VOLUME_CAPACITY_MODE`):
  1. `kubectl exec <sftpgo pod> -- df -k -P /var/lib/sftpgo` (preferred: real capacity/used/available,
     including filesystem overhead and uncounted files);
  2. on-disk cache `.volume_capacity_cache.json` (capacity only — reusing stale used/available would mislead);
  3. static `VOLUME_CAPACITY_BYTES` from the INI as a last resort.
  `volume_capacity_source` records which level supplied the value; `volume_metric_basis` distinguishes
  real (`df`) from derived (`used_quota`) metrics.

---

## 3. Requirements

- Python venv at `/home/monitoring/venv` (`requests`, `urllib3`, `ovh`, `elasticsearch`, `boto3`).
  Several collectors are deliberately stdlib-only (the two Insula KPI collectors) to avoid pip
  installs on the VM.
- `kubectl` reachable from the collection host for `K8s_health/` and for `FTP_folder/` volume discovery.
- Elastic API key with write access to the target indices; TLS verification is disabled
  (`MONITORING_VERIFY_CERTS = False`, self-signed cert on `ci-mon-db-01`).
- Crontab-level proxy variables (see §1).

---

## 4. Known rough edges

Worth cleaning up before this goes anywhere near a shared repo:

1. **Secrets in plaintext.** Every `.ini` carries live credentials (Elastic API key, Keycloak
   passwords, OVH consumer/application secrets, S3 access/secret keys, SFTPGo admin password).
   Do not commit as-is: ship `.ini.template` files and keep real values out of version control.
2. **Copy-pasted `cron_run.sh` headers.** Six wrappers still describe
   `collector_k8s_pod_health_iride_elastic.py` in their comment block while executing something
   else entirely (Catalogue, Processing, Awareness, FTP_folder, HIS_Central, and the IAM one).
   The `echo` tags are wrong too — e.g. FTP_folder logs `[collector_k8s_pod_health]`.
3. **Missing shebang** in `Catalogue_Access_Services_Availability`, `Processing_Services_Availability`,
   `API_Insula_Awereness` and `FTP_folder` wrappers. They work when cron/bash invokes them, but
   `./cron_run.sh` behaviour is not guaranteed.
4. **`FTP_folder/cron_run.sh` does not export `KUBECONFIG`** even though the collector needs
   `kubectl` for volume capacity — it silently falls back to cache or the static value unless
   `$HOME/.kube/config` happens to be valid for the `monitoring` user. `K8s_health` and
   `HIS_Central_DT1` do export it (`/home/.kube/kubeconfig`), which also contradicts the
   `$HOME/.kube/config` claim in the wrapper comments.
5. **Duplicate collector.** `API_Insula/collector_insula_kpi_iride_elastic.py` (v1.15.0) is a
   stale copy of the scheduled `KPI_Scripts/Catalogue_Access_Services_Availability` version
   (v1.17.0). Two files to keep in sync, one of them dead. Delete or symlink.
6. **Index naming drift.** Docstrings suggest `metrics-iride-*` for several collectors while the
   INIs use `logs-iride-*`. The INI wins at runtime, but the comments should be corrected.
7. **No lock on other long collectors.** Only the S3 storage scan uses `flock`. `FTP_folder`
   (full recursive inventory) and `OVH_Quota` nodepools are the next most likely to overrun a
   tight cron interval.

## 5. Out of scope for this README

No `cron_run.sh` present: `API_Insula/`, `API_AAO/` (CODATA AAO health, freshness, station
sensors, value anomaly), `MongoDB/` (catalog freshness collector + smoke test).
Present but not invoked by any wrapper: `K8s_health/audit_k8s_cluster_full_iride_elastic.py`,
`S3_Bucket/kpi_s3_synthetic_iride_elastic_e2e-prod.py`.
