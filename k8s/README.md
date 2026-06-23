# Collecting from the Kubernetes cluster

The Fleet-managed Elastic Agent runs as a DaemonSet (one per node) and ships:

- **Container logs** — from `/var/log/containers` on each node
- **Kubernetes metrics** — kubelet, controller-manager, scheduler, proxy, apiserver
- **Kubernetes state metrics** — Deployments, Pods, Nodes, etc. (needs kube-state-metrics)
- **System metrics** — CPU, memory, network, disk per node

## Prerequisites

1. The stack on `ci-mon-db-01` / `ci-mon-dash-01` is fully up (see the top-level README).
2. `ci-mon-dash-01` (Fleet Server, port 8220) is reachable from the cluster nodes.
3. The cluster nodes trust — or are given — the CA certificate from `certs/ca/ca.crt`.

## Step 1 — Deploy kube-state-metrics (KSM)

The Kubernetes integration's `state_*` datasets scrape KSM. If it isn't already
in the cluster, install it:

```bash
# Option A: upstream standard manifests
kubectl apply -k github.com/kubernetes/kube-state-metrics

# Option B: Helm
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install ksm prometheus-community/kube-state-metrics -n kube-system
```

The Agent auto-discovers KSM by its standard labels; no extra wiring needed for
a default install.

## Step 2 — Get the enrollment token

In Kibana (`https://ci-mon-dash-01:5601`):

**Fleet → Enrollment tokens** → copy the token for **Kubernetes Cluster Policy**.

## Step 3 — Provide the CA so Agents trust Fleet Server

```bash
kubectl -n kube-system create secret generic elastic-agent-fleet-ca \
  --from-file=ca.crt=../certs/ca/ca.crt
```

## Step 4 — Edit and apply the manifest

In `elastic-agent-managed.yaml` set:

- `FLEET_ENROLLMENT_TOKEN` → the token from step 2
- (already set) `FLEET_URL` → `https://ci-mon-dash-01:8220`
- (optional) bump the image tag if you changed `STACK_VERSION`

Then:

```bash
kubectl apply -f elastic-agent-managed.yaml
kubectl -n kube-system rollout status daemonset/elastic-agent
```

## Step 5 — Verify

- **Fleet → Agents** in Kibana shows one healthy agent per node.
- **Discover** → data view `logs-*` shows container logs.
- **Discover** → `metrics-*` shows Kubernetes metrics.
- The built-in **[Metrics Kubernetes]** dashboards populate.

## Notes

- `ci-mon-dash-01` and `ci-mon-db-01` must resolve from inside the cluster. If
  the nodes can't resolve those hostnames, either add the IPs to the certificate
  SANs (`certs/instances.yml`) and use IPs in `FLEET_URL` + the Fleet output, or
  add `hostAliases` to the DaemonSet pod spec.
- The CA fingerprint configured on the Fleet ES output (`CA_TRUSTED_FINGERPRINT`)
  is what lets the Agents trust Elasticsearch's certificate when shipping data.
