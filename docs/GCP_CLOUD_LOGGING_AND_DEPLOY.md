# GCP Cloud Logging & deploy to server

The bootstrap binary already writes **structured JSON** to stdout (`slog` JSON handler in `cmd/main.go`), which systemd captures for `ridechain-bootstrap.service`. To see those logs in **Google Cloud Logging**, install the **Ops Agent** on the VM and merge the snippet below.

> **I can’t run `gcloud` or SSH to your server from CI here.** Use GitHub Actions (below) or run commands on your machine / Cloud Shell.

---

## 1. Send logs to GCP (Cloud Logging)

### 1A. Install Ops Agent on the VM

SSH into the Compute Engine instance, then:

```bash
cd /path/to/ride/bootstrap   # or clone repo on the VM
sudo bash scripts/install-gcp-ops-agent.sh
```

### 1B. Merge YAML snippet (important)

The install script **does not** overwrite `/etc/google-cloud-ops-agent/config.yaml` (that would drop default metrics/logging). Manually merge **`config/gcp-ops-agent-ridechain-snippet.yaml`**:

- Add `ridechain_bootstrap` under `logging.receivers` (merge with existing receivers if any).
- Add pipeline `ridechain_to_cloud` under `logging.service.pipelines` (merge with existing pipelines).

Then:

```bash
sudo systemctl restart google-cloud-ops-agent
```

### 1C. View logs

**GCP Console → Logging → Logs Explorer**

Example filter:

```
resource.type="gce_instance"
logName=~"/ridechain"
```

Or search for JSON fields from bootstrap, e.g. `jsonPayload.msg="bootstrap starting"`.

### 1D. Cost / volume

Keep **`LOG_LEVEL`** unset (default **INFO**) in production. **`LOG_LEVEL=debug`** greatly increases volume and GCP ingestion cost. See `BOOTSTRAP_SERVICES_RUNBOOK.md`.

---

## 2. Deploy bootstrap to the server

### Option A — GitHub Actions (recommended)

Workflow: **`bootstrap/.github/workflows/deploy.yml`**

**Triggers:** push to `main`, or **Actions → Build & Deploy Bootstrap → Run workflow**.

**Repository secrets** (Settings → Secrets and variables → Actions):

| Secret | Purpose |
|--------|---------|
| `GCP_SA_KEY_JSON` | Optional: upload binary to GCS (`gs://…/bootstrap`) |
| `GCP_VM_SSH_KEY` | Private SSH key for the VM |
| `GCP_VM_IP` | VM external IP (prefer **static** IP) |
| `GCP_VM_USER` | SSH user (e.g. `ubuntu` or `ridechain`) |

If SSH secrets are missing, the workflow still **builds** but **skips** VM deploy (see workflow `echo` warnings).

After a successful deploy, the job runs `systemctl restart ridechain-bootstrap` and a quick health check on port **4005**.

### Option B — Manual deploy

1. Build Linux binary:

   ```bash
   cd bootstrap
   CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bootstrap-linux-amd64 ./cmd/main.go
   ```

2. Copy to the VM and install (paths may match `deploy.yml`):

   ```bash
   scp bootstrap-linux-amd64 USER@VM_IP:/tmp/bootstrap-new
   ssh USER@VM_IP 'sudo systemctl stop ridechain-bootstrap; sudo mv /tmp/bootstrap-new /opt/ridechain-bootstrap/bootstrap; sudo chmod +x /opt/ridechain-bootstrap/bootstrap; sudo systemctl start ridechain-bootstrap'
   ```

3. Optional: upload to GCS if the VM pulls from a bucket (see `DEPLOY_PLAN_GCP.md`).

---

## 3. Related docs

- `docs/DEPLOY_PLAN_GCP.md` — full GCP VM, Caddy, firewall, static IP  
- `docs/PRODUCTION_SETUP.md` — production checklist  
- `docs/BOOTSTRAP_SERVICES_RUNBOOK.md` — systemd, logs, `journalctl`  
