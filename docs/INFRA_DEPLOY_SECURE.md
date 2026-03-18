# Bootstrap Server — Infrastructure & Secure Deployment

> **Scope:** Bootstrap server only. Secure deployment with **no user login** (anonymous P2P peers) while staying **protected from abuse and attacks**.

---

## 1. Bootstrap server at a glance


| Component     | Port                               | Protocol               | Purpose                                                                |
| ------------- | ---------------------------------- | ---------------------- | ---------------------------------------------------------------------- |
| libp2p host   | 4001 (QUIC), 4001 (TCP), 4002 (WS) | QUIC / TCP / WebSocket | Native P2P (DHT, Gossipsub); drivers/peers connect here                |
| Rider bridge  | 4003                               | WebSocket              | Riders (apps) connect with `?city=`; relay to Gossipsub                |
| Driver bridge | 4004                               | WebSocket              | Drivers connect; relay to Gossipsub                                    |
| HTTP API      | 4005                               | HTTP/1.1               | REST: register, discover, search-by-name, put FCM/display-name/lat-lng |


**Backends:** Redis (peer meta, geo, search index), optional FCM (push).

**No user accounts:** Clients are identified by **peer ID** (from libp2p key). Protection is by **rate limiting**, **TLS**, **DDoS mitigation**, and optional **API token** for server-to-server.

---

## 2. Secure deployment without login

Goal: **no username/password**, but **hardened against attacks**.


| Threat               | Mitigation                                                                                                |
| -------------------- | --------------------------------------------------------------------------------------------------------- |
| DDoS / volumetric    | **Caddy-only:** rate limiting + connection limits. **Optional:** Cloudflare in front for edge absorption. |
| TLS stripping / MITM | **Caddy:** Auto TLS (Let’s Encrypt). No Cloudflare required.                                              |
| Abuse / scraping     | Rate limiting in Caddy (per IP and optionally per path/header).                                           |
| Bad traffic          | Request validation, size limits, timeouts in Caddy + app.                                                 |
| Secrets              | Env vars / secret manager; no credentials in client apps for “anonymous” API.                             |


**Optional:** For **server-side or internal callers** only, you can protect the HTTP API with an **API key** (e.g. `X-API-Key`). Mobile apps can stay **unauthenticated** (rate limit by IP + optional peer ID).

---

## 3. Do we need Cloudflare? Can it be done with Caddy only?

**Yes — it can be done with Caddy only.** Cloudflare is **optional**.


| Need                  | Caddy alone                                                            | With Cloudflare                                       |
| --------------------- | ---------------------------------------------------------------------- | ----------------------------------------------------- |
| **TLS**               | ✅ Auto TLS (Let’s Encrypt), HTTP/2                                     | ✅ Can terminate at edge instead                       |
| **Rate limiting**     | ✅ `rate_limit` directive (per IP, path)                                | ✅ Additional edge rate limiting                       |
| **Request logging**   | ✅ `log` directive                                                      | ✅ Plus CF analytics                                   |
| **DDoS / volumetric** | ✅ Rate limits + connection limits on Caddy; VPS still receives traffic | ✅ Traffic filtered at edge before it hits your server |


**Recommendation:**

- **Caddy-only (simplest):** Use **only Caddy** in front of the bootstrap binary. Caddy does **Auto TLS**, **rate limiting**, **logging**, and **HTTP/2**. Your VPS is the only public endpoint; firewall + Caddy’s limits protect you. Sufficient for **launch and moderate traffic**.
- **Add Cloudflare when:** You want **edge DDoS absorption** (attack traffic never hits your VPS), **extra WAF**, or **CDN/caching** for other assets. Then: DNS → Cloudflare (proxy ON) → Caddy → bootstrap.

**Summary:** You do **not** need Cloudflare. Caddy can handle TLS, rate limiting, and logging on its own. Add Cloudflare later if you need edge DDoS protection or want TLS/rate limiting at the edge.

**You can add Cloudflare anytime.** No code or server changes needed: point your domain’s nameservers to Cloudflare, add the same A record (or use CF proxy), and enable proxy (orange cloud). Traffic then flows through Cloudflare. Caddy and the bootstrap binary stay as-is.

---

## 4. Cloudflare (optional) — where to use it if you add it

**If you add Cloudflare:**


| Use case            | How                                                                                  |
| ------------------- | ------------------------------------------------------------------------------------ |
| **DDoS protection** | Proxy ON for API (and optionally WebSocket); attack traffic is handled at the edge.  |
| **TLS at edge**     | Cloudflare terminates HTTPS; origin can be HTTP or HTTPS to Caddy.                   |
| **DNS**             | Point your domain to the VPS; orange cloud = proxy, grey = DNS only (no CF in path). |


**Stack with Cloudflare:** `Internet → Cloudflare → Caddy (rate limit, log) → bootstrap`.  
**Stack without Cloudflare:** `Internet → Caddy (TLS, rate limit, log) → bootstrap`.

### Cloudflare cost — Free plan is enough


| Need                | Free plan                                     | Cost                                 |
| ------------------- | --------------------------------------------- | ------------------------------------ |
| **DDoS protection** | ✅ Unmetered (L3–L7), standard DDoS mitigation | **$0**                               |
| **TLS at edge**     | ✅ Universal SSL (HTTPS)                       | **$0**                               |
| **DNS**             | ✅ Unlimited queries, proxy (orange cloud)     | **$0**                               |
| **WebSocket**       | ✅ Supported on Free                           | **$0**                               |
| **Rate limiting**   | ✅ Basic rules (e.g. 100 req/10s per IP)       | **$0** (limited rules; more on paid) |


**Monthly cost for bootstrap:** **$0** on the **Free plan**. You get DDoS protection, edge TLS, DNS, and basic rate limiting. Billing is per **domain**; subdomains (e.g. `bootstrap.ridechain.in`) don’t add cost.

**When you’d pay:** Pro (~$20/month) or Business if you need advanced rate limiting, more WAF rules, or 24/7 support. For launch and moderate traffic, **Free is enough**.

---

## 5. Caddy — production setup (how big companies run it)

Caddy in front of the bootstrap process gives you: **Auto TLS**, **HTTP/2**, **rate limiting**, and **request logging**. Below is a production-style setup: reusable config, hardening, and operability.

### Why Caddy in production


| Concern          | Caddy                                                                        |
| ---------------- | ---------------------------------------------------------------------------- |
| **TLS**          | Automatic Let’s Encrypt; renews in background. No cert scripts.              |
| **HTTP/2**       | On by default for HTTPS; good for many small API calls.                      |
| **Config**       | One Caddyfile; snippets for reuse; reload without downtime (`caddy reload`). |
| **Logging**      | JSON to file or stdout; easy to ship to a log aggregator.                    |
| **Resource use** | Single process, low memory; handles many connections.                        |


### Production Caddyfile (optimal pattern)

Use a **global options** block, **snippets** for shared behaviour, and **per-site** blocks. Keep TLS and reverse-proxy settings explicit.

```caddyfile
# -----------------------------
# Global options (apply everywhere)
# -----------------------------
{
    # Admin API (optional; disable in prod or bind to localhost)
    admin off
    # Persist TLS certificates and other data
    storage file_system /var/lib/caddy
}

# Reusable snippet: common reverse-proxy behaviour (timeouts, buffers, headers)
(proxy_defaults) {
    reverse_proxy localhost:4005 {
        # Timeouts (avoid slow clients holding connections)
        transport http {
            read_timeout 30s
            write_timeout 30s
        }
        # Fail fast if backend is down
        fail_duration 30s
        # Optional: health check (Caddy 2.6+)
        health_uri /health
        health_interval 10s
        # Security: don’t pass through client headers that could be spoofed
        header_up Host {host}
        header_up X-Real-IP {remote_host}
        header_up X-Forwarded-For {remote_host}
        header_up X-Forwarded-Proto {scheme}
    }
}

# Reusable snippet: rate limiting for API paths
# (Caddy 2.8+ has rate_limit built-in; older builds may need caddy-ratelimit plugin)
(rate_limit_api) {
    rate_limit {
        zone api {
            key {remote_host}
            window 1s
            events 100
        }
    }
}

# Reusable snippet: JSON access log (for aggregators / dashboards)
(log_json) {
    log {
        output file /var/log/caddy/access.log {
            roll_size 100mb
            roll_keep 5
            roll_keep_for 720h
        }
        format json
        level INFO
    }
}

# -----------------------------
# Bootstrap API — main site
# -----------------------------
bootstrap.ridechain.in {
    import rate_limit_api
    import log_json

    # TLS: enforce modern config (Caddy default is already good; this makes it explicit)
    tls {
        protocols tls1.2 tls1.3
        ciphers ECDHE-ECDSA-AES128-GCM-SHA256 ECDHE-RSA-AES128-GCM-SHA256 ECDHE-ECDSA-AES256-GCM-SHA384 ECDHE-RSA-AES256-GCM-SHA384
    }

    # Optional: security headers
    header {
        X-Content-Type-Options nosniff
        X-Frame-Options DENY
        Referrer-Policy strict-origin-when-cross-origin
    }

    # Request size limit (avoid large body abuse)
    request_body {
        max_size 64kb
    }

    import proxy_defaults
}
```

If your bootstrap HTTP server does **not** expose `/health`, remove the `health_uri` / `health_interval` block or add a minimal health endpoint in the Go app.

### Snippets vs inline — why this structure

- **Snippets** (`(name) { ... }` and `import name`) let you reuse rate limiting, logging, and proxy settings across multiple server blocks (e.g. API + WebSocket later) and keep the Caddyfile short.
- **Global `{ }`** sets admin and storage once; same pattern used in larger Caddy deployments.

### Logging like big companies

- **JSON format:** One log line per request; easy to parse (e.g. Loki, Elastic, Datadog). Include what you need in the default format or use `format json` with a custom template if required.
- **Rotation:** `roll_size`, `roll_keep`, `roll_keep_for` avoid unbounded disk use. Adjust for your retention policy.
- **Level:** `INFO` for access; use `DEBUG` only when troubleshooting.

### Rate limiting — tuning

- `events 100` in `window 1s` = 100 requests per second per IP for the matched paths. Adjust to your traffic (e.g. 50 or 200).
- For stricter limits on write paths only, add a second zone (e.g. `zone writes { key {remote_host}; window 1s; events 10; match path /register ... }`).
- Confirm syntax in [Caddy rate_limit docs](https://caddyserver.com/docs/caddyfile/directives#rate_limit) for your version.

### TLS only (no HTTP)

To disable HTTP (port 80) and serve only HTTPS:

```caddyfile
bootstrap.ridechain.in {
    # ... other directives ...
}
# Or at global level: only listen 443
```

By default Caddy redirects HTTP→HTTPS; if behind Cloudflare, you can keep that or restrict listeners.

### Running Caddy in production (systemd)

- **Install:** Use the official Caddy repo or `caddy install` (systemd unit is created).
- **Caddyfile:** Put it in `/etc/caddy/Caddyfile` (or your distro’s path). Reload with `caddy reload --config /etc/caddy/Caddyfile`.
- **User:** Run as non-root; Caddy will bind to 80/443 with setcap or a fronting listener.
- **Restart policy:** `systemctl enable caddy` and `restart on-failure` so it comes back after crashes.

### Summary


| Practice       | What to do                                                                     |
| -------------- | ------------------------------------------------------------------------------ |
| **Config**     | Global options + snippets; one Caddyfile.                                      |
| **TLS**        | Auto TLS; optional explicit `tls { }` for protocols/ciphers.                   |
| **Proxy**      | `reverse_proxy` with read/write timeouts, optional health check, safe headers. |
| **Logging**    | JSON, rotation, fixed retention.                                               |
| **Rate limit** | Per-path zones; tune to traffic.                                               |
| **Security**   | Request body limit; security headers; no admin API on public interface.        |


This mirrors how Caddy is used in production at scale: minimal moving parts, reload-safe, and observable.

---

## 6. Go API gateway — do you need it?

**Current:** One binary: libp2p + rider bridge + driver bridge + **HTTP API** (net/http). No separate “API gateway” today.

**Do you need a dedicated Go API gateway?**


| Need                         | Use Go gateway?      | Alternative                                                                                                 |
| ---------------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------- |
| Rate limiting                | Optional             | **Caddy** (or Cloudflare) can rate limit by IP (and Caddy can match path).                                  |
| Routing to multiple backends | No                   | Bootstrap is a single process; one HTTP server.                                                             |
| JWT / auth                   | No (anonymous peers) | Optional API key for server callers; can be checked in **Caddy** (plugin) or in **bootstrap HTTP handler**. |
| Request ID, timeouts         | Nice to have         | Can add in **bootstrap’s own HTTP middleware** (Go) without a separate gateway.                             |


**Recommendation:**  

- **Start without a separate Go API gateway.** Use **Caddy** for TLS, rate limiting, and logging.  
- If you later need **per-peer-id rate limiting**, **API key validation**, or **complex routing**, add a **thin Go reverse proxy** in front of the bootstrap HTTP server (or implement middleware inside the existing bootstrap HTTP server).

---

## 7. gRPC for bootstrap — or direct connect?

**Current:**  

- **REST** on 4005 (register, discover, search, put FCM/display-name/lat-lng).  
- **WebSocket** on 4003 (rider) and 4004 (driver).  
- **libp2p** on 4001/4002 for native P2P (DHT, Gossipsub).

**Do we need gRPC?**


| Question                        | Answer                                                                                                                                                                                                       |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Is there gRPC today?            | **No.** Only HTTP REST and WebSocket.                                                                                                                                                                        |
| Where does “bootstrap” connect? | **Clients connect to bootstrap:** apps call HTTP API and open WebSocket to rider/driver bridge; native peers connect to libp2p. Bootstrap does **not** call out to Kafka or other services for its core job. |
| Add gRPC for what?              | Only if you want a **second API surface** (e.g. internal services calling bootstrap over gRPC). For **mobile apps and PWA**, REST + WebSocket is enough.                                                     |


**Recommendation:**  

- **Keep REST + WebSocket.** No gRPC for bootstrap unless you introduce internal services that need gRPC.  
- “Direct connect” is correct: clients (and native P2P) **connect to** the bootstrap server; bootstrap talks to **Redis** (and optionally FCM) only.

---

## 8. Kafka — do we need it on the bootstrap server?

**Short answer: no.**


| Bootstrap role                | Data flow                           | Kafka?                                                |
| ----------------------------- | ----------------------------------- | ----------------------------------------------------- |
| Peer registration / discovery | App → HTTP API → Redis              | No                                                    |
| Rider/Driver relay            | App → WebSocket → Gossipsub         | No                                                    |
| FCM push                      | Bootstrap → FCM (when peer offline) | No                                                    |
| Analytics / events            | Not a bootstrap responsibility      | Other services (e.g. API gateway, auth) emit to Kafka |


Bootstrap is **stateless** (state in Redis) and **real-time** (WebSocket + Gossipsub). It does **not** need event streaming for its own operation.

**If you later want** to emit “peer registered” or “discover called” events for analytics, a **single producer** in the bootstrap process can send to Kafka; that’s optional and **not required** for secure deployment or core functionality.

---

## 9. Suggested architecture (single diagram)

```
                    Internet
                        │
        (optional)      │
              ┌─────────▼─────────┐
              │   Cloudflare     │  Optional: DDoS, edge TLS, DNS proxy
              └─────────┬─────────┘
                        │
                        ▼
              ┌─────────────────────┐
              │   Caddy             │  Auto TLS, HTTP/2, rate limit,
              │   (reverse proxy)   │  request logging (required)
              └──────────┬──────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ▼               ▼               ▼
   localhost:4005   localhost:4003   localhost:4004
   (HTTP API)       (Rider WS)      (Driver WS)
         │               │               │
         └───────────────┴───────────────┘
                         │
              ┌──────────▼──────────┐
              │  Bootstrap process  │  libp2p (4001,4002)
              │  (one binary)       │  Redis, FCM
              └─────────────────────┘
```

**Firewall (VPS):**  

- Allow: 80, 443 (and optionally 4003, 4004 if not behind Cloudflare).  
- Allow: 4001 (UDP/TCP), 4002 if you expose libp2p WS publicly.  
- Deny everything else from the internet.

### Chat and media: does region or RAM affect speed?

**Yes.** In the current design, **all chat (text, audio, image) is relayed via the bootstrap** — riders and drivers connect to it over WebSocket; there is no direct phone-to-phone path. So:


| Factor        | Effect                                                                                                                                                                                                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Region**    | Every message goes: sender → bootstrap → receiver. If the bootstrap is far (e.g. europe-west9) and both users are in India, you add roughly **2× the RTT** to Europe (~100–200 ms extra each way). Text still feels fine; voice/images may feel slightly slower.                |
| **RAM / CPU** | Under load (many concurrent WebSocket connections, or large images/audio), a small or busy server can slow down forwarding or drop connections. **4 GB RAM** and a modest vCPU are enough for launch; scale up or add a second bootstrap in Asia if you see lag or disconnects. |


So it’s **not** “P2P always fast” — the server is in the path. Closer region (e.g. asia-south1) and adequate RAM help keep chat and media responsive.

---

## 10. Domain — where to buy and where to host

### Buying the domain


| Option                           | Notes                                                                                           | Approx cost                              |
| -------------------------------- | ----------------------------------------------------------------------------------------------- | ---------------------------------------- |
| **Cloudflare Registrar**         | Buy and keep DNS on Cloudflare; no markup, at-cost pricing. Good if you already use Cloudflare. | ~$9–12/year (.com), ~₹500–800/year (.in) |
| **Namecheap**                    | Popular; .in and .com; often cheap first year. Point nameservers to Cloudflare (free) for DNS.  | ~$8–15/year (.com), ~₹600/year (.in)     |
| **Google Domains → Squarespace** | Simple; now run by Squarespace. Export DNS or use their DNS.                                    | ~$12/year (.com)                         |
| **Porkbun**                      | Low prices, no upsell.                                                                          | ~$8–10/year (.com)                       |
| **GoDaddy / BigRock (India)**    | BigRock is India-focused (.in, .co.in).                                                         | Varies; often promo first year           |


**For India:** `.in` or `.co.in` from Namecheap, BigRock, or Cloudflare. Then use **Cloudflare for DNS** (free) so you get DDoS + proxy if you want.

### Where to “host” it

Two different things:

1. **Domain + DNS (where the name points)**
  - **Host DNS at Cloudflare (free):** Add your domain, set nameservers at the registrar to Cloudflare. Create an **A** record: `bootstrap.ridechain.in` → your VPS IP (on GCP, use a **static** external IP).  
  - **Or** keep DNS at the registrar (Namecheap, etc.) and only point the A record to the VPS. No Cloudflare proxy unless you add them.
2. **Server (where the bootstrap binary runs)**
  - **VPS:** The actual “hosting” is a small VM. You install Caddy + bootstrap binary + Redis (or managed Redis elsewhere). See **Hostinger VPS setup** below if you use Hostinger.

**Flow:** You **buy** the domain (e.g. `ridechain.in`) at a registrar → **DNS** is either at the registrar or at Cloudflare → DNS **A record** `bootstrap.ridechain.in` → **VPS public IP** (on GCP, reserve a **static** IP first) → Caddy + bootstrap listen on that server.


| Step          | Where                                                      | Cost                                     |
| ------------- | ---------------------------------------------------------- | ---------------------------------------- |
| Buy domain    | Registrar (Cloudflare, Namecheap, BigRock, etc.)           | ~₹500–1,500/year                         |
| DNS           | Cloudflare (free) or registrar’s DNS                       | $0 if Cloudflare                         |
| Run bootstrap | VPS (Hostinger, Google Cloud, DigitalOcean, Hetzner, etc.) | ~₹500–1,200/month (or ~$12–25/mo on GCP) |


### Hostinger VPS — OS, RAM, and plan for the bootstrap server

Use one VPS for: **Caddy** (reverse proxy, TLS, rate limit) + **bootstrap binary** (libp2p + HTTP API + rider/driver bridges) + **Redis** (optional, on same server or use managed Redis elsewhere).


| Choice      | Recommendation                               | Why                                                                                                      |
| ----------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| **OS**      | **Ubuntu 22.04 LTS**                         | Long support, well documented; Caddy and Go run great. Use 64-bit.                                       |
| **Plan**    | **KVM 1** (or lowest tier with ≥4 GB RAM)    | 1 vCPU, 4 GB RAM, 50 GB NVMe, 4 TB bandwidth. Enough for bootstrap + Caddy + Redis on one box at launch. |
| **RAM**     | **4 GB minimum**                             | Go binary + Caddy + Redis (if local) fit comfortably. 2 GB is tight if Redis is on the same server.      |
| **Storage** | **50 GB** (KVM 1)                            | Plenty for OS, Caddy, bootstrap binary, Redis data, and logs.                                            |
| **Region**  | **Asia** (e.g. Singapore / India if offered) | Lower latency for Indian users and apps.                                                                 |


**Hostinger VPS tiers (typical):**


| Plan      | vCPU | RAM  | Storage    | Use for bootstrap                                                  |
| --------- | ---- | ---- | ---------- | ------------------------------------------------------------------ |
| **KVM 1** | 1    | 4 GB | 50 GB NVMe | ✅ **Recommended for launch.**                                      |
| KVM 2     | 2    | 8 GB | 100 GB     | Optional if you expect high connection count or run more services. |


**After you create the VPS:**

1. **OS:** Pick **Ubuntu 22.04 LTS** (64-bit) when ordering.
2. **Access:** Use SSH (root or a user + sudo). Save the IP; point Cloudflare **A** record `bootstrap` to this IP.
3. **Firewall:** Allow 80, 443 (HTTP/HTTPS for Caddy), and the ports bootstrap uses (4001 UDP/TCP, 4002–4005 as needed). Deny everything else from the internet.
4. **Install:** Caddy (from Caddy’s repo), Redis (if on same server), and the bootstrap Go binary. No need for a control panel for a single-service setup.

This setup is enough for launch and moderate traffic; move to KVM 2 or add a separate Redis host later if you outgrow it.

### Google Cloud (GCP) — Compute Engine for the bootstrap server

Yes, you can run the bootstrap server on **Google Cloud**. Use a single **Compute Engine** VM: Caddy + bootstrap binary + Redis (on the VM or use **Memorystore for Redis**).


| Choice           | Recommendation                                        | Why                                                                                                                                                                 |
| ---------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **OS**           | **Ubuntu 22.04 LTS**                                  | Same as Hostinger; Caddy and Go run well. Use 64-bit.                                                                                                               |
| **Machine type** | **e2-medium** (or **e2-small** if Redis is elsewhere) | e2-medium: 2 shared vCPU, 4 GB RAM — comfortable for bootstrap + Caddy + Redis on one VM. e2-small: 2 vCPU, 2 GB — use only if Redis is managed (e.g. Memorystore). |
| **RAM**          | **4 GB** (e2-medium)                                  | Enough for Go binary + Caddy + local Redis.                                                                                                                         |
| **Storage**      | **20–30 GB** balanced PD or SSD                       | Sufficient for OS, binaries, Redis data, logs.                                                                                                                      |
| **Region**       | **asia-south1** (Mumbai)                              | Best latency for Indian users; asia-southeast1 (Singapore) as alternative. **europe-west9** (Paris) is fine too — expect ~100–200 ms extra RTT from India.          |


**GCP instance types (typical):**


| Type          | vCPU | RAM  | Use for bootstrap                                        |
| ------------- | ---- | ---- | -------------------------------------------------------- |
| **e2-small**  | 2    | 2 GB | Only if Redis is external (e.g. Memorystore).            |
| **e2-medium** | 2    | 4 GB | ✅ **Recommended** — bootstrap + Caddy + Redis on one VM. |
| e2-standard-2 | 2    | 8 GB | If you expect high connection count.                     |


**If e2 is unavailable (e.g. asia-south1 errors):** Use **standard** machine families instead — they are available in more regions/zones:


| Type              | vCPU | RAM     | Use for bootstrap                                           |
| ----------------- | ---- | ------- | ----------------------------------------------------------- |
| **n1-standard-1** | 1    | 3.75 GB | Smallest; use with external Redis or light Redis.           |
| **n1-standard-2** | 2    | 7.5 GB  | ✅ **Good fallback** — enough for bootstrap + Caddy + Redis. |
| **n2-standard-1** | 1    | 4 GB    | Newer gen; 4 GB RAM, single vCPU.                           |
| **n2-standard-2** | 2    | 8 GB    | Newer gen; comfortable headroom.                            |


Pick **n1-standard-2** or **n2-standard-2** for asia-south1 (Mumbai) when e2 is not offered. If you still see zone errors, try another zone in the same region (e.g. `asia-south1-b` or `asia-south1-c`) or use **asia-southeast1** (Singapore), where e2 is often available.

**Cost note:** GCP on-demand is usually higher than Hostinger for a single small VM (~$12–25/month depending on region and type). Use **Committed Use (1-year)** or **Spot VMs** (for non-critical workloads) to reduce cost. Free tier includes limited e2-micro; for bootstrap + Redis, e2-small/e2-medium is more realistic.

**Cheapest worldwide (for testing, delay OK):**


| Choice                | Recommendation                                                                                                                                                                                                                                         | Approx cost                      |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------- |
| **Region**            | **us-central1** (Iowa) or **us-east1** (South Carolina) — among the cheapest globally. **me-central2** (Dammam) is also low-cost.                                                                                                                      | —                                |
| **Machine**           | **e2-micro** (0.25 vCPU, 1 GB RAM) — free tier in eligible regions (e.g. us-west1, us-central1, us-east1: one e2-micro free per month), or ~**$6/month** on-demand. Very tight for Redis on the same VM; use for light testing or run Redis elsewhere. | **$0** (free tier) or **~$6/mo** |
| **Even cheaper**      | **Spot (preemptible) VM** — same machine type, up to ~91% off. VM can be stopped by GCP with 30s notice; fine for testing.                                                                                                                             | **~$1–3/mo** for e2-micro        |
| **Slightly more RAM** | **e2-small** Spot in us-central1 — 2 vCPU, 2 GB; still cheap, fewer OOM risks.                                                                                                                                                                         | **~$3–5/mo** Spot                |


Pick **Region:** us-central1 or us-east1 → **Machine type:** e2-micro (or Spot e2-micro / Spot e2-small). Expect higher latency from India; for testing that’s acceptable.

**Where to run Redis when the VM is too small (e2-micro):**

The bootstrap binary reads **REDIS_URL** (default `redis://localhost:6379`). You can run Redis on the same box with very little RAM or use an external Redis.


| Option                           | Where Redis runs                                               | Cost                                        | Notes                                                                                                                                                      |
| -------------------------------- | -------------------------------------------------------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A. Same VM (e2-micro)**        | On the same instance as bootstrap + Caddy                      | $0 extra                                    | Install Redis, set `maxmemory 256mb` (and `maxmemory-policy allkeys-lru`). Tight but OK for light testing (few peers, low traffic).                        |
| **B. Managed Redis (free tier)** | **Upstash** (serverless Redis) or **Redis Cloud** (30 MB free) | **$0**                                      | Create a free Redis in the cloud, get a URL (e.g. `rediss://default:xxx@xxx.upstash.io:6379`). Set **REDIS_URL** in the bootstrap env. No Redis on the VM. |
| **C. Bigger single VM**          | **e2-small** (2 GB) or **Spot e2-small** — Redis on same VM    | ~3–6/mo (Spot) or ~12/mo on-demand          | One machine for Caddy + bootstrap + Redis; no external service.                                                                                            |
| **D. GCP Memorystore**           | Managed Redis in same GCP project                              | Paid (no free tier; small instance ~30+/mo) | Overkill for testing; consider when you need HA.                                                                                                           |


**Recommendation for cheapest testing:** Use **Option B** (Upstash or Redis Cloud free tier). Sign up → create a Redis database → copy the URL → set `REDIS_URL` when starting the bootstrap. Your e2-micro then runs only Caddy + bootstrap; Redis is offloaded.

**FCM (push notifications): where token is stored and when the server sends**

| Question | Answer |
| -------- | ------ |
| **Where is the FCM token saved?** | In **Redis**. Key: `peer:meta:{peerId}`. Value: JSON with `peer_id`, `display_name`, `geohash`, `lat`, `lng`, `fcm_token`, `updated_at`. |
| **How is it saved?** | App calls **PUT /register/fcm** (body: `{"peerId","fcmToken"`) and/or **POST /register** with `fcmToken` in the body. Both write to the same Redis peer meta. |
| **When does the server send a push?** | **Only when a message targets a rider who is offline.** Another client (rider or driver) sends a message directed at that peer’s ID; if that peer is not connected via WebSocket, the bootstrap looks up the peer’s FCM token in Redis and sends one FCM message. The server does **not** send “test” or standalone notifications — only relay-to-offline-peer. |
| **Why do Firebase Console test sends work but server sends don’t?** | (1) **Credentials:** Bootstrap needs credentials (see below) or ADC on GCP. (2) **Trigger:** Server only sends when an offline peer is the target of a message; it never sends “just because” you opened the app. |
| **How to verify token is saved?** | Check bootstrap logs on the VM. After app registers you should see: `fcm_token_saved peer_id=... token_len=...` (PUT /register/fcm) or `register_fcm_saved peer_id=... token_len=...` (POST /register with fcmToken). |
| **How to verify server sent a push?** | When a message targets an offline peer, logs show: `fcm_offline msg=attempting push` → then either `push sent` or `no FCM token for peer` / `send failed`. |

**Why is no push coming? (trigger)**

The server sends FCM **only when**:

1. A message arrives that is **targeted** at a specific rider: the JSON must contain **`"to": "<peerId>"`** or **`"target_peer_id": "<peerId>"`**.
2. That rider is **not connected** via the rider WebSocket (app in background, closed, or device offline).

If you only open the app, lock the phone, and wait, **no one is sending a targeted message**, so no push is sent. To test:

- **Two devices:** On device A (or a driver app), send a chat/message **to** device B’s peer ID (the one that should get the push). Put device B in background or close the app so it’s offline. Then send from A. In logs you should see `forward_targeted peer offline` and `fcm_offline msg=attempting push` then `push sent` or `no FCM token for peer`.
- **Check logs:** Search for `forward_targeted peer offline` — if this never appears, no targeted message is reaching the bridge. Search for `fcm_offline` — if you see `attempting push` but then `no FCM token for peer`, the token for that peer is missing or not in Redis (re-register from the app). If you see `push sent`, FCM was sent (check device B’s notification / battery settings).

**To enable FCM on the bootstrap VM** (choose one):

1. **Application Default Credentials (recommended on GCP)** — no key file on the VM.  
   - **IAM:** In **GCP Console → IAM & Admin**, find the VM’s service account (e.g. `PROJECT_NUMBER-compute@developer.gserviceaccount.com`). Edit → **Add another role** → **Firebase Cloud Messaging API Admin** (or **Firebase Admin**). Save.  
   - **VM OAuth scopes (required for ADC):** The VM’s access token must include a scope that allows FCM. If you see **"Request had insufficient authentication scopes"** in logs, the instance was created with limited scopes. Fix: **Stop the VM** → **Edit** → **Access scopes** → set to **“Allow full access to all Cloud APIs”** (or at least add **Cloud Platform**). Save and start the VM. Scopes can only be changed when the VM is stopped.  
   - Do **not** set `GOOGLE_APPLICATION_CREDENTIALS` or `FIREBASE_SERVICE_ACCOUNT_JSON` if using ADC.  
   - Restart: `sudo systemctl restart ridechain-bootstrap`. Logs should show: `auth=Application Default Credentials (e.g. GCP VM service account)`.

2. **Key file** — add to `/etc/ridechain-bootstrap.env`:  
   `GOOGLE_APPLICATION_CREDENTIALS=/opt/ridechain-bootstrap/firebase-key.json` or  
   `FIREBASE_SERVICE_ACCOUNT_JSON=/opt/ridechain-bootstrap/firebase-key.json`  
   Then `sudo systemctl restart ridechain-bootstrap`.

**To disable FCM** (e.g. local dev): set `FCM_DISABLED=true` in the env. Then bootstrap uses a noop sender and does not try ADC.

**Error: "Request had insufficient authentication scopes"**

This means the VM’s **OAuth scopes** don’t include permission to call the FCM API. IAM roles alone are not enough — the instance’s access token is limited by the scopes set when the VM was created.

- **Fix 1 (recommended):** Stop the VM → Edit → **Access scopes** → choose **“Allow full access to all Cloud APIs”** → Save → Start the VM. Then restart bootstrap.
- **Fix 2 (no VM change):** Use a **key file** instead of ADC. Download a service account JSON key from **IAM & Admin → Service accounts → Keys**, put it on the VM (e.g. `/opt/ridechain-bootstrap/firebase-key.json`), set `GOOGLE_APPLICATION_CREDENTIALS=/opt/ridechain-bootstrap/firebase-key.json` in `/etc/ridechain-bootstrap.env`, and restart the bootstrap. Key-based auth does not use instance scopes.

**Searching FCM logs on GCP**

- **On the VM (always works)** — bootstrap logs go to the systemd journal. SSH into the VM and run:
  ```bash
  # Follow bootstrap logs and filter for FCM-related lines
  sudo journalctl -u ridechain-bootstrap.service -f | grep -E 'fcm|fcm_offline|fcm_token_saved|register_fcm'
  ```
  Or search recent logs without following:
  ```bash
  sudo journalctl -u ridechain-bootstrap.service --since "1 hour ago" | grep -E 'fcm|fcm_offline|fcm_token_saved|register_fcm'
  ```
  Useful strings: `fcm_token_saved`, `register_fcm_saved`, `fcm_offline`, `msg=push sent`, `msg=no FCM token`, `msg=send failed`, `auth=Application Default Credentials`, `initialized; will send push`.

- **In GCP Cloud Logging (Logs Explorer)** — if the VM has the **Ops Agent** (or legacy Stackdriver logging agent) installed and sending logs to Cloud Logging:
  1. Open **Logging → Logs Explorer**.
  2. Under **Query**, restrict to your VM: e.g. select **Resource type = GCE VM Instance** and your instance.
  3. Add a filter so the log payload contains FCM text. For example (adjust if your log field names differ):
     - **Simple:** `"fcm"` in the log line, or
     - **Structured:** `jsonPayload.message=~"fcm" OR textPayload=~"fcm"`.
  4. Set time range and run. You should see entries like `fcm_token_saved`, `fcm_offline msg=push sent`, or `fcm msg=initialized`.
  If you don’t see any bootstrap logs in Logs Explorer, logs are only on the VM; use **journalctl** above.

**Log volume and GCP cost:** GCP Cloud Logging charges for ingestion (first 50 GiB/month free). The bootstrap defaults to **INFO** level, so only startup, errors, register, and FCM events are logged. Do **not** set `LOG_LEVEL=debug` in production — it enables per-connection, per-message, and peer_stats logs and can increase cost. Use `LOG_LEVEL=debug` only when troubleshooting; see runbook “GCP Cloud Logging — reduce volume and cost”.

**After you create the VM (do in this order):**

1. **Image:** Use **Ubuntu 22.04 LTS** when creating the instance.
2. **Reserve static external IP (do this before pointing DNS):**  
   GCP Console → **VPC network** → **IP addresses**. Find your VM’s external IP → if type is **Ephemeral**, click it → **Promote to static**, name it (e.g. `bootstrap-static`), Reserve. Attach it to the VM if needed (VM → Edit → Networking → External IP → select the static IP).  
   If you skip this, the IP can change on restart and Cloudflare + the app will break.
3. **Firewall:** VPC → Firewall → create an ingress rule allowing **tcp:80, tcp:443** (and 4003–4005 if needed) from **0.0.0.0/0** (or Cloudflare IPs) to the VM.
4. **DNS:** Point Cloudflare **A** record (e.g. `bootstrap` → `bootstrap.ridechain.in`) to the **static** IP from step 2. Set **SSL/TLS** to **Flexible** if Caddy serves HTTP only on 80.
5. **Install:** SSH in and install Caddy, Redis (if on VM), and deploy the bootstrap binary — or use the **startup script and custom image** below so that after a preemption you can start a new VM and have everything run automatically.

#### GCP: Startup script and custom image

A **startup script** installs Redis, Caddy, and the bootstrap binary on a fresh Ubuntu 22.04 VM. You can then create a **custom image** from that VM; new VMs from the image (e.g. after Spot preemption) will already have everything and start services on boot.

**1. Startup script (what it does)**

- Installs and configures **Redis** (maxmemory 256 MB by default).
- Installs **Caddy** and writes a Caddyfile that reverse-proxies to the bootstrap HTTP API (port 4005). Optional: TLS if you set a domain in metadata.
- Downloads the **bootstrap binary** from a URL (or builds from a Git repo) and installs it under `/opt/ridechain-bootstrap`.
- Creates **systemd** unit `ridechain-bootstrap.service` and enables Redis, Caddy, and bootstrap.

Script path in repo: `**services/bootstrap/scripts/setup-bootstrap-server.sh`**.

**2. How to use the startup script when creating a VM**

- **Option A — Paste at creation:** In the GCP Console, create a VM → **Advanced options** → **Management** → **Automation** → paste the contents of `setup-bootstrap-server.sh` into **Startup script**.
- **Option B — From a GCS object:** Upload the script to a bucket, e.g. `gs://your-bucket/scripts/setup-bootstrap-server.sh`. In **Startup script** enter: `gsutil cat gs://your-bucket/scripts/setup-bootstrap-server.sh | bash` (or set **Startup script URL** to the `gs://` URL if your console supports it).

**3. Instance metadata (optional)**

Set these in the VM **Metadata** (Create instance → **Advanced options** → **Management** → **Metadata**):


| Key                    | Example                                  | Description                                                                                                               |
| ---------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `bootstrap_binary_url` | `https://...` or `gs://bucket/bootstrap` | URL to download the bootstrap binary. **Recommended:** build locally, upload to GCS, set this so the script downloads it. |
| `bootstrap_repo`       | `https://github.com/you/chain`           | If no binary URL: clone this repo and build from source.                                                                  |
| `bootstrap_repo_ref`   | `main`                                   | Branch or tag to build (default: main).                                                                                   |
| `bootstrap_domain`     | `bootstrap.ridechain.in`                 | Domain for Caddy TLS. If unset, Caddy serves HTTP on port 80 only.                                                        |
| `redis_maxmemory`      | `256mb`                                  | Redis maxmemory (default: 256mb).                                                                                         |


**4. Build and upload the binary (for `bootstrap_binary_url`)**

On your machine (from the monorepo root):

```bash
cd /path/to/chain
go build -o bootstrap ./services/bootstrap/cmd
# Upload to GCS (replace bucket name)
gsutil cp bootstrap gs://YOUR_BUCKET/bootstrap
# Make it readable by the VM’s service account, or use a signed URL / public read.
```

Then set metadata `bootstrap_binary_url=gs://YOUR_BUCKET/bootstrap` (the VM’s default service account must have Storage Object Viewer on that bucket).

**5. Create a custom image (after first successful run)**

1. Create a VM with **Ubuntu 22.04**, add the startup script and metadata (e.g. `bootstrap_binary_url`), and start the VM.
2. Wait for the script to finish (check serial console or SSH and `systemctl status ridechain-bootstrap`).
3. (Optional) Remove sensitive data: clear bash history, remove any ad-hoc SSH keys.
4. In the GCP Console: **Compute Engine** → **Images** → **Create image** → Source: **Disk** → select the VM’s boot disk → Create.
5. Stop/delete the original VM if you no longer need it.

**6. After Spot preemption — bring the server back**

- **Using the custom image:** Create a new VM → **Images** → choose your custom image. Redis, Caddy, and the bootstrap binary are already installed; systemd will start them on boot. **Reserve a static external IP** (VPC → IP addresses → Reserve new or Promote existing), attach it to the new VM, then point Cloudflare A record to that static IP. Do not use an ephemeral IP for DNS or the IP will change on restart.
- **Using the startup script again:** Create a new VM from the same **Ubuntu 22.04** image, set the same startup script and metadata. On first boot the script will reinstall everything and download the binary again.

**7. Run bootstrap service when binary is in GCS (one-time on VM)**

If Redis, Caddy, and the systemd unit are already on the VM (from the setup script) and the binary is in GCS at **gs://ridechain-bootstrap/bootstrap**:

1. **Let the VM read the bucket**  
   GCP Console → **Cloud Storage** → **Buckets** → **ridechain-bootstrap** → **Permissions** → **Grant access**.  
   Principal: **your VM’s service account** (e.g. `PROJECT_NUMBER-compute@developer.gserviceaccount.com`, or find it in Compute Engine → your VM → **Details** → Service account).  
   Role: **Storage Object Viewer** → Save.

2. **SSH into the VM** (GCP Console → VM → **SSH**).

3. **Download binary and start service** — on the VM, paste and run (all in one block):
   ```bash
   sudo systemctl stop ridechain-bootstrap.service 2>/dev/null || true
   sudo gsutil -q cp gs://ridechain-bootstrap/bootstrap /opt/ridechain-bootstrap/bootstrap
   sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap
   sudo chmod 755 /opt/ridechain-bootstrap/bootstrap
   sudo systemctl start ridechain-bootstrap.service
   sudo systemctl status ridechain-bootstrap.service --no-pager
   ```
   If `gsutil` says "AccessDenied", do step 1 (grant the VM's service account **Storage Object Viewer** on the bucket). Then run the same five commands again.

4. **Test:**  
   On VM: `curl -s -X POST http://127.0.0.1:4005/register -H "Content-Type: application/json" -d '{"peerId":"test"}'`  
   From Mac: `curl -s -X POST http://bootstrap.ridechain.in/register -H "Content-Type: application/json" -d '{"peerId":"test"}'`  
   Both should return `{"status":"ok","peerId":"test"}`.

**7a. Bucket in same region as VM (e.g. Mumbai)**

If your VM is in **Mumbai (asia-south1)** and the bucket is in another region (e.g. Toronto), it still works: the VM can `gsutil cp` from the other region. You get slightly higher latency and cross-region egress. For a single binary and occasional deploys this is usually fine.

To use a **Mumbai bucket** (same region as VM, lower latency, data in India):

1. **Create a bucket in Mumbai:**  
   GCP Console → **Cloud Storage** → **Buckets** → **Create**.  
   Name (e.g. `ridechain-bootstrap-mumbai`). **Location type:** Region. **Region:** `asia-south1` (Mumbai). Create.

2. **Copy the binary from the existing bucket** (or upload a fresh build):
   ```bash
   # From your machine or from Cloud Shell (has gsutil):
   gsutil cp gs://ridechain-bootstrap/bootstrap gs://ridechain-bootstrap-mumbai/bootstrap
   ```
   Or build and upload directly to the Mumbai bucket:
   ```bash
   GOOS=linux GOARCH=amd64 go build -o bootstrap ./services/bootstrap/cmd
   gsutil cp bootstrap gs://ridechain-bootstrap-mumbai/bootstrap
   ```

3. **Grant the VM’s service account access:**  
   Bucket → **Permissions** → **Grant access**. Principal: your VM’s service account. Role: **Storage Object Viewer**. Save.

4. **Use the Mumbai bucket on the VM:**  
   In all `gsutil cp` and setup commands, use `gs://ridechain-bootstrap-mumbai/bootstrap` instead of `gs://ridechain-bootstrap/bootstrap`. If you use **bootstrap_binary_url** in VM metadata, set it to `gs://ridechain-bootstrap-mumbai/bootstrap`.

**7b. Redeploy bootstrap to GCP (update binary only)**

When you change bootstrap code (e.g. FCM high-priority fix) and want to push a new binary to the existing VM:

1. **On your machine (monorepo root):** build the Linux binary and upload to GCS:
   ```bash
   cd /path/to/chain
   GOOS=linux GOARCH=amd64 go build -o bootstrap ./services/bootstrap/cmd
   gsutil cp bootstrap gs://ridechain-bootstrap/bootstrap
   ```
2. **On the GCP VM (SSH):** stop service, pull new binary, restart:
   ```bash
   sudo systemctl stop ridechain-bootstrap.service
   sudo gsutil -q cp gs://ridechain-bootstrap/bootstrap /opt/ridechain-bootstrap/bootstrap
   sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap
   sudo chmod 755 /opt/ridechain-bootstrap/bootstrap
   sudo systemctl start ridechain-bootstrap.service
   sudo systemctl status ridechain-bootstrap.service --no-pager
   ```
3. **Verify:** `curl -s https://bootstrap.ridechain.in/health` (or your health/register endpoint).

**8. Clean slate and reinstall (same VM)**

If Caddy, Redis, or the bootstrap are broken or half-installed and you want to **wipe and reinstall on the same VM** (and you now have **bootstrap_binary_url** set in GCP metadata):

1. **On the VM** — run the clean script (from repo: `services/bootstrap/scripts/clean-bootstrap-server.sh`):
   ```bash
   # Copy the script contents to the VM or paste and run:
   sudo bash -c 'curl -sL https://raw.githubusercontent.com/YOUR_ORG/chain/main/services/bootstrap/scripts/clean-bootstrap-server.sh | bash'
   # Or paste clean-bootstrap-server.sh into a file, then:
   sudo bash /path/to/clean-bootstrap-server.sh
   ```
   This stops and disables ridechain-bootstrap, Caddy, and Redis; removes their config and data; purges the Caddy and Redis packages.

2. **In GCP** — set VM metadata **bootstrap_binary_url** = `gs://YOUR_BUCKET/bootstrap` (and optionally **bootstrap_domain** = `bootstrap.ridechain.in`). The VM reads metadata on boot; if you already have it set, skip this.

3. **On the VM** — run the setup script again:
   ```bash
   sudo bash /path/to/setup-bootstrap-server.sh
   ```
   Or paste the contents of `setup-bootstrap-server.sh` into a file and run it. The script will reinstall Redis, Caddy, download the binary from **bootstrap_binary_url**, and start all three services.

4. **Verify:** `sudo systemctl status redis-server caddy ridechain-bootstrap` — all three should be **active (running)**.

**Optional:** Use **Memorystore for Redis** (managed) and an **e2-small** VM to keep the VM light and leave Redis to GCP.

### How to use ridechain.in on Cloudflare (step-by-step)

You have the domain **ridechain.in**. To use it on Cloudflare (DNS + optional proxy/DDoS):

**1. Add the site in Cloudflare**

- Go to [dash.cloudflare.com](https://dash.cloudflare.com) → **Add a site**.
- Enter **ridechain.in** → Choose **Free** plan → Continue.
- Cloudflare will show a scan of existing DNS records (if any). You can import them or skip and add records in step 3.

**2. Change nameservers at your registrar**

- Cloudflare will show **two nameservers**, for example:
  - `ada.ns.cloudflare.com`
  - `bob.ns.cloudflare.com`
- Log in where you bought **ridechain.in** (Namecheap, BigRock, GoDaddy, etc.).
- Find **Domain nameservers** / **DNS settings** for ridechain.in.
- **Replace** the current nameservers with the two Cloudflare gives you. Save.
- Propagation can take from a few minutes up to 24–48 hours. Cloudflare will email you when the domain is active on their side.

**3. Add DNS records in Cloudflare**

- In Cloudflare: **ridechain.in** → **DNS** → **Records**.
- Add an **A** record for the **bootstrap** subdomain (so apps can call `https://bootstrap.ridechain.in`):


| Type | Name      | Content     | Proxy status                        | TTL  |
| ---- | --------- | ----------- | ----------------------------------- | ---- |
| A    | bootstrap | YOUR_VPS_IP | Proxied (orange) or DNS only (grey) | Auto |


- **Name:** `bootstrap` (full hostname will be `bootstrap.ridechain.in`).
- **Content:** The **public IP** of the VPS where Caddy + bootstrap run. **On GCP use a static external IP** (VPC → IP addresses → Promote to static) so the IP does not change on restart.
- **Proxy status:**
  - **Proxied (orange cloud):** Traffic goes through Cloudflare (DDoS, TLS at edge). Use this for the HTTP API.
  - **DNS only (grey cloud):** DNS points to your VPS but traffic does not go through Cloudflare. Use this if you want Caddy to do TLS and no Cloudflare in the path.
- Optional: add a root **A** or **CNAME** for **ridechain.in** (e.g. for a landing page) pointing to the same or another IP.

**4. WebSocket via Cloudflare (fix “timeout on port 4003”)**

If the app resolves **bootstrap.ridechain.in** to a **Cloudflare IP** (e.g. 172.67.x.x), traffic goes through Cloudflare. Cloudflare **only proxies ports 80 and 443**. So **ws://bootstrap.ridechain.in:4003** times out. Fix: proxy WebSocket **on 443** in Caddy so the app can use **wss://bootstrap.ridechain.in/rider** and **wss://bootstrap.ridechain.in/driver** (no port).

**On the VM** — add WebSocket routes to Caddy. Edit the server block so it has **handle** blocks for `/rider` and `/driver` before the main reverse_proxy:

```caddyfile
# Inside the block for :80 or bootstrap.ridechain.in, add before reverse_proxy localhost:4005:
    handle /rider* {
        reverse_proxy localhost:4003
    }
    handle /driver* {
        reverse_proxy localhost:4004
    }
    # then the existing reverse_proxy localhost:4005 ...
```

Reload Caddy: `sudo systemctl reload caddy`. Then in the app use **wss://bootstrap.ridechain.in/rider** and **wss://bootstrap.ridechain.in/driver** (gradle.properties already updated).

**5. SSL/TLS (if using proxy)**

- **SSL/TLS** → **Overview**: set encryption mode to **Full** or **Full (strict)** so Cloudflare talks to your origin (Caddy) over HTTPS. If Caddy is not yet using TLS, use **Full** and later move to **Full (strict)** when Caddy has a cert.

**6. Check it works**

- After nameservers have propagated, open `https://bootstrap.ridechain.in` (or your API path). You should hit Caddy → bootstrap.
- In the app, set the bootstrap base URL to `https://bootstrap.ridechain.in` (no trailing slash).

**Summary**


| Step | Action                                                                                 |
| ---- | -------------------------------------------------------------------------------------- |
| 1    | Add ridechain.in in Cloudflare (Free plan).                                            |
| 2    | At registrar, set nameservers to Cloudflare’s two NS.                                  |
| 3    | In GCP: reserve a **static** external IP and attach to VM; then in Cloudflare DNS add **A** record: `bootstrap` → that **static** IP; choose Proxied or DNS only. |
| 4    | SSL/TLS: Full or Full (strict) if proxy is on (or **Flexible** if origin is HTTP on 80). |
| 5    | Use `https://bootstrap.ridechain.in` in the app once DNS has propagated.               |


### I have the VM and DNS — what next?

You have a GCP VM (e.g. external IP **34.130.222.215**, internal **10.188.0.2**) and a Cloudflare **A** record: **bootstrap** (or **boot**) → **34.130.222.215**. Do the following in order.

**1. Make the external IP static (so it doesn’t change)**

- GCP Console → **VPC network** → **IP addresses**.
- Find **34.130.222.215** → if type is “Ephemeral”, click it → **Promote to static** and name it (e.g. `bootstrap-static`). Attach it to your VM if it isn’t already.

**2. Open firewall for the VM (step-by-step)**

GCP blocks incoming traffic until you add a firewall rule. Do this once:

1. In **Google Cloud Console** (console.cloud.google.com), open the **☰** menu (top left) → **VPC network** → **Firewall** (or search for “Firewall” in the top search box and open **Firewall** under VPC network).
2. Click **+ CREATE FIREWALL RULE** (top of the page).
3. Fill in:
  - **Name:** `allow-bootstrap-http` (any name you like).
  - **Network:** leave **default** (or the VPC your VM uses).
  - **Direction:** **Ingress** (already selected).
  - **Action on match:** **Allow**.
  - **Targets:** choose **All instances in the network** (easiest — no tags needed).  
  *(If you prefer “Specified target tags”, type e.g. `bootstrap`, then on your VM you must add the same tag in the VM’s “Network tags”.)*
  - **Source filter:** **IPv4 ranges**.
  - **Source IPv4 ranges:** type `0.0.0.0/0` (allows the whole internet; fine for testing).
  - **Protocols and ports:**  
    - Check **Specified protocols and ports**.  
    - In **tcp** type: `80, 443`  
    - If your app will call the VM directly (not only via Cloudflare), also add: `80, 443, 4003, 4004, 4005` (one comma‑separated list in the **tcp** box).  
    - Leave **udp** empty unless you need native P2P (port 4001); then add a second rule or in **udp** put `4001`.
4. Click **CREATE**.

After this, traffic on ports 80 and 443 (and 4003–4005 if you added them) can reach any VM in that network. No need to “attach” the rule to one VM if you chose **All instances in the network**.

**3. Run the stack on the VM (if not already)**

**When to use `systemctl status redis-server caddy ridechain-bootstrap`:** Only **after** Redis, Caddy, and the bootstrap are installed. That command only **checks** whether the three services are running; it does not install anything. If your VM has no code yet, do the “Install” step below first, then use the status command to verify.

- **If you used the startup script at VM creation:** After the VM has booted and the script has run (wait a few minutes), SSH in and run `systemctl status redis-server caddy ridechain-bootstrap` — all three should show **active**. If the bootstrap binary wasn’t installed (no `bootstrap_binary_url` / `bootstrap_repo` metadata), build locally, upload to GCS, download on the VM to `/opt/ridechain-bootstrap/bootstrap`, then `sudo systemctl start ridechain-bootstrap`.
- **If your VM is empty (no script was run):** Install the stack first:
  1. SSH into the VM.
  2. Run the startup script once (see “GCP: Startup script and custom image”): paste the contents of `services/bootstrap/scripts/setup-bootstrap-server.sh` into a file, e.g. `sudo nano /tmp/setup.sh`, paste, save, then `sudo bash /tmp/setup.sh`. Or download it: `curl -sL https://raw.githubusercontent.com/YOUR_ORG/chain/main/services/bootstrap/scripts/setup-bootstrap-server.sh | sudo bash` (if the repo is public). You must set up the binary (e.g. metadata `bootstrap_binary_url` or `bootstrap_repo`, or manually copy the binary to `/opt/ridechain-bootstrap/bootstrap`).
  3. **Then** run `systemctl status redis-server caddy ridechain-bootstrap` to confirm all are active.

**4. Caddy and domain (so HTTPS works)**

- If you use **Cloudflare proxy (orange cloud):** Caddy can stay on HTTP (port 80) and Cloudflare will do TLS at the edge. Set **SSL/TLS** → **Overview** → **Full** (or **Full (strict)** once Caddy has a cert).
- If you want **Caddy to do TLS** (DNS only / grey cloud): on the VM set metadata `bootstrap_domain=bootstrap.ridechain.in` and ensure Caddy is configured for that hostname (the startup script does this when that metadata is set). Or manually add a Caddyfile server block for `bootstrap.ridechain.in` and reload Caddy.

**5. Test from your machine**

```bash
# If Cloudflare proxy is ON (orange cloud) — use the hostname (Cloudflare will resolve to your IP)
curl -s https://bootstrap.ridechain.in/health

# If DNS only (grey cloud) and Caddy serves HTTP only
curl -s http://bootstrap.ridechain.in/health

# Or by IP (if firewall allows and Caddy listens on 80)
curl -s http://34.130.222.215/health
```

- The bootstrap HTTP API may expose a path like **/health** or **/api/health**; if not, try **/register** with a GET (you might get 405 Method Not Allowed, which still shows Caddy → bootstrap is up).

**6. Use the URL in the app**

- In the rider (and driver) app, set the bootstrap base URL to `**https://bootstrap.ridechain.in`** (or `**http://...**` if you’re not using TLS yet). No trailing slash.

**Service not running? Check and install the bootstrap binary**

If you ran the startup script **without** `bootstrap_binary_url` or `bootstrap_repo` metadata, Redis and Caddy are installed but the **bootstrap binary is missing** (the script created an empty placeholder). The service is “installed” (systemd unit exists) but won’t run until a real binary is in place.

**1. On the VM — see what’s running**

```bash
# All three should be "active (running)". If ridechain-bootstrap is "inactive" or failed, the binary is missing or broken.
sudo systemctl status redis-server caddy ridechain-bootstrap

# Check if the binary exists and has size (not 0)
ls -la /opt/ridechain-bootstrap/bootstrap
```

If `ridechain-bootstrap` is **inactive** or **failed** and `/opt/ridechain-bootstrap/bootstrap` is **0 bytes** or missing, do step 2.

**2. On your local machine — build the binary**

From the **monorepo root** (the folder that contains `services/bootstrap`):

```bash
cd /path/to/chain
GOOS=linux GOARCH=amd64 go build -o bootstrap ./services/bootstrap/cmd
```

You should get a file `**bootstrap**` (many MB). That is the binary the VM needs.

**3. Copy the binary to the VM**

Replace `YOUR_VM_USER` with the user you use to SSH (e.g. the name GCP shows, or `root` if you SSH as root), and `YOUR_VM_IP` with the VM’s external IP (e.g. `34.130.222.215`):

```bash
scp bootstrap YOUR_VM_USER@YOUR_VM_IP:/tmp/bootstrap
```

**4. On the VM again — install the binary and start the service**

```bash
sudo mv /tmp/bootstrap /opt/ridechain-bootstrap/bootstrap
sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap
sudo chmod +x /opt/ridechain-bootstrap/bootstrap
sudo systemctl start ridechain-bootstrap
sudo systemctl status ridechain-bootstrap
```

You should see **active (running)**. Then test from your machine:

```bash
curl -s http://YOUR_VM_IP/health
# or
curl -s https://bootstrap.ridechain.in/health
```

If the bootstrap HTTP API has a `/health` route, you’ll get a response. If not, try `curl -s http://YOUR_VM_IP/` or `/register` to confirm Caddy is proxying to the bootstrap.

**Summary**


| Step | Action                                                                            |
| ---- | --------------------------------------------------------------------------------- |
| 1    | Reserve static external IP (VPC → IP addresses → Promote to static); attach to VM. |
| 2    | Firewall: allow tcp 80, 443 (and 4003–4005 if needed).                            |
| 3    | On VM: ensure Redis, Caddy, and bootstrap are running (startup script or manual). |
| 4    | Cloudflare: SSL/TLS Full if proxy on; or configure Caddy for TLS if DNS only.     |
| 5    | Test with `curl https://bootstrap.ridechain.in/health` (or http).                 |
| 6    | Set app bootstrap URL to `https://bootstrap.ridechain.in`.                        |


---

## 11. Checklist — secure deployment (no login)

- **Domain:** Bought at a registrar (Cloudflare, Namecheap, BigRock, etc.); DNS at Cloudflare or registrar.
- **DNS:** A record `bootstrap.ridechain.in` (or your subdomain) → VPS public IP.
- **TLS:** Caddy Auto TLS (Let’s Encrypt); HTTPS for API, WSS for WebSocket. Cloudflare optional for edge TLS/DDoS.
- **DDoS / abuse:** Caddy rate limiting (and connection limits); add Cloudflare only if you need edge absorption.
- **Rate limiting:** Caddy (or Cloudflare) per IP (and path); tune for register/discover/search.
- **Logging:** Caddy access logs; bootstrap app logs (JSON) to stdout or file.
- **Secrets:** `BOOTSTRAP_IDENTITY_SEED`, Redis URL, FCM key in env/secret manager; never in client apps.
- **Optional:** API key for server-side callers; validate in Caddy or in bootstrap HTTP handler.
- **No gRPC** for bootstrap unless you add internal services that need it.
- **No Kafka** on bootstrap for core operation; optional later for analytics only.

This gives you **non-login (anonymous peer) usage** while staying **protected and deployable securely** for the bootstrap server.


GOOS=linux GOARCH=amd64 go build -o bootstrap ./services/bootstrap/cmd


sudo systemctl stop ridechain-bootstrap.service
sudo gsutil -q cp gs://ridechain-bootstrap/bootstrap /opt/ridechain-bootstrap/bootstrap
sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap
sudo chmod 755 /opt/ridechain-bootstrap/bootstrap
sudo systemctl start ridechain-bootstrap.service
sudo systemctl status ridechain-bootstrap.service --no-pager