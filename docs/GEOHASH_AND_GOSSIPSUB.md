# Geohash and Gossipsub at District Level

## How geohash works (district vs cell)

- **Geohash cells (S2 Level 8 / ~600 m):** Used for **driver-side** P2P topics (when we build the driver app). Each driver subscribes to a **9-cell window** (current cell + 8 neighbours). Format: `/ridechain/{city}/{geohash_l8}/{type}/v1` (see driver app `TopicManager` + `GeohashUtil`).
- **District:** A **display-only** label derived from the **first 4 characters** of a geohash (e.g. `tdr2` → "Hyderabad"). Used in bootstrap **API responses** (e.g. discover) so the app can show "Hyderabad" or "Bangalore". It is **not** used for topic creation or storage. See `redis.DistrictForGeohash()` in `internal/redis/store.go`.

So **geohash at “district level”** here means: we use a short geohash **prefix** only to map to a human-readable district name for UI. All discovery and storage use **full coordinates** (or full geohash) and a single **Redis GEO index**; there are no per-district indexes or per-district topics.

## Where is Gossipsub stored, and when is it created?

| Where | What is stored | When created |
|-------|----------------|--------------|
| **Bootstrap server** | One in-memory Gossipsub instance; **one topic** joined at startup. Default: `/ridechain/in/p2p/v1` (India P2P). Set `BOOTSTRAP_GOSSIPSUB_TOPIC` to override. | **Once at process start.** Not created when a new district or user is added. |
| **Driver phones** | Each driver runs go-libp2p with Gossipsub. Topics are **subscribed in memory** per 9-cell window (no persistent “storage”). | When the driver app comes online and calls `TopicManager.updateSubscriptions(city, lat, lng)`. (Out of scope for current rider-only focus.) |
| **Rider app / bridge** | Riders connect via WebSocket with `?city=`. Bootstrap joins **one topic per city** (`/ridechain/{city}/p2p/v1`) on demand. No 9-cell; shard by city only. | When first rider from that city connects. |

So:

- **Topics are not stored** in a DB. Gossipsub mesh state lives **in memory** on bootstrap and on each driver node.
- **Bootstrap:** One topic is **created once** at startup. New districts or new users do **not** create new topics.
- **Drivers:** A topic is “created” in the sense that when a driver subscribes to a cell, that topic may be created by the Gossipsub implementation if it’s the first subscriber in the mesh; it’s still **in-memory, no DB**.

## If many users subscribe to a topic, is another one created at runtime?

- **Bootstrap:** No. There is a single topic. All bridge-connected riders and drivers share it. More subscribers = more peers in the **same** topic’s mesh; no automatic sharding or new topics.
- **Driver app (future production):** Per CLAUDE.md, each **geohash cell** (and type) has one topic. Many drivers in the same cell subscribe to the **same** topic; Gossipsub forms a single mesh for that topic. The system does **not** create extra topics or shards when subscriber count grows. If we need to scale later, we’d do it by design (e.g. more cells, more cities), not by “creating another topic at runtime” for the same cell.

## Summary

| Question | Answer |
|----------|--------|
| Geohash at district level? | District = 4-char geohash prefix → label only. Discovery uses full lat/lng (Redis GEO). |
| Where is Gossipsub stored? | In memory only (bootstrap + driver nodes). No DB. |
| When is a topic created? | Bootstrap: **once at startup** (India default `/ridechain/in/p2p/v1`). Driver: when they subscribe to a cell (in-memory). |
| New district → new topic? | No. Bootstrap uses one topic; district is not used for topic creation. |
| Many users → new topic/shard? | No. Same topic; more peers in the mesh. See [SHARDING_STRATEGY.md](./SHARDING_STRATEGY.md) for when to revisit. |
