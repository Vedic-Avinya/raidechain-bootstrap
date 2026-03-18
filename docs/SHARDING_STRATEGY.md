# Sharding strategy for Gossipsub topics

## Implemented: **shard by city only (riders)**

- **Rider bridge:** Riders are sharded **by city**. Topic format: `/ridechain/{city}/p2p/v1`. When a rider connects with `?city=bangalore`, the bootstrap joins that city's topic on demand and relays only that topic's messages to riders in that city. This keeps each city's Gossip mesh small and local.
- **Rider app:** Does **not** run go-libp2p or a 9-cell TopicManager. It connects via WebSocket to the bootstrap and sends `city` (derived from discovery location displayName) in the URL. So "like driver-android" for **topic locality** is achieved by **city in the URL** and bootstrap joining per-city topics; we do **not** use a 9-cell window for riders.
- **Driver app (future):** Per CLAUDE.md, one topic per (city, geohash_cell, type). Many drivers in the same cell subscribe to the same topic. No automatic split when subscriber count grows.

---

## When to consider further sharding

Consider sharding (or multiple topics) when:

- **Rider bridge:** One topic has too many peers (e.g. mesh size or fan-out causes latency/memory issues), or we need to isolate regions/cities for legal or latency reasons.
- **Driver mesh:** We already shard by cell; if a **single cell** has too many drivers, we could later split by a sub-cell or by "channel" (e.g. ride vs delivery), but only with data to justify it.

---

## Possible strategies (for later)

| Strategy | When | How |
|----------|------|-----|
| **By city** | ✅ **Done.** | One topic per city: `/ridechain/{city}/p2p/v1`. Bootstrap rider bridge joins on demand; rider app sends `?city=` from discovery location. |
| **By region** | Very large scale | One topic per region (e.g. state or cluster of cities). |
| **By use case** | Separate chat vs ride vs delivery | For rider bridge we could split e.g. `/ridechain/{city}/car_walkie/v1` if traffic justifies it. |
| **Multiple bootstrap nodes** | High availability or geo-latency | Run several bootstrap nodes; each joins the same topic(s). No topic sharding, just more capacity. |

---

## Summary

| Question | Answer |
|----------|--------|
| Shard by city? | **Yes.** Rider bridge joins `/ridechain/{city}/p2p/v1` per city; rider app passes city from discovery location. |
| 9-cell for riders? | **No.** Riders use one topic per city (shard by city only). Driver app (future) uses 9-cell per CLAUDE.md. |
| When to revisit? | When a single city's mesh is too large (metrics); then consider region or multiple bootstrap nodes. |

---

## New city vs India topic

**When a user registers or sets discovery location to a new city, are they on that city's topic or still "India"?**

- They are on **that city's topic**. The rider app sends `?city=` in the WebSocket URL (derived from the discovery location / place name). The bootstrap rider bridge joins `/ridechain/{city}/p2p/v1` **on demand** per city. So Bangalore → `/ridechain/bangalore/p2p/v1`, Hyderabad → `/ridechain/hyderabad/p2p/v1`. The default topic `/ridechain/in/p2p/v1` is used for the **driver** bridge and broadcast; riders are **not** all on "India" — they are on the city topic they connected with.

**Can a single topic handle millions of users?**

- **In theory:** Gossipsub meshes can scale to large peer counts, but fan-out and message volume grow with mesh size. A single topic with millions of **publishers** would stress the mesh (gossip, I/O, memory).
- **In practice:** We **shard by city**, so each city has its own topic. No single topic holds "all India" riders. If one **city** (e.g. Mumbai) had millions of concurrent riders on `/ridechain/mumbai/p2p/v1`, that single topic could become a bottleneck; then we'd revisit **region** sharding or multiple bootstrap nodes per city (see "When to revisit?" above).
