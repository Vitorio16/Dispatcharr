# Profile-aware hot standby (opt-in)

This branch adds a second, already-connected upstream for actively viewed live
Proxy HTTP/HTTPS MPEG-TS channels. It is disabled by default. It reduces upstream
startup delay on failover; it does **not** promise seamless playback or zero
buffering in Plex.

## Operation and scope

The active input must first deliver recognizable MPEG-TS framing. The manager
must have a known channel, worker owner, and M3U profile assignment. Socket I/O
must be gevent patched, as in the normal streaming worker. Preview streams,
FFmpeg/transcoding, HLS input, RTSP/RTP and UDP retain their existing paths.

For eligible live channels, candidates are active profiles of the current
stream's active M3U account, followed by the other assigned streams rotated in
`channelstream__order`. Each stream's default profile is considered first, then
other profiles by ID. The active `(stream_id, profile_id)` is excluded. Profile
and credential pool limits are checked again at reservation time. No provider
`active_cons` field participates in local accounting. XC URLs use the existing
credential transformation/playback URL resolver.

A spare moves through in-memory states `RESERVED`, `WARMING`, `READY` and
`PROMOTING`. READY requires at least three complete 188-byte packets with valid
TS headers and recent subsequent media. HTTP status alone is insufficient. The
reader continues draining and discarding packets while idle. It retains at most
563 synchronization bytes before lock-on, then at most 187 incomplete packet
bytes between reads. One bounded HTTP chunk is processed at a time. There is no
second media queue or Redis media buffer. A dead/stalled spare is closed and
released; the next pair is attempted while the active source keeps playing.

When the active reader ends or the existing inactivity threshold expires, a
READY spare is promoted before normal reconnect backoff. The old input is closed,
the existing buffer's discontinuity handler flushes complete old packets and
drops its partial tail, and the spare's existing readable pipe is attached to
the manager. Its requests response is preserved: promotion makes no second
upstream GET. Only the manager writes to StreamBuffer. A replacement spare is
prepared on the next maintenance pass (normally within 250 ms).

The manager's explicit upstream state distinguishes `HOT_PROMOTED` (existing
spare attached), `CONNECTED` (ordinary connection/reconnect established), and
`CONNECTION_REQUIRED` (cold switch, failed attachment or disconnected input).
The run loop resumes an attached source directly, including when the spare only
became ready after the inner data loop exited. Recovery methods retain their
boolean success contract; callers do not use that boolean to infer whether a
new connection is needed. A cold switch still goes through normal establishment.

If no spare is ready, normal retries continue. The supported path's eventual
cold switch considers stream/profile pairs in the same order. Unsupported
transports can still use the legacy FFmpeg failover path. Failed pairs enter a
temporary monotonic-clock cooldown and can be tried again afterward. Capacity
exhaustion is not treated as a permanent health failure.

## Resource implications

One active plus one spare means up to two provider connections and approximately
twice the upstream bandwidth per active channel. All viewers sharing that live
channel still share its single active/spare pair. Insufficient capacity simply
leaves the channel without a spare. Profiles sharing credentials in a ServerGroup
continue to share its enforced limit.

`max_streams=0` means unlimited, never zero capacity. Reservations now count
unlimited connections in the existing `profile_connections:{id}` counter as well;
zero still bypasses the existing credential-limit policy. This ensures releases
and same-profile handoffs balance. Deploy/restart all streaming workers together
so old, uncounted unlimited reservations are not mixed with the new accounting.

## Configuration

Set these environment variables on the Dispatcharr streaming containers and
recreate them. No database migration or UI change is required.

| Variable | Default | Meaning |
| --- | --- | --- |
| `HOT_STANDBY_ENABLED` | `false` | Set to `true` to opt in |
| `HOT_STANDBY_WARMUP_TIMEOUT` | `10` | Maximum wall-clock seconds to get valid TS |
| `HOT_STANDBY_IDLE_TIMEOUT` | `10` | Seconds without valid standby media before replacement |
| `HOT_STANDBY_COOLDOWN` | `60` | Seconds to skip a failed stream/profile pair |
| `HOT_STANDBY_SEARCH_INTERVAL` | `5` | Seconds between searches when no spare can be reserved |

Timeouts/cooldown have a one-second minimum; search interval has a 250 ms minimum.
Use finite positive numeric values. Existing active connection/startup timeout
settings still control failure detection. Raising them also raises stall-to-
promotion latency. Disabling the feature means setting the flag to `false` and
recreating the containers; existing sessions are stopped by normal teardown.

## Reservation and crash recovery

Active assignment keys remain `channel_stream:{channel_pk}` and
`stream_profile:{stream_id}`, with the active profile in channel metadata.
The separate `channel_standby:{channel_uuid}` JSON record contains a unique token,
worker ID, stream ID, profile ID and creation time. It contains no URL or login.
The durable record denotes an owned reservation; readiness is intentionally local
to the live socket rather than persisted as stale Redis health information.

The existing `reserve_profile_slot` and `release_profile_slot` WATCH helpers now
accept guard/commit hooks. A reserve commits the token and the existing counters
together. Release compares the token and consumes it together with the counter
decrements. Repeating cleanup, or cleaning a token already promoted, is a no-op.

Promotion uses the existing release transaction, guarded by owner, stopping flag,
active assignment, metadata and the standby token. It transfers the existing
spare reservation to the active assignment and decrements the old reservation.
It never reserves the promoted profile again. Queuing removal of the token
cannot expose an intermediate missing-token/incomplete-assignment state: all
writes commit at EXEC. If EXEC's reply is lost, the coordinator reads back the
committed assignment. If attaching the pipe fails after commit, the slot remains
owned by the active assignment and normal reconnect/active teardown handles it.
If Redis is also unavailable for read-back, the manager ends rather than
reconnecting an old source whose reservation may already have been released.
The durable active/spare token remains available to normal teardown/recovery.

stop(), finally, ownership loss and warm-up failure close the reader and attempt
idempotent release. Channel/server teardown also recovers the durable token even
if the channel row has been deleted. A replacement manager can reclaim a
predecessor's token. The existing worker cleanup loop scans orphan tokens once
per minute, waits at least 60 seconds from creation, and releases only when that
worker's 30-second heartbeat is absent. Thus a hard-killed worker is eventually
reaped even if no viewer returns. Tokens have **no TTL**: expiring a release token
without decrementing its counters would leak a slot. Redis outages defer cleanup;
do not manually delete tokens/counters while streams are running.
The local worker also reaps aged tokens that no open local coordinator owns,
covering failed cleanup during a manual switch without waiting for worker death.

## Troubleshooting and limitations

At INFO look for `Hot standby warming`, `Hot standby ready`, `Promoting hot
standby`, `Hot standby promoted`, and `Preparing replacement standby`. WARN logs
identify failed candidates/readers. Exhausting current-stream candidates logs
the move to the next assigned stream. New lifecycle logs contain channel/stream/
profile IDs and exception types, not full upstream URLs or exception text.
Existing URL resolution and active-reader logs elsewhere in Dispatcharr can
still contain URLs; this change does not globally redact legacy logging.

Same-stream promotions emit `stream_switch` with reason
`hot_standby_promotion`, unchanged previous/current stream IDs, and old/new M3U
profile IDs. A cold profile switch uses `profile_failover`.

No spare may mean the flag is off, an unsupported input, unknown assignment,
unpatched runtime, full pool, all pairs cooling down, or failed TS validation.
The feature does not synthesize PAT/PMT/keyframes, normalize PCR/PTS/DTS, align
encoders or guarantee decodability at the first promoted packet. It reuses the
existing TS discontinuity stamping/sidecar. A client may wait for program tables
or a keyframe, and existing failure detection and old-input shutdown take time.
Hard Redis data loss is subject to the existing pool's persistence guarantees.
Do not run mixed old/new workers on the same Redis/database during rollout.

## Build and deploy with Docker Compose

Run on a Linux Docker host with this **edited checkout** available. Changes are
local until pushed; cloning the remote branch alone will not include local
commits. From the repository root:

```sh
git branch --show-current             # must say hot-standby-failover
git log -2 --oneline
docker build --pull -f docker/Dockerfile -t dispatcharr:hot-standby-failover .
```

The Dockerfile defaults to the official
`ghcr.io/dispatcharr/dispatcharr:base` runtime. Build for the deployment host's
architecture. For another host, transfer the image with `docker save -o
dispatcharr-hot-standby.tar dispatcharr:hot-standby-failover` and `docker load -i
dispatcharr-hot-standby.tar`, or use your own registry.

Before replacing the official instance, record its image and preserve it locally:

```sh
docker inspect --format '{{.Config.Image}} {{.Image}}' dispatcharr
docker image tag "$(docker inspect --format '{{.Image}}' dispatcharr)" dispatcharr:rollback-before-standby
```

Back up the database/data using Dispatcharr's backup feature. For an AIO cold
copy, stop it with its existing Compose file (replace `compose.yml` below with
your actual file), then copy `/data` from the stopped container:

```sh
docker compose -f compose.yml stop dispatcharr
docker cp dispatcharr:/data ./dispatcharr-data-before-hot-standby
```

Keep your existing Compose project name, volume/bind mounts, ports, devices,
credentials and other service settings. Save this as `compose.hot-standby.yml`
beside that Compose file:

```yaml
services:
  dispatcharr:
    image: dispatcharr:hot-standby-failover
    pull_policy: never
    environment:
      HOT_STANDBY_ENABLED: "true"
      HOT_STANDBY_WARMUP_TIMEOUT: "10"
      HOT_STANDBY_IDLE_TIMEOUT: "10"
      HOT_STANDBY_COOLDOWN: "60"
      HOT_STANDBY_SEARCH_INTERVAL: "5"
```

```sh
docker compose -f compose.yml -f compose.hot-standby.yml config
docker compose -f compose.yml -f compose.hot-standby.yml up -d --no-build --pull never dispatcharr
docker logs --since 5m dispatcharr
```

For a new installation the repository's AIO Compose file is
`docker/docker-compose.aio.yml`. For an existing installation keep its existing
file/project; using a different project can silently select a different volume.
For modular deployments recreate every streaming worker with the same code and
environment; preserve the existing PostgreSQL and Redis services.

Validate one channel with multiple profiles and two available connection slots:
wait for READY, interrupt the active upstream, verify the promoted profile and
the replacement warm-up, and watch Plex through recovery. Repeat for no-capacity,
failed credentials, standby EOF/stall, stop and worker restart. Inspect both
profile and shared-credential counters before/after. No real-provider/Plex soak
test has been performed by the local automated tests.

## Portainer (Docker Standalone)

1. Build/load the image on the Docker endpoint managed by Portainer, not merely
   on the machine running the browser. Preserve the old image and backup above.
2. Open **Stacks**, select the existing Dispatcharr stack, then **Editor**.
   Preserve its existing mounts, stack name, ports and all other settings.
3. Change the Dispatcharr service image to `dispatcharr:hot-standby-failover`, add
   `pull_policy: never`, and add the environment variables above. Do not paste a
   second independent stack over the existing installation.
4. Choose **Update the stack**, with **Re-pull image and redeploy** disabled for
   this locally built tag. Inspect the container logs and validate the channel.
   A Git-managed stack must instead update its source Compose configuration or
   use Portainer's supported detach/edit workflow.

For Swarm/multiple nodes, publish to your private registry and use an immutable
registry tag/digest available on every node; the local-image procedure above is
for Standalone. UI labels vary by Portainer version.

References: [Portainer stack editing](https://docs.portainer.io/user/docker/stacks/edit),
[Compose pull policy](https://docs.docker.com/reference/compose-file/services/#pull_policy),
[Compose up](https://docs.docker.com/reference/cli/docker/compose/up/).

## Rollback to official Dispatcharr

1. Stop all viewing sessions through Dispatcharr first so the branch closes and
   releases active/spare reservations. Set the feature to `false` and recreate
   branch containers if you only want to disable it.
2. To restore the exact previous official image, change the service image to
   `dispatcharr:rollback-before-standby`, remove the HOT_STANDBY variables, and
   keep `pull_policy: never`. Recreate using the same Compose file/project and
   `/data` volume. In Portainer use the same stack Editor/Update flow, without
   re-pulling the local rollback tag.
3. Alternatively restore the original pinned official registry image/digest
   recorded before deployment. Pull that exact image, restore the original
   Compose configuration and remove the standby override. Do not assume the
   moving `latest` tag is the same release you previously ran.
4. This feature adds no database migrations. If other branch history or an image
   upgrade changed the schema, restore the cold `/data` backup before starting
   the older image. Never run two AIO containers against the same data directory.
5. If a hard crash prevented teardown, first run this branch with the flag off
   and let its janitor reclaim dead-worker reservations before returning to old
   code, which does not understand the standby ledger. For modular Redis, retain
   that cleanup-capable worker long enough for the heartbeat/grace window and
   verify the ledger is empty. Do not use FLUSHDB on shared Redis.

## Validation commands on the supported runtime

From a Linux base-image environment with the repository mounted as `/app`:

```sh
bash scripts/ci_bootstrap_backend.sh apps.proxy.live_proxy.tests apps.m3u.tests.test_connection_pool -v2
python -c "from gevent import monkey; monkey.patch_all(); import unittest; result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromName('apps.proxy.live_proxy.tests.test_hot_standby.CooperativeHTTPTests')); raise SystemExit(not result.wasSuccessful())"
```

The bootstrap needs the base image's PostgreSQL/Redis packages and creates an
isolated test database. Do not execute the service-starting bootstrap inside
your live production container. Local Windows compatibility results are recorded
in the implementation report; they do not replace these production-runtime tests.
