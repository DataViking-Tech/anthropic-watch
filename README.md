# anthropic-watch

A tiny Cloudflare Worker that polls
[status.anthropic.com](https://status.anthropic.com/) every 2 minutes and DMs
Wesley on Slack via [alerthub-ingress](https://github.com/DataViking-Tech/alert-hub)
when the Anthropic API degrades.

The whole point of this Worker is **Anthropic-outage Disaster Recovery**: when
Claude API is degraded, mayors may go silent. The notify path here uses
**Gemini 2.5 Flash via OpenRouter** to summarise — _no Claude / Anthropic SDK
anywhere in the hot path_ — so the alert lands even when Anthropic is down.

## Architecture

```
       ┌─────────────────────────────────────┐
       │  Cron Trigger (*/2 * * * *)         │
       └────────────────┬────────────────────┘
                        ▼
        ┌────────────────────────────────────┐
        │ status.anthropic.com summary.json  │
        └────────────────┬───────────────────┘
                        ▼
                  diff vs KV
              (last_indicator,
               last_seen_incidents)
                        │
              transition? ─── no ──▶ update KV, exit
                        │
                       yes
                        ▼
              Gemini 2.5 Flash via
              OpenRouter (terse JSON
              {title, body})
                        │
                        ▼
         HMAC-signed POST /alert
              alerthub-ingress
              (proactive:anthropic-watch)
                        │
                        ▼
         Slack DM to Wesley via
              alerthub-fanout
```

## Producer identity

This Worker is registered with Alert Hub as a **proactive producer**:

| Field          | Value                          |
| -------------- | ------------------------------ |
| `producer_type`| `proactive`                    |
| `producer_id`  | `anthropic-watch`              |
| HMAC env var   | `HMAC_PROACTIVE_ANTHROPIC_WATCH` |
| Origin header  | `proactive:anthropic-watch`    |

Signing matches `verifyProducerHmac` in alert-hub (`src/lib/hmac.ts`):

```
X-Gastown-Origin:    proactive:anthropic-watch
X-Gastown-Timestamp: <ISO-8601>
X-Gastown-Signature: hex sha256( HMAC(secret, `${timestamp}\n${body}`) )
```

## Kill switch

`ANTHROPIC_WATCH_ENABLED` must be `"true"` or the cron handler early-returns.
Defaults to `"false"` so first-deploy is safe. Flip it via:

```sh
echo true | wrangler secret put ANTHROPIC_WATCH_ENABLED
```

## Secrets

Set once with `wrangler secret put`:

| Secret                          | Source                                  |
| ------------------------------- | --------------------------------------- |
| `OPENROUTER_API_KEY`            | Doppler `gastown/prd`                   |
| `HMAC_PROACTIVE_ANTHROPIC_WATCH`| Mint fresh; mirror to `alerthub-ingress` |
| `ANTHROPIC_WATCH_ENABLED`       | `"true"` after smoke test               |

## KV namespace

```sh
wrangler kv namespace create ANTHROPIC_WATCH_STATE
# Paste the id into wrangler.toml under [[kv_namespaces]].
```

## Smoke test

1. Deploy with `ANTHROPIC_WATCH_ENABLED=false`.
2. Seed a fake "degraded" baseline so the next tick reads a transition:
   ```sh
   wrangler kv key put --binding=STATE watch_state_v1 \
     '{"last_indicator":"none","last_seen_incident_ids":["FAKE-SEED-ID"],"last_tick_at":"2026-05-12T00:00:00Z"}'
   echo true | wrangler secret put ANTHROPIC_WATCH_ENABLED
   curl https://anthropic-watch.<account>.workers.dev/tick
   ```
3. Confirm Slack DM lands within 30s.
4. Reset KV to actual current status (or just let next cron tick overwrite).

## Deploys

Production deploys run via `.github/workflows/deploy.yml` on push to `main`.
First-deploy bootstrap can be a single direct `wrangler deploy` (one-time
exception per Wesley's no-hot-architecture-changes rule). All subsequent
deploys go through the GitHub Actions pipeline.

## License

MIT — see [LICENSE](./LICENSE).
