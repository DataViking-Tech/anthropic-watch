/**
 * anthropic-watch — Cloudflare Worker.
 *
 * Polls https://status.anthropic.com/api/v2/summary.json every 2 minutes on a
 * Cron Trigger. Detects state transitions vs. the previous tick (stored in KV),
 * has Gemini 2.5 Flash via OpenRouter draft a one-line summary, and posts an
 * HMAC-signed AlertEvent to alerthub-ingress as a `proactive:anthropic-watch`
 * producer.
 *
 * The LLM substrate is intentionally NOT Anthropic — the whole point is for
 * the notify path to survive a Claude outage.
 *
 * Kill switch: ANTHROPIC_WATCH_ENABLED must equal "true" or the cron handler
 * early-returns. Defaults to "false" per Wesley policy (paging features ship off).
 */

export interface Env {
  STATE: KVNamespace;

  // Vars (wrangler.toml [vars])
  ANTHROPIC_WATCH_ENABLED: string;
  OPENROUTER_BASE_URL: string;
  OPENROUTER_MODEL: string;
  ALERTHUB_INGRESS_URL: string;
  PRODUCER_TYPE: string;
  PRODUCER_ID: string;
  PRODUCER_VERSION: string;
  TARGET_CHANNEL: string;
  STATUS_URL: string;

  // Secrets (wrangler secret put)
  OPENROUTER_API_KEY: string;
  HMAC_PROACTIVE_ANTHROPIC_WATCH: string;
}

const SCHEMA_VERSION = "1.0.0" as const;
const STATE_KEY = "watch_state_v1";

type Indicator = "none" | "minor" | "major" | "critical" | "maintenance";

interface StatusSummary {
  status?: { indicator?: string; description?: string };
  incidents?: Array<{
    id: string;
    name: string;
    impact?: string;
    status?: string;
    shortlink?: string;
    incident_updates?: Array<{ status?: string; body?: string; created_at?: string }>;
  }>;
  components?: Array<{ id: string; name: string; status?: string }>;
}

interface WatchState {
  last_indicator: Indicator;
  last_seen_incident_ids: string[];
  last_tick_at: string;
}

interface TransitionContext {
  prev_indicator: Indicator;
  next_indicator: Indicator;
  new_incidents: NonNullable<StatusSummary["incidents"]>;
  resolved_incident_ids: string[];
  description: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// Severity mapping. status.anthropic.com indicator → AlertEvent severity.
//   none      → (no alert, unless recovery from non-none)
//   minor     → warning
//   major     → critical
//   critical  → page
//   maintenance → info
// Recovery (transition back to "none") fires "info".
// ─────────────────────────────────────────────────────────────────────────────
function severityFor(next: Indicator, prev: Indicator): "info" | "warning" | "critical" | "page" {
  if (next === "none") return "info";
  if (next === "maintenance") return "info";
  if (next === "minor") return "warning";
  if (next === "major") return "critical";
  if (next === "critical") return "page";
  // Unknown indicator — bias toward warning rather than silent.
  void prev;
  return "warning";
}

function normaliseIndicator(raw: string | undefined): Indicator {
  switch ((raw ?? "").toLowerCase()) {
    case "none":
    case "minor":
    case "major":
    case "critical":
    case "maintenance":
      return raw!.toLowerCase() as Indicator;
    default:
      return "none";
  }
}

async function loadState(env: Env): Promise<WatchState> {
  const raw = await env.STATE.get(STATE_KEY);
  if (!raw) {
    return { last_indicator: "none", last_seen_incident_ids: [], last_tick_at: "" };
  }
  try {
    const parsed = JSON.parse(raw) as Partial<WatchState>;
    return {
      last_indicator: normaliseIndicator(parsed.last_indicator as string | undefined),
      last_seen_incident_ids: Array.isArray(parsed.last_seen_incident_ids)
        ? parsed.last_seen_incident_ids.filter((x): x is string => typeof x === "string")
        : [],
      last_tick_at: typeof parsed.last_tick_at === "string" ? parsed.last_tick_at : "",
    };
  } catch {
    return { last_indicator: "none", last_seen_incident_ids: [], last_tick_at: "" };
  }
}

async function saveState(env: Env, state: WatchState): Promise<void> {
  await env.STATE.put(STATE_KEY, JSON.stringify(state));
}

function diffState(prev: WatchState, summary: StatusSummary): TransitionContext | null {
  const nextIndicator = normaliseIndicator(summary.status?.indicator);
  const description = summary.status?.description ?? "";
  const incidents = (summary.incidents ?? []).filter((i) => i && typeof i.id === "string");
  const currentIds = new Set(incidents.map((i) => i.id));
  const prevIds = new Set(prev.last_seen_incident_ids);

  const newIncidents = incidents.filter((i) => !prevIds.has(i.id));
  const resolved: string[] = [];
  for (const id of prevIds) if (!currentIds.has(id)) resolved.push(id);

  const indicatorChanged = nextIndicator !== prev.last_indicator;
  const hasNewIncidents = newIncidents.length > 0;
  const hasResolutions = resolved.length > 0;

  if (!indicatorChanged && !hasNewIncidents && !hasResolutions) return null;

  return {
    prev_indicator: prev.last_indicator,
    next_indicator: nextIndicator,
    new_incidents: newIncidents,
    resolved_incident_ids: resolved,
    description,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Gemini summary via OpenRouter (OpenAI-compatible chat completions).
// Returns { title, body } — short, Wesley-readable in Slack.
// On error, falls back to a deterministic template so we never *fail* to notify.
// ─────────────────────────────────────────────────────────────────────────────
async function summariseWithGemini(
  env: Env,
  ctx: TransitionContext,
  summary: StatusSummary,
): Promise<{ title: string; body: string }> {
  const fallbackTitle = buildFallbackTitle(ctx);
  const fallbackBody = buildFallbackBody(ctx, summary);

  if (!env.OPENROUTER_API_KEY) return { title: fallbackTitle, body: fallbackBody };

  const prompt = [
    "You are a terse on-call summariser. The Anthropic status page just transitioned.",
    `Previous overall indicator: ${ctx.prev_indicator}`,
    `Current overall indicator:  ${ctx.next_indicator}`,
    ctx.new_incidents.length > 0
      ? `New incidents: ${ctx.new_incidents.map((i) => `${i.name} [${i.impact ?? "?"}/${i.status ?? "?"}]`).join("; ")}`
      : "No new incidents.",
    ctx.resolved_incident_ids.length > 0
      ? `Resolved incident IDs: ${ctx.resolved_incident_ids.join(", ")}`
      : "No resolutions.",
    `Status page description: ${ctx.description || "(none)"}`,
    "",
    "Write JSON: {\"title\": \"...\", \"body\": \"...\"}.",
    "- title: ≤80 chars, prefix with severity emoji (🟢/🟡/🟠/🔴).",
    "- body: 1-3 sentences, plain text, no markdown headers. Include the indicator transition.",
    "- No preamble, no code fences, just raw JSON.",
  ].join("\n");

  try {
    const resp = await fetch(`${env.OPENROUTER_BASE_URL}/chat/completions`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.OPENROUTER_API_KEY}`,
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/DataViking-Tech/anthropic-watch",
        "X-Title": "anthropic-watch",
      },
      body: JSON.stringify({
        model: env.OPENROUTER_MODEL,
        messages: [
          { role: "system", content: "Return only valid JSON. No prose." },
          { role: "user", content: prompt },
        ],
        temperature: 0.2,
        response_format: { type: "json_object" },
      }),
      signal: AbortSignal.timeout(15_000),
    });
    if (!resp.ok) {
      console.log(JSON.stringify({ kind: "openrouter_http_error", status: resp.status }));
      return { title: fallbackTitle, body: fallbackBody };
    }
    const data = (await resp.json()) as { choices?: Array<{ message?: { content?: string } }> };
    const content = data.choices?.[0]?.message?.content ?? "";
    const parsed = JSON.parse(content) as { title?: unknown; body?: unknown };
    const title =
      typeof parsed.title === "string" && parsed.title.trim().length > 0
        ? parsed.title.trim().slice(0, 140)
        : fallbackTitle;
    const body =
      typeof parsed.body === "string" && parsed.body.trim().length > 0
        ? parsed.body.trim().slice(0, 8192)
        : fallbackBody;
    return { title, body };
  } catch (err) {
    console.log(JSON.stringify({ kind: "openrouter_exception", error: String(err) }));
    return { title: fallbackTitle, body: fallbackBody };
  }
}

const INDICATOR_EMOJI: Record<Indicator, string> = {
  none: "🟢",
  minor: "🟡",
  major: "🟠",
  critical: "🔴",
  maintenance: "🔧",
};

function buildFallbackTitle(ctx: TransitionContext): string {
  const emoji = INDICATOR_EMOJI[ctx.next_indicator] ?? "⚠️";
  return `${emoji} Anthropic status: ${ctx.prev_indicator} → ${ctx.next_indicator}`.slice(0, 140);
}

function buildFallbackBody(ctx: TransitionContext, summary: StatusSummary): string {
  const lines: string[] = [];
  lines.push(`Indicator: ${ctx.prev_indicator} → ${ctx.next_indicator}`);
  if (ctx.description) lines.push(`Status: ${ctx.description}`);
  if (ctx.new_incidents.length > 0) {
    lines.push("New incidents:");
    for (const i of ctx.new_incidents) {
      lines.push(`  • ${i.name} (impact=${i.impact ?? "?"}, status=${i.status ?? "?"})`);
    }
  }
  if (ctx.resolved_incident_ids.length > 0) {
    lines.push(`Resolved: ${ctx.resolved_incident_ids.join(", ")}`);
  }
  void summary;
  return lines.join("\n").slice(0, 8192);
}

// ─────────────────────────────────────────────────────────────────────────────
// HMAC signing — matches alert_hub's verifyProducerHmac (src/lib/hmac.ts).
//   X-Gastown-Origin:    "proactive:anthropic-watch"
//   X-Gastown-Timestamp: ISO-8601
//   X-Gastown-Signature: hex sha256 HMAC of `${timestamp}\n${body}`
// ─────────────────────────────────────────────────────────────────────────────
async function hmacSha256Hex(secret: string, message: string): Promise<string> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(message));
  return Array.from(new Uint8Array(sig))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

async function sha256Hex(message: string): Promise<string> {
  const enc = new TextEncoder();
  const digest = await crypto.subtle.digest("SHA-256", enc.encode(message));
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

interface AlertEventBody {
  schema_version: typeof SCHEMA_VERSION;
  idempotency_key: string;
  severity: "info" | "warning" | "critical" | "page";
  producer_type: string;
  producer_id: string;
  producer_version: string;
  target_channel: string;
  title: string;
  body: string;
  context?: { extra?: Record<string, string> };
  retry_safe: boolean;
  ack_required: boolean;
  created_at: string;
}

async function buildAlertBody(
  env: Env,
  ctx: TransitionContext,
  summary: StatusSummary,
): Promise<AlertEventBody> {
  const { title, body } = await summariseWithGemini(env, ctx, summary);
  const severity = severityFor(ctx.next_indicator, ctx.prev_indicator);
  const ack_required = severity === "critical" || severity === "page";
  const created_at = new Date().toISOString();

  // Idempotency key: stable for the same logical transition. If the cron retries
  // before the KV write lands, alert_hub will 409 on the dup and we won't double-page.
  const newIncidentIds = ctx.new_incidents.map((i) => i.id).sort().join(",");
  const resolvedIds = ctx.resolved_incident_ids.slice().sort().join(",");
  const keyMaterial = [
    env.PRODUCER_TYPE,
    env.PRODUCER_ID,
    ctx.prev_indicator,
    ctx.next_indicator,
    newIncidentIds,
    resolvedIds,
  ].join("|");
  const idempotency_key = (await sha256Hex(keyMaterial)).slice(0, 64);

  const extra: Record<string, string> = {
    prev_indicator: ctx.prev_indicator,
    next_indicator: ctx.next_indicator,
    status_page: "https://status.anthropic.com",
  };
  if (ctx.new_incidents.length > 0) {
    extra.new_incident_ids = ctx.new_incidents.map((i) => i.id).join(",");
    const firstShortlink = ctx.new_incidents.find((i) => typeof i.shortlink === "string")?.shortlink;
    if (firstShortlink) extra.first_incident_url = firstShortlink;
  }
  if (ctx.resolved_incident_ids.length > 0) {
    extra.resolved_incident_ids = ctx.resolved_incident_ids.join(",");
  }

  return {
    schema_version: SCHEMA_VERSION,
    idempotency_key,
    severity,
    producer_type: env.PRODUCER_TYPE,
    producer_id: env.PRODUCER_ID,
    producer_version: env.PRODUCER_VERSION,
    target_channel: env.TARGET_CHANNEL,
    title: title.slice(0, 140),
    body: body.slice(0, 8192),
    context: { extra },
    retry_safe: true,
    ack_required,
    created_at,
  };
}

async function postAlert(env: Env, alert: AlertEventBody): Promise<{ ok: boolean; status: number; body: string }> {
  const raw = JSON.stringify(alert);
  const timestamp = new Date().toISOString();
  const origin = `${env.PRODUCER_TYPE}:${env.PRODUCER_ID}`;
  const signature = await hmacSha256Hex(env.HMAC_PROACTIVE_ANTHROPIC_WATCH, `${timestamp}\n${raw}`);

  const resp = await fetch(env.ALERTHUB_INGRESS_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Gastown-Origin": origin,
      "X-Gastown-Timestamp": timestamp,
      "X-Gastown-Signature": signature,
    },
    body: raw,
    signal: AbortSignal.timeout(10_000),
  });
  const respBody = await resp.text();
  return { ok: resp.ok, status: resp.status, body: respBody };
}

async function fetchStatus(env: Env): Promise<StatusSummary> {
  const resp = await fetch(env.STATUS_URL, {
    headers: { "User-Agent": "anthropic-watch/0.1 (+https://github.com/DataViking-Tech/anthropic-watch)" },
    signal: AbortSignal.timeout(10_000),
  });
  if (!resp.ok) throw new Error(`status fetch ${resp.status}`);
  return (await resp.json()) as StatusSummary;
}

async function tick(env: Env, ctx: ExecutionContext): Promise<{ ok: true; action: string }> {
  void ctx;
  if ((env.ANTHROPIC_WATCH_ENABLED ?? "false").toLowerCase() !== "true") {
    console.log(JSON.stringify({ kind: "tick_skipped_kill_switch" }));
    return { ok: true, action: "skipped_kill_switch" };
  }

  const summary = await fetchStatus(env);
  const prev = await loadState(env);
  const transition = diffState(prev, summary);

  if (!transition) {
    // Steady state — just refresh tick timestamp (and seed incident IDs on first run).
    const incidentIds = (summary.incidents ?? []).map((i) => i.id).filter((x): x is string => typeof x === "string");
    const nextIndicator = normaliseIndicator(summary.status?.indicator);
    await saveState(env, {
      last_indicator: nextIndicator,
      last_seen_incident_ids: incidentIds,
      last_tick_at: new Date().toISOString(),
    });
    return { ok: true, action: "no_change" };
  }

  const alert = await buildAlertBody(env, transition, summary);
  const result = await postAlert(env, alert);

  console.log(
    JSON.stringify({
      kind: "transition_notify",
      from: transition.prev_indicator,
      to: transition.next_indicator,
      new_incident_count: transition.new_incidents.length,
      resolved_count: transition.resolved_incident_ids.length,
      ingress_status: result.status,
      ingress_ok: result.ok,
    }),
  );

  // Persist new state regardless of ingress success — if ingress is down, we'd
  // rather miss one notification than spam Wesley on every 2-min retry.
  // (Ingress idempotency_key would dedup anyway, but conservative either way.)
  const incidentIds = (summary.incidents ?? []).map((i) => i.id).filter((x): x is string => typeof x === "string");
  await saveState(env, {
    last_indicator: transition.next_indicator,
    last_seen_incident_ids: incidentIds,
    last_tick_at: new Date().toISOString(),
  });

  return { ok: true, action: "notified" };
}

export default {
  // Cron Trigger entry point.
  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    try {
      const result = await tick(env, ctx);
      console.log(JSON.stringify({ kind: "tick_done", ...result }));
    } catch (err) {
      console.log(JSON.stringify({ kind: "tick_error", error: String(err) }));
      throw err;
    }
  },

  // HTTP entry point — health check, manual smoke-trigger.
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return new Response(
        JSON.stringify({ ok: true, enabled: env.ANTHROPIC_WATCH_ENABLED === "true" }),
        { headers: { "Content-Type": "application/json" } },
      );
    }

    // Manual debug trigger: GET /tick (no auth — Worker only reads state, posts at most one alert).
    // Useful for smoke tests after seeding a fake state via `wrangler kv key put`.
    if (request.method === "GET" && url.pathname === "/tick") {
      try {
        const result = await tick(env, ctx);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        return new Response(JSON.stringify({ ok: false, error: String(err) }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    return new Response("not found", { status: 404 });
  },
};
