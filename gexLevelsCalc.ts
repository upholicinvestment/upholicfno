// -----------------------------------------------------------------------------------------------------------

// src/routes/gexLevelsCalc.ts
import { Express, Request, Response } from "express";
import { Db } from "mongodb";

// If you're not on Node 18+, uncomment the next line:
// import fetch from "node-fetch";

const API_BASE = "https://api.upholictech.com/api";
// const API_BASE = "http://localhost:8000/api";

/* ================== Types ================== */
type GexRow = {
  strike: number;
  gex_oi_raw: number;
  gex_vol_raw: number;
  ce_oi: number;
  pe_oi: number;
  ce_vol: number;
  pe_vol: number;
};
type ApiRow = Partial<GexRow>;

type GexCacheResp = {
  symbol: string;
  expiry: string;
  spot: number;
  rows: ApiRow[];
  updated_at?: string;
};

type TickPoint = { x: number; y: number };
type TicksApiResp = {
  symbol: string;
  expiry: string;
  trading_day_ist: string; // "YYYY-MM-DD"
  from?: string;
  to?: string;
  points: TickPoint[];
  count?: number;
};

type LevelName = "R1" | "R2" | "S1" | "S2" | "Flip";
type Side = "ce" | "pe";

const OI_WEIGHT = 2;
const VOL_WEIGHT = 1;

const finite = (n: any): n is number =>
  typeof n === "number" && Number.isFinite(n);

/* ================== Rank + selection (same as frontend) ================== */
function rankMap(
  rows: GexRow[],
  key: "gex_oi_raw" | "gex_vol_raw",
  desc: boolean
): Map<number, number> {
  const sorted = [...rows].sort((a, b) => (desc ? b[key] - a[key] : a[key] - b[key]));
  const map = new Map<number, number>();
  for (let i = 0; i < sorted.length; i++) map.set(sorted[i].strike, i + 1);
  return map;
}
function rankMapAbs(rows: GexRow[], key: "gex_oi_raw"): Map<number, number> {
  const sorted = [...rows].sort((a, b) => Math.abs(a[key]) - Math.abs(b[key]));
  const map = new Map<number, number>();
  for (let i = 0; i < sorted.length; i++) map.set(sorted[i].strike, i + 1);
  return map;
}

type LineMark = { strike: number; label: "R1" | "R2" | "S1" | "S2"; color: string; side: Side };

function pickLinesByRaw(rows: GexRow[]): { red: LineMark[]; green: LineMark[] } {
  const valid = rows.filter(
    (r) =>
      finite(r.strike) &&
      finite(r.gex_oi_raw) &&
      finite(r.gex_vol_raw) &&
      !(r.gex_oi_raw === 0 && r.gex_vol_raw === 0)
  );
  const pos = valid.filter((r) => r.gex_oi_raw > 0 && r.gex_vol_raw > 0);
  const neg = valid.filter((r) => r.gex_oi_raw < 0 && r.gex_vol_raw < 0);

  const red: LineMark[] = [];
  if (pos.length) {
    const oiRank = rankMap(pos, "gex_oi_raw", true);
    const volRank = rankMap(pos, "gex_vol_raw", true);
    const scored = pos.map((r) => {
      const oiR = oiRank.get(r.strike) ?? 9999;
      const volR = volRank.get(r.strike) ?? 9999;
      return { strike: r.strike, oiR, volR, score: OI_WEIGHT * oiR + VOL_WEIGHT * volR };
    });
    scored.sort((a, b) => a.score - b.score || a.oiR - b.oiR || a.volR - b.volR);
    if (scored[0]) red.push({ strike: scored[0].strike, label: "R1", color: "#ef4444", side: "ce" });
    if (scored[1]) red.push({ strike: scored[1].strike, label: "R2", color: "#fca5a5", side: "ce" });
  }

  const green: LineMark[] = [];
  if (neg.length) {
    const oiRank = rankMap(neg, "gex_oi_raw", false);
    const volRank = rankMap(neg, "gex_vol_raw", false);
    const scored = neg.map((r) => {
      const oiR = oiRank.get(r.strike) ?? 9999;
      const volR = volRank.get(r.strike) ?? 9999;
      return { strike: r.strike, oiR, volR, score: OI_WEIGHT * oiR + VOL_WEIGHT * volR };
    });
    scored.sort((a, b) => a.score - b.score || a.oiR - b.oiR || a.volR - b.volR);
    if (scored[0]) green.push({ strike: scored[0].strike, label: "S1", color: "#10b981", side: "pe" });
    if (scored[1]) green.push({ strike: scored[1].strike, label: "S2", color: "#86efac", side: "pe" });
  }
  return { red, green };
}

function pickZeroGammaStrike(rows: GexRow[], r1?: number, s1?: number): number | null {
  if (!finite(r1) || !finite(s1)) return null;
  const lo = Math.min(r1!, s1!);
  const hi = Math.max(r1!, s1!);
  const between = rows.filter(
    (r) =>
      finite(r.strike) &&
      r.strike >= lo &&
      r.strike <= hi &&
      finite(r.gex_oi_raw) &&
      finite(r.gex_vol_raw)
  );
  if (!between.length) return null;

  const absOiRank = rankMapAbs(between, "gex_oi_raw");
  const volRank = rankMap(between, "gex_vol_raw", true);
  const scored = between.map((r) => ({
    strike: r.strike,
    score: (absOiRank.get(r.strike) || 9999) + (volRank.get(r.strike) || 9999),
  }));
  scored.sort((a, b) => a.score - b.score);
  return scored[0]?.strike ?? null;
}

/* ================== Date helpers (IST + minute bucket) ================== */
const IST_TZ = "Asia/Kolkata";
const ONE_MIN_MS = 60_000;

// ✅ correct
function formatIst(now: Date = new Date()): string {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: IST_TZ,
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(now);
}

function minuteBucket(now = Date.now()): number {
  return Math.floor(now / 60000);
}

/** Extract IST parts we care about (weekday, hour, minute, second). */
function getIstParts(d = new Date()): { weekday: string; hour: number; minute: number; second: number } {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: IST_TZ,
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(d);

  const map: Record<string, string> = {};
  for (const p of parts) {
    if (p.type !== "literal") map[p.type] = p.value;
  }
  return {
    weekday: map.weekday, // "Mon"..."Sun"
    hour: Number(map.hour || 0),
    minute: Number(map.minute || 0),
    second: Number(map.second || 0),
  };
}

/** Market window in minutes since midnight IST: 09:15 to 15:30 inclusive. */
const MARKET_START_MIN = 9 * 60 + 15;  // 555
const MARKET_END_MIN   = 15 * 60 + 30; // 930

function isMarketOpenIst(d = new Date()): boolean {
  const { weekday, hour, minute } = getIstParts(d);
  const mins = hour * 60 + minute;
  const isWeekday = weekday !== "Sat" && weekday !== "Sun";
  return isWeekday && mins >= MARKET_START_MIN && mins <= MARKET_END_MIN;
}

function msUntilNextMinute(d = new Date()): number {
  return ONE_MIN_MS - (d.getTime() % ONE_MIN_MS);
}

/* ================== Core compute+save ================== */
async function computeAndSaveSnapshot(db: Db, symbol = "NIFTY", explicitExpiry?: string) {
  const coll = db.collection("gex_levels");

  // Resolve expiry (prefer explicit, else auto-detect from cache)
  let expiry = explicitExpiry || "";
  if (!expiry) {
    const r = await fetch(`${API_BASE}/gex/nifty/cache`);
    if (!r.ok) throw new Error(`Upstream cache HTTP ${r.status}`);
    const j = (await r.json()) as GexCacheResp;
    expiry = j?.expiry;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(expiry)) {
    throw new Error("No valid expiry resolved.");
  }

  // Fetch upstream
  const [cacheResp, ticksResp] = await Promise.all([
    fetch(`${API_BASE}/gex/nifty/cache?expiry=${encodeURIComponent(expiry)}`),
    fetch(`${API_BASE}/gex/nifty/ticks?expiry=${encodeURIComponent(expiry)}`),
  ]);
  if (!cacheResp.ok) throw new Error(`Upstream cache HTTP ${cacheResp.status}`);
  if (!ticksResp.ok) throw new Error(`Upstream ticks HTTP ${ticksResp.status}`);

  const cacheJson = (await cacheResp.json()) as GexCacheResp;
  const ticksJson = (await ticksResp.json()) as TicksApiResp;

  // Normalize rows
  const rows: GexRow[] = Array.isArray(cacheJson.rows)
    ? cacheJson.rows
        .map((r: ApiRow): GexRow => ({
          strike: Number(r?.strike ?? 0),
          gex_oi_raw: Number(r?.gex_oi_raw ?? 0),
          gex_vol_raw: Number(r?.gex_vol_raw ?? 0),
          ce_oi: Number(r?.ce_oi ?? 0),
          pe_oi: Number(r?.pe_oi ?? 0),
          ce_vol: Number(r?.ce_vol ?? 0),
          pe_vol: Number(r?.pe_vol ?? 0),
        }))
        .filter((r) => Number.isFinite(r.strike))
    : [];

  const symbolUp = cacheJson.symbol || symbol;
  const spot = Number(cacheJson.spot ?? 0);
  const trading_day_ist = ticksJson.trading_day_ist || undefined;

  // Calculate levels
  const { red, green } = pickLinesByRaw(rows);
  const r1 = red.find((l) => l.label === "R1")?.strike ?? null;
  const r2 = red.find((l) => l.label === "R2")?.strike ?? null;
  const s1 = green.find((l) => l.label === "S1")?.strike ?? null;
  const s2 = green.find((l) => l.label === "S2")?.strike ?? null;
  const flip = pickZeroGammaStrike(rows, r1 ?? undefined, s1 ?? undefined);

  // row lookup for values
  const getRow = (strike?: number | null): GexRow | undefined => {
    if (!rows || !finite(strike as any)) return;
    const exact = rows.find((r) => r.strike === strike);
    if (exact) return exact;
    let best: GexRow | undefined;
    let dmin = Infinity;
    for (const r of rows) {
      const d = Math.abs(r.strike - (strike as number));
      if (d < dmin) {
        dmin = d;
        best = r;
      }
    }
    return dmin <= 0.5 ? best : undefined;
  };

  const levels: Array<{
    name: LevelName;
    strike: number;
    gex_oi_raw: number | null;
    gex_vol_raw: number | null;
    side?: Side;
  }> = [];

  const pushLevel = (name: LevelName, strike: number | null, side?: Side) => {
    if (!finite(strike as any)) return;
    const row = getRow(strike!);
    levels.push({
      name,
      strike: strike!,
      gex_oi_raw: row?.gex_oi_raw ?? null,
      gex_vol_raw: row?.gex_vol_raw ?? null,
      side,
    });
  };

  pushLevel("R1", r1, "ce");
  pushLevel("R2", r2, "ce");
  pushLevel("S1", s1, "pe");
  pushLevel("S2", s2, "pe");
  pushLevel("Flip", flip ?? null);

  // Build doc
  const now = new Date();
  const minute_bucket = minuteBucket(now.getTime());
  const at_utc = now;
  const at_ist = formatIst(now);

  const client_key =
    `${symbolUp}|${expiry}|${trading_day_ist ?? "NA"}|` +
    `${levels.map((l) => `${l.name}:${l.strike}`).join("|")}`;

  const doc = {
    symbol: symbolUp,
    expiry,
    spot,
    trading_day_ist,
    levels,
    at_utc,
    at_ist,
    minute_bucket,
    client_key,
  };

  // Save once per minute per trading day
  let saved = false;
  try {
    await coll.insertOne(doc as any);
    saved = true;
  } catch (e: any) {
    if (e?.code !== 11000) throw e; // ignore duplicate
  }

  return { saved, doc };
}

/* ================== Route + 1-min runner ================== */
export function GexLevelsCalc(app: Express, db: Db) {
  const coll = db.collection("gex_levels");

  // Unique per minute per trading day
  coll
    .createIndex({ symbol: 1, expiry: 1, trading_day_ist: 1, minute_bucket: 1 }, { unique: true })
    .catch(() => {});

  // On-demand compute & save
  app.get("/api/gex/levels/calc", async (req: Request, res: Response): Promise<void> => {
    try {
      const symbol = (req.query.symbol as string) || "NIFTY";
      const expiry = (req.query.expiry as string) || undefined;
      const result = await computeAndSaveSnapshot(db, symbol, expiry);
      res.json({ ok: true, ...result });
    } catch (err: any) {
      res.status(500).json({ ok: false, error: err?.message || String(err) });
    }
  });
}

/**
 * Call this once at server startup. It runs on the minute boundary,
 * but only during IST market hours (Mon–Fri, 09:15–15:30).
 *
 * Example:
 *   import { GexLevelsCalc, startGexLevelsEveryMinute } from "./routes/gexLevelsCalc";
 *   GexLevelsCalc(app, db);
 *   startGexLevelsEveryMinute(db);
 */
export function startGexLevelsEveryMinute(db: Db) {
  let running = false;

  const tick = async () => {
    if (running) return;
    running = true;
    try {
      if (isMarketOpenIst()) {
        await computeAndSaveSnapshot(db, "NIFTY"); // expiry auto-detected
      } else {
        // outside market hours → skip
      }
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error("[gex-levels-1m] error:", e);
    } finally {
      running = false;
    }
  };

  // Align to the next minute boundary, then run every minute.
  const initialDelay = msUntilNextMinute();
  setTimeout(() => {
    tick(); // first run on the next minute
    setInterval(tick, ONE_MIN_MS);
  }, initialDelay);

  // eslint-disable-next-line no-console
  console.log("[gex-levels-1m] scheduled: runs every minute during 09:15–15:30 IST, Mon–Fri");
}
