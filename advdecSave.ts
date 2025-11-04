// ---------------------------------------------------------------------------------------------------------------

// src/routes/advdecSave.ts
import type { Express, Request, Response } from "express";
import type { Db } from "mongodb";
// If you're on Node < 18, install node-fetch and uncomment:
// import fetch from "node-fetch";

/* ================= Types (mirror /api/advdec payload) ================= */
type AdvDecPoint = { time: string; advances: number; declines: number };
type AdvDecResponse = {
  current: { advances: number; declines: number; total: number };
  chartData: AdvDecPoint[];
};

type SaveOpts = {
  bin?: number;          // default 5
  sinceMin?: number;     // default 1440
  expiry?: string;       // optional
  symbol?: string;       // default "NIFTY"
  baseUrl?: string;      // base to call /api/advdec (route uses request host; cron uses localhost)
};

type StartOpts = Omit<SaveOpts, "baseUrl">;

/* ================= Time helpers (IST + minute bucket) ================= */
const IST_TZ = "Asia/Kolkata";
const ONE_MIN_MS = 60_000;
const LOCAL_BASE = "https://api.upholictech.com"; // used by the minute job
// const LOCAL_BASE = "http://localhost:8000"; // used by the minute job

function formatIst(now = new Date()): string {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: IST_TZ,
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(now); // e.g. "16/10/2025, 13:10:05"
}

function tradingDayIst(now = new Date()): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: IST_TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now); // "YYYY-MM-DD"
}

function minuteBucket(ts = Date.now()): number {
  return Math.floor(ts / ONE_MIN_MS);
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

/* ================= Mongo setup ================= */
const COLL_NAME = "advdec_snapshots";
let indexesEnsured = false;

async function ensureIndexes(db: Db) {
  if (indexesEnsured) return;
  const coll = db.collection(COLL_NAME);
  await coll.createIndex(
    { symbol: 1, trading_day_ist: 1, bin: 1, minute_bucket: 1 },
    { unique: true }
  );
  indexesEnsured = true;
}

/* ================= Core save logic (shared) ================= */
async function saveAdvDecSnapshot(db: Db, opts: SaveOpts) {
  await ensureIndexes(db);
  const bin = Math.max(1, Number(opts.bin ?? 5));
  const sinceMin = Math.max(1, Number(opts.sinceMin ?? 1440));
  const expiry = opts.expiry?.trim() || undefined;
  const symbol = (opts.symbol || "NIFTY").toUpperCase();

  const base = (opts.baseUrl && opts.baseUrl.trim()) || LOCAL_BASE;
  const url = new URL("/api/advdec", base);
  url.searchParams.set("bin", String(bin));
  url.searchParams.set("sinceMin", String(sinceMin));
  if (expiry) url.searchParams.set("expiry", expiry);

  const resp = await fetch(url.toString(), {
    headers: { "Cache-Control": "no-cache" },
  });
  if (!resp.ok) throw new Error(`Downstream /api/advdec HTTP ${resp.status}`);
  const data = (await resp.json()) as AdvDecResponse;

  const coll = db.collection(COLL_NAME);
  const now = new Date();
  const doc = {
    symbol,
    bin,
    sinceMin,
    expiry: expiry ?? null,
    current: data.current,
    chartData: data.chartData,
    at_utc: now, // Date
    at_ist: formatIst(now), // string
    trading_day_ist: tradingDayIst(now), // "YYYY-MM-DD"
    minute_bucket: minuteBucket(now.getTime()),
    client_key: `ADVDEC|${symbol}|${expiry ?? "NA"}|${tradingDayIst(now)}|${bin}|${minuteBucket(
      now.getTime()
    )}`,
  };

  try {
    const r = await coll.insertOne(doc as any);
    return { saved: true, _id: r.insertedId, meta: { symbol, bin, mb: doc.minute_bucket } };
  } catch (e: any) {
    // ignore duplicate within same minute/bin/day
    if (e?.code === 11000) return { saved: false, duplicate: true };
    throw e;
  }
}

/* ================= Route: one API to save current snapshot ================= */
export function AdvDecSave(app: Express, db: Db) {
  app.get(
    "/api/advdec/save",
    async (req: Request, res: Response): Promise<void> => {
      try {
        const bin = Math.max(1, Number(req.query.bin) || 5);
        const sinceMin = Math.max(1, Number(req.query.sinceMin) || 1440);
        const expiry =
          typeof req.query.expiry === "string" && req.query.expiry.trim()
            ? req.query.expiry.trim()
            : undefined;
        const symbol =
          typeof req.query.symbol === "string" && req.query.symbol.trim()
            ? req.query.symbol.trim().toUpperCase()
            : "NIFTY";

        const base = `${req.protocol}://${req.get("host") || "localhost:8000"}`;

        const result = await saveAdvDecSnapshot(db, {
          bin,
          sinceMin,
          expiry,
          symbol,
          baseUrl: base,
        });

        res.json({ ok: true, ...result });
      } catch (err: any) {
        console.error("Error in /api/advdec/save:", err);
        res.status(500).json({ ok: false, error: err?.message || "Internal Error" });
      }
    }
  );
}

/* ================= 1-minute job (runs only during IST market hours) ================= */
export function startAdvDecMinuteJob(db: Db, opts?: StartOpts) {
  // tick function gated to market hours
  const tick = () => {
    if (isMarketOpenIst()) {
      void saveAdvDecSnapshot(db, { ...opts, baseUrl: LOCAL_BASE }).catch((err) => {
        console.error("advdec minute job error:", err?.message || err);
      });
    } else {
      // outside market hours → skip
    }
  };

  // Align to the next minute boundary, then run every minute.
  const initialDelay = msUntilNextMinute();
  setTimeout(() => {
    tick(); // first run on the next minute
    setInterval(tick, ONE_MIN_MS);
  }, initialDelay);
}
