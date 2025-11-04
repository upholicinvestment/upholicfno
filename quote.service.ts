// src/services/quote.service.ts
import axios from "axios";
import { Db } from "mongodb";
import { Readable } from "stream";
import csvParser from "csv-parser";
import { scheduleQuote } from "./dhanPacer"; // quote bucket

let db: Db | null = null;

/* ====================================================================== */
/* ============================= DB wiring ============================== */
/* ====================================================================== */

export const setQuoteDatabase = (database: Db) => {
  db = database;

  // Helpful indexes (idempotent)
  db.collection("market_quotes").createIndex({ timestamp: -1 }).catch(() => {});
  db.collection("market_quotes").createIndex({ security_id: 1, timestamp: -1 }).catch(() => {});

  // Refresh target: keep ONE doc per {security_id, expiry_date}
  db
    .collection("nse_futstk_ohlc")
    .createIndex({ security_id: 1, expiry_date: 1 }, { unique: true })
    .catch(() => {});
  db.collection("nse_futstk_ohlc").createIndex({ received_at: -1 }).catch(() => {});

  // Tick store: append-only time series
  db
    .collection("nse_futstk_ticks")
    .createIndex({ security_id: 1, expiry_date: 1, received_at: -1 })
    .catch(() => {});
  db.collection("nse_futstk_ticks").createIndex({ received_at: -1 }).catch(() => {});
};

/* ====================================================================== */
/* ========================= Time & retries ============================= */
/* ====================================================================== */

// Convert UTC to IST (we store raw Date from server clock)
function getISTDate(): Date {
  return new Date();
}

function isMarketOpen(): boolean {
  const now = getISTDate();
  const minutes = now.getHours() * 60 + now.getMinutes();
  const day = now.getDay(); // 0=Sun, 6=Sat
  if (day === 0 || day === 6) return false;
  return minutes >= 9 * 60 + 15 && minutes <= 15 * 60 + 30;
}

// Backoff-aware retry that ALWAYS goes through the pacing bucket
async function quoteRetry<T>(fn: () => Promise<T>, tries = 3): Promise<T> {
  let lastErr: any;
  for (let i = 0; i < tries; i++) {
    try {
      return await scheduleQuote(fn);
    } catch (err: any) {
      lastErr = err;
      const status = err?.response?.status;
      if (status === 429 || (status >= 500 && status < 600)) {
        const wait = Math.min(8000, 1000 * (i + 1));
        console.log(`⏳ Rate limit (${status}): retrying in ${wait}ms...`);
        await new Promise((r) => setTimeout(r, wait));
        continue;
      }
      throw err;
    }
  }
  throw lastErr;
}

/* ====================================================================== */
/* ================== CSV → instruments (both variants) ================= */
/* ====================================================================== */

/** Parse '28-10-2025 14:30:00' | '28-10-2025' | '2025-10-28' → '2025-10-28' */
function normalizeExpiryInput(expiry: string): string {
  if (!expiry) return "";
  const part = expiry.trim().split(/\s+/)[0];
  if (/^\d{4}-\d{2}-\d{2}$/.test(part)) return part;
  const m = part.match(/^(\d{2})-(\d{2})-(\d{4})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]}`;
  const d = new Date(expiry);
  if (!isNaN(d.getTime())) {
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }
  return expiry;
}

/** Tolerant CSV row parser for BOTH scrip-master CSVs. */
function parseInstrumentRow(row: any) {
  const security_id = parseInt(
    row["SECURITY_ID"] ?? row["SEM_SMST_SECURITY_ID"] ?? row["SMST_SECURITY_ID"] ?? "0",
    10
  );

  const trading_symbol =
    row["SYMBOL_NAME"] ??
    row["SEM_TRADING_SYMBOL"] ??
    row["TRADING_SYMBOL"] ??
    row["SEM_CUSTOM_SYMBOL"] ??
    "";

  const instrument_type =
    row["INSTRUMENT"] ??
    row["SEM_INSTRUMENT_NAME"] ??
    row["SEM_EXCH_INSTRUMENT_TYPE"] ??
    "";

  const expiry_raw =
    row["SM_EXPIRY_DATE"] ?? row["SEM_EXPIRY_DATE"] ?? row["EXPIRY_DATE"] ?? null;

  const strike_price = parseFloat(
    row["STRIKE_PRICE"] ?? row["SEM_STRIKE_PRICE"] ?? row["STRIKE"] ?? "0"
  );

  const option_type = row["OPTION_TYPE"] ?? row["SEM_OPTION_TYPE"] ?? row["OPT_TYPE"] ?? "";
  const expiry_flag = row["EXPIRY_FLAG"] ?? row["SEM_EXPIRY_FLAG"] ?? "";

  const expiry_date = expiry_raw ? normalizeExpiryInput(String(expiry_raw)) : null;

  return {
    security_id,
    trading_symbol,
    instrument_type,
    expiry_date,
    expiry_raw: expiry_raw ?? null,
    strike_price,
    option_type,
    expiry_flag,
  };
}

export const fetchAndStoreInstruments = async () => {
  try {
    const url =
      process.env.DHAN_SCRIP_CSV_URL?.trim() ||
      "https://images.dhan.co/api-data/api-scrip-master-detailed.csv";

    console.log("📡 Fetching instrument master from:", url);
    const response = await axios.get(url, { responseType: "stream", timeout: 30000 });

    const dataStream = response.data as Readable;
    const instruments: any[] = [];

    await new Promise<void>((resolve, reject) => {
      dataStream
        .pipe(csvParser())
        .on("data", (row: any) => {
          const parsed = parseInstrumentRow(row);
          if (parsed.security_id > 0) instruments.push(parsed);
        })
        .on("end", resolve)
        .on("error", reject);
    });

    if (!db) throw new Error("Database not initialized");
    await db.collection("instruments").deleteMany({});
    if (instruments.length > 0) {
      await db.collection("instruments").insertMany(instruments, { ordered: false });
      await db.collection("instruments").createIndex({ security_id: 1 }, { unique: true }).catch(() => {});
      await db.collection("instruments").createIndex({ instrument_type: 1, expiry_date: 1 }).catch(() => {});
      await db.collection("instruments").createIndex({ instrument_type: 1, expiry_raw: 1 }).catch(() => {});
    }

    console.log(`💾 Saved ${instruments.length} instruments to DB.`);
  } catch (err) {
    console.error("❌ Error fetching/storing instruments:", err);
  }
};

/* ====================================================================== */
/* ========================= Marketfeed helpers ========================= */
/* ====================================================================== */

async function fetchOhlcAny(ids: number[]): Promise<Record<string, any>> {
  if (!ids?.length) return {};
  const segments = ["NSE_FNO", "NSE_FUT"];
  for (const seg of segments) {
    try {
      const payload: any = { [seg]: ids };
      const response: any = await quoteRetry(
        () =>
          axios.post("https://api.dhan.co/v2/marketfeed/ohlc", payload, {
            headers: {
              "Content-Type": "application/json",
              "access-token": process.env.DHAN_API_KEY || "",
              "client-id": process.env.DHAN_CLIENT_ID || "",
            },
            timeout: 15000,
          }),
        3
      );
      const data = response?.data?.data?.[seg] || {};
      const count = Object.keys(data).length;
      console.log(`📡 OHLC ${seg} -> ${count} items`);
      if (count > 0) return data;
      console.warn(`ℹ️ OHLC empty for ${seg}, trying next…`);
    } catch (err: any) {
      console.warn(`❗ OHLC error on ${seg}:`, err?.response?.status || err?.message || err);
    }
  }
  return {};
}

async function fetchQuoteAny(ids: number[]): Promise<Record<string, any>> {
  if (!ids?.length) return {};
  const segments = ["NSE_FNO", "NSE_FUT"];
  for (const seg of segments) {
    try {
      const payload: any = { [seg]: ids };
      const response: any = await quoteRetry(
        () =>
          axios.post("https://api.dhan.co/v2/marketfeed/quote", payload, {
            headers: {
              "Content-Type": "application/json",
              "access-token": process.env.DHAN_API_KEY || "",
              "client-id": process.env.DHAN_CLIENT_ID || "",
            },
            timeout: 15000,
          }),
        3
      );
      const data = response?.data?.data?.[seg] || {};
      const count = Object.keys(data).length;
      console.log(`📡 QUOTE ${seg} -> ${count} items`);
      if (count > 0) return data;
      console.warn(`ℹ️ QUOTE empty for ${seg}, trying next…`);
    } catch (err: any) {
      console.warn(`❗ QUOTE error on ${seg}:`, err?.response?.status || err?.message || err);
    }
  }
  return {};
}

/** Public wrappers */
export const fetchOhlc = async (ids: number[]): Promise<Record<string, any>> => fetchOhlcAny(ids);
export const fetchMarketQuote = async (ids: number[]): Promise<Record<string, any>> =>
  fetchQuoteAny(ids);

/* ====================================================================== */
/* ============== Persist market quotes (separate collection) =========== */
/* ====================================================================== */

export const saveMarketQuote = async (data: Record<string, any>) => {
  try {
    if (!db) throw new Error("Database not initialized");

    const timestamp = new Date();
    const securityIds = Object.keys(data).map((id) => parseInt(id, 10));

    const instruments = await db
      .collection("instruments")
      .find({ security_id: { $in: securityIds } })
      .toArray();

    const instrumentMap = Object.fromEntries(instruments.map((inst) => [inst.security_id, inst]));

    const documents = Object.entries(data).map(([security_id, details]: [string, any]) => {
      const sid = parseInt(security_id, 10);
      const instrument = instrumentMap[sid] || {};

      return {
        security_id: sid,
        trading_symbol: instrument.trading_symbol || "",
        instrument_type: instrument.instrument_type || "",
        expiry_date: instrument.expiry_date || null,
        strike_price: instrument.strike_price || 0,
        option_type: instrument.option_type || "",
        expiry_flag: instrument.expiry_flag || "",
        average_price: details?.average_price ?? 0,
        buy_quantity: details?.buy_quantity ?? 0,
        depth: details?.depth ?? { buy: [], sell: [] },
        exchange: "NSE_FNO",
        last_price: details?.last_price ?? 0,
        last_quantity: details?.last_quantity ?? 0,
        last_trade_time: details?.last_trade_time ?? "",
        lower_circuit_limit: details?.lower_circuit_limit ?? 0,
        net_change: details?.net_change ?? 0,
        ohlc: details?.ohlc ?? { open: 0, close: 0, high: 0, low: 0 },
        oi: details?.oi ?? 0,
        oi_day_high: details?.oi_day_high ?? 0,
        oi_day_low: details?.oi_day_low ?? 0,
        sell_quantity: details?.sell_quantity ?? 0,
        upper_circuit_limit: details?.upper_circuit_limit ?? 0,
        volume: details?.volume ?? 0,
        timestamp,
      };
    });

    if (documents.length > 0) {
      const result = await db.collection("market_quotes").insertMany(documents);
      const count =
        (result as any).insertedCount ?? Object.keys((result as any).insertedIds || {}).length;
      console.log(`💾 Saved ${count} Market Quote docs at ${timestamp.toLocaleString("en-IN")}.`);

      const ltpDocs = documents.map((d) => ({
        security_id: d.security_id,
        LTP: d.last_price ?? 0,
        trading_symbol: d.trading_symbol,
        instrument_type: d.instrument_type,
        expiry_date: d.expiry_date,
        strike_price: d.strike_price ?? 0,
        option_type: d.option_type,
        expiry_flag: d.expiry_flag,
        timestamp: new Date(),
      }));
      if (ltpDocs.length) {
        await db.collection("ltp_history").insertMany(ltpDocs, { ordered: false });
      }
    }
  } catch (err) {
    console.error("❌ Error saving Market Quote:", err);
  }
};

/* ====================================================================== */
/* ========== FUTSTK OHLC by Expiry (helpers & INSERT version) ========= */
/* ====================================================================== */

async function getFutstkIdsForExpiry(
  expiryInput: string
): Promise<{ ids: number[]; meta: Record<number, any>; resolvedExpiry: string }> {
  if (!db) throw new Error("Database not initialized");

  const iso = normalizeExpiryInput(expiryInput);
  const [y, m, d] = iso.split("-");
  const dmy = `${d}-${m}-${y}`;

  let docs = await db
    .collection("instruments")
    .find(
      {
        instrument_type: "FUTSTK",
        $or: [
          { expiry_date: iso },
          { expiry_raw: { $regex: `^${dmy}(?:\\b|\\s)` } },
          { expiry_raw: { $regex: `^${iso}(?:\\b|\\s)` } },
        ],
      },
      { projection: { security_id: 1, trading_symbol: 1, expiry_date: 1, expiry_raw: 1 } }
    )
    .toArray();

  let resolvedIso = iso;

  if (docs.length === 0) {
    const all = await db
      .collection("instruments")
      .find(
        { instrument_type: "FUTSTK" },
        { projection: { expiry_date: 1, expiry_raw: 1 } }
      )
      .toArray();

    const candidates: string[] = [];
    for (const r of all) {
      const cand =
        (r.expiry_date && typeof r.expiry_date === "string" && r.expiry_date) ||
        (r.expiry_raw && normalizeExpiryInput(String(r.expiry_raw))) ||
        null;
      if (cand) candidates.push(cand);
    }

    const target = new Date(iso + "T00:00:00Z").getTime();
    let best: { iso: string; diff: number } | null = null;
    for (const c of candidates) {
      const t = new Date(c + "T00:00:00Z").getTime();
      const diff = Math.abs(t - target);
      if (!best || diff < best.diff) best = { iso: c, diff };
    }
    if (best) {
      resolvedIso = best.iso;
      const [yy, mm, dd2] = resolvedIso.split("-");
      const altDmy = `${dd2}-${mm}-${yy}`;
      docs = await db
        .collection("instruments")
        .find(
          {
            instrument_type: "FUTSTK",
            $or: [
              { expiry_date: resolvedIso },
              { expiry_raw: { $regex: `^${altDmy}(?:\\b|\\s)` } },
              { expiry_raw: { $regex: `^${resolvedIso}(?:\\b|\\s)` } },
            ],
          },
          { projection: { security_id: 1, trading_symbol: 1, expiry_date: 1, expiry_raw: 1 } }
        )
        .toArray();
      console.log(`ℹ️ Resolved FUTSTK expiry '${expiryInput}' → '${resolvedIso}'`);
    }
  }

  const ids = docs.map((d) => Number(d.security_id)).filter(Number.isFinite);
  const meta: Record<number, any> = {};
  for (const d of docs) meta[Number(d.security_id)] = d;

  return { ids, meta, resolvedExpiry: resolvedIso };
}

/** build a tick doc from details */
function buildTickDoc(
  sid: number,
  meta: any,
  expiry_date: string,
  details: any,
  now: Date
) {
  const o = details?.ohlc || {};
  const LTP = Number(details?.last_price ?? 0);
  const open = Number(o?.open ?? 0);
  const high = Number(o?.high ?? 0);
  const low = Number(o?.low ?? 0);
  const close = Number(o?.close ?? (Number.isFinite(LTP) ? LTP : 0));

  return {
    // identity
    security_id: sid,
    trading_symbol: meta?.trading_symbol || "",
    instrument_type: "FUTSTK",
    exchange: "NSE_FNO",
    expiry_date,

    // prices
    LTP,
    open,
    high,
    low,
    close,

    // metadata
    source: "refresh",       // "insert" or "refresh"
    type: "Full Data",       // useful if your analytics expect this
    received_at: now,
  };
}

/**
 * INSERT version (one-shot): writes new docs every call and logs ticks.
 */
export const fetchAndSaveFutstkOhlc = async (
  expiryInput: string,
  forceZero = false
): Promise<{ matched: number; fetched: number; inserted: number; tickInserted: number; resolvedExpiry?: string }> => {
  if (!db) throw new Error("Database not initialized");

  const { ids, meta, resolvedExpiry } = await getFutstkIdsForExpiry(expiryInput);
  if (!ids.length) {
    console.warn("⚠️ No FUTSTK instruments found for expiry:", expiryInput);
    return { matched: 0, fetched: 0, inserted: 0, tickInserted: 0, resolvedExpiry };
  }

  try { await db.collection("nse_futstk_ohlc").createIndex({ security_id: 1, received_at: -1 }); } catch {}

  const chunkSize = 1000;
  let fetched = 0;
  let inserted = 0;
  let tickInserted = 0;

  const allowZero =
    forceZero || (process.env.FUTSTK_INSERT_ZERO || "true").toLowerCase() !== "false";

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);

    let data = await fetchOhlcAny(chunk);
    if (!data || Object.keys(data).length === 0) {
      console.warn("ℹ️ OHLC returned empty; falling back to QUOTE for this batch.");
      const q = await fetchQuoteAny(chunk);
      data = {};
      for (const [sid, det] of Object.entries<any>(q)) {
        (data as any)[sid] = {
          last_price: det?.last_price ?? 0,
          ohlc: det?.ohlc ?? { open: 0, high: 0, low: 0, close: Number(det?.last_price ?? 0) },
        };
      }
    }

    const now = new Date();

    const docs = Object.entries<any>(data).map(([sidStr, details]) => {
      fetched++;
      const sid = Number(sidStr);
      const m = meta[sid] || {};
      const o = details?.ohlc || {};
      return {
        security_id: sid,
        trading_symbol: m.trading_symbol || "",
        expiry_date: m.expiry_date || resolvedExpiry || normalizeExpiryInput(expiryInput),
        instrument_type: "FUTSTK",
        exchange: "NSE_FNO",
        LTP: Number(details?.last_price ?? 0),
        open: Number(o?.open ?? 0),
        high: Number(o?.high ?? 0),
        low: Number(o?.low ?? 0),
        close: Number(o?.close ?? Number(details?.last_price ?? 0)),
        received_at: now,
      };
    });

    const finalDocs = allowZero
      ? docs.filter((d) => Number.isFinite(d.security_id))
      : docs.filter((d) => Number.isFinite(d.security_id) && (d.LTP > 0 || d.open > 0 || d.high > 0 || d.low > 0 || d.close > 0));

    if (finalDocs.length) {
      const res = await db!.collection("nse_futstk_ohlc").insertMany(finalDocs, { ordered: false });
      inserted += (res as any).insertedCount ?? Object.keys((res as any).insertedIds || {}).length;

      // also write tick rows
      const tickDocs = finalDocs.map((d) =>
        ({ ...d, source: "insert", type: "Full Data" })
      );
      if (tickDocs.length) {
        const tr = await db!.collection("nse_futstk_ticks").insertMany(tickDocs, { ordered: false });
        tickInserted += (tr as any).insertedCount ?? Object.keys((tr as any).insertedIds || {}).length;
      }
    }
  }

  console.log(
    `💾 FUTSTK OHLC saved | expiry=${resolvedExpiry || normalizeExpiryInput(expiryInput)} matched=${ids.length} fetched=${fetched} inserted=${inserted} ticks=${tickInserted}`
  );
  return { matched: ids.length, fetched, inserted, tickInserted, resolvedExpiry };
};

/* ====================================================================== */
/* ========= FUTSTK OHLC by Expiry (refresh/upsert + ticks) ============= */
/* ====================================================================== */

export const refreshAndUpsertFutstkOhlc = async (
  expiryInput: string,
  forceZero = false
): Promise<{ matched: number; fetched: number; upserted: number; modified: number; tickInserted: number; resolvedExpiry?: string }> => {
  if (!db) throw new Error("Database not initialized");

  const { ids, meta, resolvedExpiry } = await getFutstkIdsForExpiry(expiryInput);
  if (!ids.length) {
    console.warn("⚠️ No FUTSTK instruments found for expiry:", expiryInput);
    return { matched: 0, fetched: 0, upserted: 0, modified: 0, tickInserted: 0, resolvedExpiry };
  }

  try {
    await db.collection("nse_futstk_ohlc").createIndex(
      { security_id: 1, expiry_date: 1 },
      { unique: true }
    );
  } catch {}

  const chunkSize = 1000;
  let fetched = 0;
  let upserted = 0;
  let modified = 0;
  let tickInserted = 0;

  const allowZero =
    forceZero || (process.env.FUTSTK_INSERT_ZERO || "true").toLowerCase() !== "false";

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);

    let data = await fetchOhlcAny(chunk);
    if (!data || Object.keys(data).length === 0) {
      const q = await fetchQuoteAny(chunk);
      data = {};
      for (const [sid, det] of Object.entries<any>(q)) {
        (data as any)[sid] = {
          last_price: det?.last_price ?? 0,
          ohlc: det?.ohlc ?? { open: 0, high: 0, low: 0, close: Number(det?.last_price ?? 0) },
        };
      }
    }

    const now = new Date();
    const ops: any[] = [];
    const tickDocs: any[] = [];

    for (const [sidStr, details] of Object.entries<any>(data)) {
      const sid = Number(sidStr);
      if (!Number.isFinite(sid)) continue;
      fetched++;

      const m = meta[sid] || {};
      const o = details?.ohlc || {};
      const LTP = Number(details?.last_price ?? 0);
      const open = Number(o?.open ?? 0);
      const high = Number(o?.high ?? 0);
      const low = Number(o?.low ?? 0);
      const close = Number(o?.close ?? (Number.isFinite(LTP) ? LTP : 0));

      if (!allowZero && !(LTP > 0 || open > 0 || high > 0 || low > 0 || close > 0)) continue;

      const expiry_date = m.expiry_date || resolvedExpiry || normalizeExpiryInput(expiryInput);

      ops.push({
        updateOne: {
          filter: { security_id: sid, expiry_date },
          update: {
            $set: {
              trading_symbol: m.trading_symbol || "",
              instrument_type: "FUTSTK",
              exchange: "NSE_FNO",
              LTP,
              open,
              high,
              low,
              close,
              received_at: now,
            },
            $setOnInsert: {
              security_id: sid,
              expiry_date,
            },
          },
          upsert: true,
        },
      });

      // mirror each refreshed row as a tick
      tickDocs.push(
        buildTickDoc(sid, m, expiry_date, { last_price: LTP, ohlc: { open, high, low, close } }, now)
      );
    }

    if (ops.length) {
      const res = await db!.collection("nse_futstk_ohlc").bulkWrite(ops, { ordered: false });
      const up =
        (res.upsertedCount as number) ??
        (Array.isArray((res as any).upsertedIds) ? (res as any).upsertedIds.length : 0) ??
        0;
      const mod = (res.modifiedCount as number) ?? 0;
      upserted += up;
      modified += mod;
      console.log(`💾 FUTSTK batch refreshed: upserted=${up} modified=${mod}`);
    }

    if (tickDocs.length) {
      // mark the source of these tick docs
      for (const t of tickDocs) t.source = "refresh";
      const tr = await db!.collection("nse_futstk_ticks").insertMany(tickDocs, { ordered: false });
      tickInserted += (tr as any).insertedCount ?? Object.keys((tr as any).insertedIds || {}).length;
    }
  }

  console.log(
    `✅ FUTSTK OHLC refreshed | expiry=${resolvedExpiry || normalizeExpiryInput(expiryInput)} matched=${ids.length} fetched=${fetched} upserted=${upserted} modified=${modified} ticks=${tickInserted} (allowZero=${allowZero})`
  );
  return { matched: ids.length, fetched, upserted, modified, tickInserted, resolvedExpiry };
};

/* ====================================================================== */
/* ==================== Background refresher (cron-ish) ================= */
/* ====================================================================== */

async function findActiveFutstkExpiry(): Promise<string | null> {
  if (!db) return null;

  const todayIso = normalizeExpiryInput(new Date().toISOString().slice(0, 10));

  const distinct = await db
    .collection("instruments")
    .distinct("expiry_date", { instrument_type: "FUTSTK", expiry_date: { $ne: null } });

  const dates = (distinct as string[])
    .filter(Boolean)
    .map((s) => s.trim())
    .filter((s) => /^\d{4}-\d{2}-\d{2}$/.test(s))
    .sort();

  if (!dates.length) return null;

  const todayT = new Date(todayIso + "T00:00:00Z").getTime();
  let best: string | null = null;
  for (const iso of dates) {
    const t = new Date(iso + "T00:00:00Z").getTime();
    if (t >= todayT) {
      best = iso;
      break;
    }
  }
  return best || dates[dates.length - 1];
}

/**
 * Starts an interval task that refreshes (upsert) FUTSTK OHLC in-place
 * and appends a tick for every refresh.
 *
 * Env:
 *  - FUTSTK_REFRESH_ENABLED=true|false (default true)
 *  - FUTSTK_REFRESH_MS=10000 (min 10000)
 *  - FUTSTK_REFRESH_EXPIRY=auto | yyyy-mm-dd
 *  - FUTSTK_FORCE_ZERO=false
 */
export function startFutstkOhlcRefresher() {
  const enabled = (process.env.FUTSTK_REFRESH_ENABLED || "true").toLowerCase() !== "false";
  if (!enabled) {
    console.log("⏸️ FUTSTK refresher disabled by env FUTSTK_REFRESH_ENABLED=false");
    return;
  }

  const rawMs = Number(process.env.FUTSTK_REFRESH_MS || 10000);
  const intervalMs = Math.max(10000, isFinite(rawMs) ? rawMs : 10000); // 🔟 seconds min
  const configuredExpiry = (process.env.FUTSTK_REFRESH_EXPIRY || "auto").trim().toLowerCase();
  const forceZero = (process.env.FUTSTK_FORCE_ZERO || "false").toLowerCase() === "true";

  console.log(
    `▶️ FUTSTK refresher started: every ${intervalMs} ms | expiry=${configuredExpiry} | forceZero=${forceZero}`
  );

  let ticking = false;

  const tick = async () => {
    if (ticking) return;
    ticking = true;
    try {
      if (!db) return;
      if (!isMarketOpen()) {
        return; // quiet outside market hours
      }

      let expiryToUse: string | null = null;
      if (configuredExpiry === "auto") {
        expiryToUse = await findActiveFutstkExpiry();
      } else {
        expiryToUse = normalizeExpiryInput(configuredExpiry);
      }

      if (!expiryToUse) {
        console.warn("⚠️ FUTSTK refresher: could not resolve an expiry to refresh.");
        return;
      }

      await refreshAndUpsertFutstkOhlc(expiryToUse, forceZero);
    } catch (err: any) {
      console.warn("❗ FUTSTK refresher tick error:", err?.message || err);
    } finally {
      ticking = false;
    }
  };

  setInterval(tick, intervalMs);
}

