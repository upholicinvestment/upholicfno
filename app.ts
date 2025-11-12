// src/app.ts
import express, { Request, Response, NextFunction, Express } from "express";
import cors from "cors";
import dotenv from "dotenv";
import compression from "compression"; // ✅ NEW
import { Db } from "mongodb";
import { createServer } from "http";
import { Server as SocketIOServer } from "socket.io";

dotenv.config();

/* ---------- DB helper (single place) ---------- */
import { connectPrimary, connectFno, getPrimaryDb, closeAll } from "./db";

/* ---------- Market / data sockets & services (KEEP ONLY THESE) ---------- */
import { GexLevelsCalc, startGexLevelsEveryMinute } from "./gexLevelsCalc";
import { AdvDecSave, startAdvDecMinuteJob } from "./advdecSave";

import { DhanSocket } from "./dhan.socket";
import { ltpRoutes } from "./ltp.route";
import { setLtpDatabase } from "./ltp.service";

import {
  fetchAndStoreInstruments,
  setQuoteDatabase,
  startFutstkOhlcRefresher,
} from "./quote.service";
import { setInstrumentDatabase } from "./instrument.service";

/* ---------- Option Chain deps & public endpoints (ADDED) ---------- */
import {
  fetchExpiryList,
  pickNearestExpiry,
  fetchOptionChainRaw,
  getLiveOptionChain,
  toNormalizedArray,
  DhanOptionLeg,
} from "./option_chain";
import registerOptionChainExpiries from "./expiries";
import registerOptionChainSnapshot from "./snapshot";
import { istNowString, istTimestamp } from "./time";
import { getDhanMinGap } from "./dhanPacer";

/// small helpers
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
function getISTDate(): Date {
  return new Date();
}
function isMarketOpen(): boolean {
  const now = getISTDate();
  const day = now.getDay(); // 0=Sun,6=Sat
  if (day === 0 || day === 6) return false;
  const totalMinutes = now.getHours() * 60 + now.getMinutes();
  // NSE: 09:15–15:30 IST
  return totalMinutes >= 9 * 60 + 15 && totalMinutes <= 15 * 60 + 30;
}

/* ====================================================================== */
/* ================== Option Chain Watcher (safe scheduler) ============== */
/* ====================================================================== */

function strikeStepFromKeys(keys: string[]): number {
  const n = Math.min(keys.length, 20);
  const nums = keys
    .slice(0, n)
    .map((k) => Number(k))
    .filter(Number.isFinite);
  nums.sort((a, b) => a - b);
  let step = 50;
  for (let i = 1; i < nums.length; i++) {
    const diff = Math.abs(nums[i] - nums[i - 1]);
    if (diff > 0) {
      step = diff;
      break;
    }
  }
  return step;
}
function roundToStep(price: number, step: number): number {
  return Math.round(price / step) * step;
}
function isActive(leg?: DhanOptionLeg): boolean {
  if (!leg) return false;
  return (
    (leg.last_price ?? 0) > 0 ||
    (leg.top_bid_price ?? 0) > 0 ||
    (leg.top_ask_price ?? 0) > 0 ||
    (leg.oi ?? 0) > 0 ||
    (leg.volume ?? 0) > 0
  );
}

type OcWatcherHandle = { stop: () => void };

async function startOptionChainWatcher(db: Db): Promise<OcWatcherHandle> {
  if ((process.env.OC_DISABLED || "false").toLowerCase() === "true") {
    // console.log("⏸️  [OC] watcher disabled by env OC_DISABLED=true");
    return { stop: () => {} };
  }

  const sym = process.env.OC_SYMBOL || "NIFTY";
  const id = Number(process.env.OC_UNDERLYING_ID || 13);
  const seg = process.env.OC_SEGMENT || "IDX_I";
  const windowSteps = Number(process.env.OC_WINDOW_STEPS || 15);
  const pcrSteps = Number(process.env.OC_PCR_STEPS || 3);
  const verbose =
    (process.env.OC_LOG_VERBOSE || "true").toLowerCase() === "true";

  // Market-hours gate & closed sleep
  const MARKET_HOURS_ONLY =
    (process.env.OC_MARKET_HOURS_ONLY || "true").toLowerCase() !== "false";
  const CLOSED_SLEEP_MS = Math.max(
    10_000,
    Number(process.env.OC_CLOSED_MS || 60_000)
  );
  let lastClosedLog = 0; // rate-limit the closed log to once/min

  const BASE_MIN = 3100;
  let baseInterval = Math.max(Number(process.env.OC_LIVE_MS || 7000), BASE_MIN);

  let backoffSteps = 0;
  const MAX_BACKOFF_STEPS = 12;
  const STEP_MS = 1000;

  const START_OFFSET_MS = Number(process.env.OC_START_OFFSET_MS || 1600);

  // Helpful indexes (idempotent)
  try {
    await db
      .collection("option_chain")
      .createIndex(
        { underlying_security_id: 1, underlying_segment: 1, expiry: 1 },
        { unique: true }
      );
    await db.collection("option_chain").createIndex({ "strikes.strike": 1 });
    await db.collection("option_chain_ticks").createIndex({
      underlying_security_id: 1,
      underlying_segment: 1,
      expiry: 1,
      ts: 1,
    });
  } catch (ixErr: any) {
    console.warn("[OC] index creation warning/error:", ixErr && ixErr.message ? ixErr.message : ixErr);
  }

  // Log which DB is being used by this watcher (helps confirm primary vs fnodb)
  try {
    console.log(`[OC] startOptionChainWatcher using DB: ${db?.databaseName || "<unknown>"}`);
  } catch (logErr: any) {
    // benign - don't break the watcher
    console.warn("[OC] failed to log DB name:", logErr && logErr.message ? logErr.message : logErr);
  }

  async function resolveExpiry(): Promise<string | null> {
    try {
      const exps = await fetchExpiryList(id, seg);
      const picked = pickNearestExpiry(exps);
      if (picked) {
        await sleep(3100);
        return picked;
      }
    } catch {
      /* ignore and try live fetch */
    }
    const res = await getLiveOptionChain(id, seg);
    // getLiveOptionChain may return null in some builds; normalize to null-safe here
    return (res as any)?.expiry ?? null;
  }

  let expiry: string | null = (process.env.OC_EXPIRY || "").trim() || null;
  if (!expiry) {
    // retry with gentle backoff until we get an expiry
    let waitMs = 15000;
    for (;;) {
      expiry = await resolveExpiry();
      if (expiry) break;
      console.warn(
        `⚠️ [OC] No expiry yet. Retrying in ${Math.floor(waitMs / 1000)}s...`
      );
      await sleep(waitMs);
      waitMs = Math.min(waitMs * 2, 5 * 60_000);
    }
  }

  console.log(
    `▶️  [OC] Live Option Chain for ${sym} ${id}/${seg} @ expiry ${expiry}`
  );
  console.log(
    `⏱️  [OC] Interval: ${baseInterval} ms (Dhan limit ≥ 3000 ms) | Global min gap: ${getDhanMinGap()} ms`
  );

  let stopped = false;
  let inFlight = false;

  const effectiveInterval = () =>
    Math.max(BASE_MIN, baseInterval + backoffSteps * STEP_MS) +
    Math.floor(Math.random() * 250);

  // Re-resolve expiry after a day change (first open tick of the day)
  let lastExpiryResolveDayKey = new Date().toISOString().slice(0, 10);
  const dayKey = () => new Date().toISOString().slice(0, 10);

  async function tickOnce() {
    if (stopped || inFlight) return;

    // Skip outside market hours (with rate-limited log)
    if (MARKET_HOURS_ONLY && !isMarketOpen()) {
      const now = Date.now();
      if (now - lastClosedLog > 60_000) {
        console.log("⏳ Market closed. Skipping Option chain.");
        lastClosedLog = now;
      }
      return;
    }

    inFlight = true;
    try {
      // Re-anchor expiry once per new day when we are within market hours
      const dk = dayKey();
      if (dk !== lastExpiryResolveDayKey) {
        try {
          const newExp = await resolveExpiry();
          if (newExp && newExp !== expiry) {
            console.log(
              `📅 [OC] New day detected → expiry ${expiry} → ${newExp}`
            );
            expiry = newExp;
          }
        } catch {}
        lastExpiryResolveDayKey = dk;
      }

      const ts = new Date();
      const ts_ist = istTimestamp(ts);

      if (!expiry) return; // guard
      const { data } = await fetchOptionChainRaw(id, seg, expiry); // paced inside service
      const norm = toNormalizedArray(data.oc);

      // Upsert latest snapshot (with logging)
      try {
        const upsertRes = await db.collection("option_chain").updateOne(
          { underlying_security_id: id, underlying_segment: seg, expiry },
          {
            $set: {
              underlying_security_id: id,
              underlying_segment: seg,
              underlying_symbol: sym,
              expiry,
              last_price: data.last_price,
              strikes: norm,
              updated_at: ts,
              updated_at_ist: ts_ist,
            },
          },
          { upsert: true }
        );
        console.log(`[OC] option_chain upsert result: matched=${upsertRes.matchedCount} modified=${upsertRes.modifiedCount} upsertedId=${JSON.stringify(upsertRes.upsertedId)}`);
      } catch (err: any) {
        console.error("[OC] option_chain upsert ERROR:", err && err.message ? err.message : err);
        // rethrow to let outer catch handle backoff etc.
        throw err;
      }

      // Append tick (with logging)
      try {
        const insertRes = await db.collection("option_chain_ticks").insertOne({
          underlying_security_id: id,
          underlying_segment: seg,
          underlying_symbol: sym,
          expiry,
          last_price: data.last_price,
          strikes: norm,
          ts,
          ts_ist,
        });
        console.log(`[OC] option_chain_ticks insertId: ${insertRes.insertedId}`);
      } catch (err: any) {
        console.error("[OC] option_chain_ticks insert ERROR:", err && err.message ? err.message : err);
        throw err;
      }

      if (backoffSteps > 0) backoffSteps = Math.max(0, backoffSteps - 1);

      if (verbose) {
        const keys = Object.keys(data.oc);
        const step = strikeStepFromKeys(keys);
        const atm = roundToStep(data.last_price, step);

        const windowed = norm.filter(
          (r) => Math.abs(r.strike - atm) <= windowSteps * step
        );
        const ceOIAll = windowed.reduce((a, r) => a + (r.ce?.oi ?? 0), 0);
        const peOIAll = windowed.reduce((a, r) => a + (r.pe?.oi ?? 0), 0);
        const pcrAll = ceOIAll > 0 ? peOIAll / ceOIAll : 0;

        const near = norm.filter(
          (r) => Math.abs(r.strike - atm) <= pcrSteps * step
        );
        const ceOINear = near.reduce((a, r) => a + (r.ce?.oi ?? 0), 0);
        const peOINear = near.reduce((a, r) => a + (r.pe?.oi ?? 0), 0);
        const pcrNear = ceOINear > 0 ? peOINear / ceOINear : 0;

        console.log(
          `[${istNowString()}] [OC] LTP:${
            data.last_price
          } ATM:${atm} PCR(±win):${pcrAll.toFixed(
            2
          )} | PCR(near):${pcrNear.toFixed(2)}`
        );
      }
    } catch (e: any) {
      const status = e?.response?.status;
      const msg = status
        ? `${status} ${e?.response?.statusText || ""}`.trim()
        : e?.message || String(e);
      console.warn(`[OC] Tick error: ${msg}`);
      if (status === 429 || (status >= 500 && status < 600)) {
        backoffSteps = Math.min(MAX_BACKOFF_STEPS, backoffSteps + 1);
        console.warn(`[OC] Backing off → interval ~${effectiveInterval()} ms`);
      }
    } finally {
      inFlight = false;
    }
  }

  async function loop() {
    await sleep(START_OFFSET_MS); // stagger vs quotes
    while (!stopped) {
      await tickOnce();
      // Longer sleep when market is closed
      const delay =
        MARKET_HOURS_ONLY && !isMarketOpen()
          ? CLOSED_SLEEP_MS
          : effectiveInterval();
      await sleep(delay);
    }
  }

  loop();
  return {
    stop: () => {
      // actually stop
      stopped = true;
      console.log("🛑 [OC] watcher stopped.");
    },
  };
}

/* ====================================================================== */
/* ============================ App runtime ============================= */
/* ====================================================================== */

const app: Express = express();
const httpServer = createServer(app);

app.use(
  cors({
    origin: process.env.CLIENT_URL || "http://localhost:5173",
    credentials: true,
  })
);
app.use(compression()); // ✅ NEW: gzip/brotli
app.use(express.json());

/* ================== Dhan WebSocket (LTP) ================== */
const dhanSocket = new DhanSocket(
  process.env.DHAN_API_KEY || "",
  process.env.DHAN_CLIENT_ID || ""
);

/* ================== DB connect + server start (trimmed) ================== */
let ocWatcherHandle: OcWatcherHandle | null = null;
/* NEW: keep a ref to the OC rows materializer interval so we can clear it on shutdown */
let ocRowsTimer: NodeJS.Timeout | null = null;

async function startServer() {
  try {
    // connect to primary DB using centralized helper
    await connectPrimary();
    const db: Db = getPrimaryDb();

    console.log("✅ Connected to primary MongoDB (app.ts)");

    // Attempt to connect to FNO DB and prefer it for FNO-related services.
    let fnoDb: Db | null = null;
    try {
      // connectFno() throws if env missing / connection fails
      fnoDb = await connectFno();
      console.log("✅ Connected to FNO MongoDB (app.ts) ->", fnoDb.databaseName);
    } catch (e: any) {
      console.warn("⚠️ connectFno() failed or not configured — continuing with primary DB for FNO services");
    }

    /* ---- Inject DB into modules ---- */
    // prefer fnoDb for LTP/quote/instrument flows; fall back to primary `db`
    const dbForFnoWork = fnoDb ?? db;
    setLtpDatabase(dbForFnoWork);
    setQuoteDatabase(dbForFnoWork);
    setInstrumentDatabase(dbForFnoWork);

    // Register lightweight APIs that exist in this trimmed file
    app.use("/api/ltp", ltpRoutes);

    // Register GEX & AdvDec features (kept)
    try {
      GexLevelsCalc(app, db);
      startGexLevelsEveryMinute(db);
      AdvDecSave(app, db);

      // start the 1-minute advdec saver
      startAdvDecMinuteJob(db, {
        bin: 5,
        sinceMin: 1440,
        symbol: process.env.ADVDEC_SYMBOL || "NIFTY",
      });
    } catch (e: any) {
      console.warn("⚠️ GEX/AdvDec init skipped:", e?.message || e);
    }

    /* ================== Data boot (kept) ================== */
    try {
      await fetchAndStoreInstruments();
    } catch (e: any) {
      console.warn("⚠️ fetchAndStoreInstruments failed:", e?.message || e);
    }

    // FUTSTK refresher (optional, kept)
    try {
      // don't change your call signature — the database used by the refresher
      // will be the one set via setQuoteDatabase() above (we passed dbForFnoWork)
      startFutstkOhlcRefresher();
    } catch (e: any) {
      console.warn("⚠️ startFutstkOhlcRefresher failed:", e?.message || e);
    }

    /* ============ REGISTER OPTION-CHAIN PUBLIC ENDPOINTS ============ */
    try {
      registerOptionChainExpiries(app, db);
      console.log("✅ Option chain expiries endpoint registered");
    } catch (e: any) {
      console.warn("⚠️ registerOptionChainExpiries failed:", e?.message || e);
    }
    try {
      registerOptionChainSnapshot(app, db);
      console.log("✅ Option chain snapshot endpoint registered");
    } catch (e: any) {
      console.warn("⚠️ registerOptionChainSnapshot failed:", e?.message || e);
    }

    /* ================== Start OC watcher ================== */
    try {
      // START THE WATCHER USING THE FNO DB (dbForFnoWork) so OC writes go to Upholicfno
      ocWatcherHandle = await startOptionChainWatcher(dbForFnoWork);
      console.log("✅ Option chain watcher started");
    } catch (e: any) {
      console.warn("⚠️ startOptionChainWatcher failed:", e?.message || e);
    }

    /* ================== Start HTTP + Socket.IO ================== */
    const PORTA = Number(process.env.PORTA) || 8100;
    httpServer.listen(PORTA, () => {
      console.log(`🚀 Server running at http://localhost:${PORTA}`);
      console.log(`🔗 Allowed CORS origin: ${process.env.CLIENT_URL || "http://localhost:5173"}`);
    });
  } catch (err: any) {
    console.error("❌ app.ts startup error:", err);
    try {
      await closeAll();
    } catch {}
    process.exit(1);
  }
}

startServer();

/* ================== Socket.IO ================== */
const io = new SocketIOServer(httpServer, {
  cors: {
    origin: process.env.CLIENT_URL || "http://localhost:5173",
    methods: ["GET", "POST"],
  },
});
io.on("connection", (socket) => {
  console.log("🔌 New client connected:", socket.id);
  socket.on("disconnect", (reason) =>
    console.log(`Client disconnected (${socket.id}):`, reason)
  );
});

/* ================== Graceful shutdown ================== */
async function shutdown(code = 0) {
  console.log("🛑 Shutting down gracefully...");
  try {
    // stop dhan socket if it exposes a stop method
    try {
      // @ts-ignore
      if (typeof dhanSocket?.stop === "function") dhanSocket.stop();
    } catch {}
  } catch {}
  try {
    ocWatcherHandle?.stop();
  } catch {}
  try {
    if (ocRowsTimer) clearInterval(ocRowsTimer);
  } catch {}
  try {
    await closeAll(); // centralized close
  } catch {}
  httpServer.close(() => {
    console.log("✅ Server closed");
    process.exit(code);
  });
}
process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));

export { io };


