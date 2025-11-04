import { Db } from "mongodb";
import { getInstrumentMetadata } from "./instrument.service";

let db: Db | null = null;

export const setLtpDatabase = (database: Db) => {
  db = database;
  // indexes to keep reads snappy
  db.collection("ltp_history").createIndex({ timestamp: -1 }).catch(() => {});
  db.collection("ltp_history").createIndex({ security_id: 1, timestamp: -1 }).catch(() => {});
};

function getISTDate(): Date {
  // Node runs UTC; store raw Date and format at the edge/UI if needed
  return new Date();
}

function toNumber(n: any, d: number = 0): number {
  const x = Number(n);
  return Number.isFinite(x) ? x : d;
}

/** Normalize various incoming tick shapes into a small object for persistence. */
function normalizeTick(data: any) {
  const security_id = toNumber(
    data?.securityId ?? data?.security_id ?? data?.securityID ?? data?.instrument_token ?? data?.securityid,
    0
  );
  const LTP = toNumber(data?.LTP ?? data?.ltp ?? data?.last_price ?? data?.lastPrice, 0);
  return { security_id, LTP };
}

export const saveLTP = async (data: any) => {
  try {
    if (!db) throw new Error("Database not initialized");

    const base = normalizeTick(data);
    if (!base.security_id) {
      console.warn("⚠️  Dropping LTP tick with missing security_id:", data);
      return;
    }

    // Join with instrument metadata if available (best-effort)
    let meta: any = null;
    try {
      meta = await getInstrumentMetadata(base.security_id);
    } catch {}

    const ltpDoc = {
      security_id: base.security_id,
      LTP: base.LTP,
      trading_symbol: meta?.trading_symbol || "",
      instrument_type: meta?.instrument_type || "",
      expiry_date: meta?.expiry_date || null,
      strike_price: meta?.strike_price ?? 0,
      option_type: meta?.option_type || "",
      expiry_flag: meta?.expiry_flag || "",
      timestamp: getISTDate(),
    };

    await db.collection("ltp_history").insertOne(ltpDoc);
    // console.log("✅ Saved LTP to DB:", ltpDoc.security_id, ltpDoc.LTP);
  } catch (err) {
    console.error("❌ Error saving LTP data:", err);
  }
};

export const getRecentLTPs = async (limit = 50) => {
  if (!db) throw new Error("Database not initialized");
  return await db
    .collection("ltp_history")
    .find({})
    .sort({ timestamp: -1 })
    .limit(limit)
    .toArray();
};

