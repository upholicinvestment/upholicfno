import axios from "axios";
import { Db } from "mongodb";
import csvParser from "csv-parser";
import { Readable } from "stream";

let db: Db | null = null;

export const setInstrumentDatabase = (database: Db) => {
  db = database;
  // helpful indexes (idempotent)
  db.collection("instruments").createIndex({ security_id: 1 }, { unique: true }).catch(() => {});
  db.collection("instruments").createIndex({ trading_symbol: 1 }).catch(() => {});
};

/**
 * Fetch and store instrument metadata from Dhan Scrip Master CSV (public CDN).
 */
export const fetchAndStoreInstruments = async () => {
  try {
    console.log("📡 Fetching instrument master from Dhan...");
    const response = await axios.get(
      "https://images.dhan.co/api-data/api-scrip-master-detailed.csv",
      { responseType: "stream", timeout: 30000 }
    );

    const dataStream = response.data as Readable;
    const instruments: any[] = [];

    await new Promise<void>((resolve, reject) => {
      dataStream
        .pipe(csvParser())
        .on("data", (row: any) => {
          instruments.push({
            security_id: parseInt(row["SECURITY_ID"] || "0", 10),
            trading_symbol: row["SYMBOL_NAME"] || "",
            instrument_type: row["INSTRUMENT"] || "",
            expiry_date: row["SM_EXPIRY_DATE"] || null,
            strike_price: parseFloat(row["STRIKE_PRICE"] || "0"),
            option_type: row["OPTION_TYPE"] || "",
            expiry_flag: row["EXPIRY_FLAG"] || "",
          });
        })
        .on("end", resolve)
        .on("error", reject);
    });

    if (!db) throw new Error("Database not initialized");
    await db.collection("instruments").deleteMany({});
    if (instruments.length > 0) {
      await db.collection("instruments").insertMany(instruments, { ordered: false });
    }

    console.log(`💾 Saved ${instruments.length} instruments to DB.`);
  } catch (err) {
    console.error("❌ Error fetching/storing instruments:", err);
  }
};

/**
 * Get instrument metadata by security_id
 */
export const getInstrumentMetadata = async (security_id: number) => {
  if (!db) throw new Error("Database not initialized");
  return db.collection("instruments").findOne({ security_id });
};
