// // src/dbfno/oc_rows_cache.ts
// import { Db, MongoClient } from "mongodb";

// /* ========= Types ========= */
// type Signal = "Bullish" | "Bearish";
// export type Mode = "level" | "delta";
// export type Unit = "bps" | "pct" | "points";

// type TickDoc = {
//   underlying_security_id: number;
//   underlying_segment?: string;
//   underlying_symbol?: string;
//   expiry: string;
//   last_price?: number;
//   spot?: number;
//   strikes?: Array<{
//     strike: number;
//     ce?: { greeks?: { delta?: number }; oi?: number };
//     pe?: { greeks?: { delta?: number }; oi?: number };
//   }>;
//   ts: Date | string;
// };

// /**
//  * Final cache shape in Upholic_Score_Vol:
//  *  - expiry:      "YYYY-MM-DD"
//  *  - intervalMin: 3 | 5 | 15 | 30
//  *  - date:        "YYYY-MM-DD" (IST)
//  *  - time:        "HH:MM:SS IST"
//  *  - volatility:  number
//  *  - signal:      "Bullish" | "Bearish"
//  *  - spot:        number
//  */
// export type CacheDoc = {
//   expiry: string;
//   intervalMin: number;
//   date: string;
//   time: string;
//   signal: Signal;
//   spot: number;
//   volatility: number;
// };

// /* ========= Constants ========= */
// const TICKS_COLL = process.env.OC_SOURCE_COLL || "option_chain_ticks";
// const CACHE_COLL = "Upholic_Score_Vol"; // ✅ final target collection

// const VERBOSE =
//   (process.env.OC_ROWS_LOG_VERBOSE || "true").toLowerCase().trim() !== "false";

// /* ========= Helpers ========= */
// function toDateSafe(d: Date | string): Date {
//   return d instanceof Date ? d : new Date(d);
// }

// function timeIST(d: Date): string {
//   return (
//     new Intl.DateTimeFormat("en-IN", {
//       timeZone: "Asia/Kolkata",
//       hour12: false,
//       hour: "2-digit",
//       minute: "2-digit",
//       second: "2-digit",
//     }).format(d) + " IST"
//   );
// }

// function istDateString(utc: Date): string {
//   const parts = new Intl.DateTimeFormat("en-CA", {
//     timeZone: "Asia/Kolkata",
//     year: "numeric",
//     month: "2-digit",
//     day: "2-digit",
//   }).formatToParts(utc);

//   const y = parts.find((p) => p.type === "year")?.value || "0000";
//   const m = parts.find((p) => p.type === "month")?.value || "01";
//   const d = parts.find((p) => p.type === "day")?.value || "01";
//   return `${y}-${m}-${d}`;
// }

// /** Floor to N-minute bucket (ms since epoch, UTC) */
// function floorToBucket(d: Date, minutes: number): number {
//   const ms = minutes * 60 * 1000;
//   return Math.floor(d.getTime() / ms) * ms;
// }

// function gaussianWeight(k: number, spot: number, width = 300): number {
//   const d = k - spot;
//   return Math.exp(-(d * d) / (2 * width * width));
// }

// function signalFromPrice(prevSpot: number, spot: number): Signal {
//   return spot >= prevSpot ? "Bullish" : "Bearish";
// }

// function signalFromWeightedDelta(
//   strikes: TickDoc["strikes"] = [],
//   spot: number
// ): Signal {
//   let net = 0;
//   for (const s of strikes) {
//     const w = gaussianWeight(Number(s.strike), spot, 300);
//     const ceDelta = s.ce?.greeks?.delta ?? 0;
//     const peDelta = s.pe?.greeks?.delta ?? 0;
//     const ceOI = s.ce?.oi ?? 0;
//     const peOI = s.pe?.oi ?? 0;
//     net += w * (ceDelta * ceOI + peDelta * peOI);
//   }
//   return net >= 0 ? "Bullish" : "Bearish";
// }

// function volatilityValue(
//   prevSpot: number | undefined,
//   spot: number,
//   unit: Unit = "bps"
// ): number {
//   if (!prevSpot || prevSpot <= 0) return 0;
//   const diff = spot - prevSpot;

//   if (unit === "points") return Number(diff.toFixed(2));

//   const pct = (diff / prevSpot) * 100;
//   if (unit === "pct") return Number(pct.toFixed(2));

//   const bps = pct * 100;
//   return Number(bps.toFixed(2));
// }

// /* ========= Resolve active expiry ========= */
// async function resolveActiveExpiry(
//   client: MongoClient,
//   dbName: string,
//   underlying: number,
//   segment = "IDX_I"
// ): Promise<string | null> {
//   const db = client.db(dbName);

//   const snap = await db
//     .collection("option_chain")
//     .find({
//       underlying_security_id: underlying,
//       underlying_segment: segment,
//     } as any)
//     .project({ expiry: 1, updated_at: 1 })
//     .sort({ updated_at: -1 })
//     .limit(1)
//     .toArray();

//   if (snap.length && (snap[0] as any)?.expiry) {
//     return String((snap[0] as any).expiry);
//   }

//   const tick = await db
//     .collection(TICKS_COLL)
//     .find({
//       underlying_security_id: underlying,
//       underlying_segment: segment,
//     } as any)
//     .project({ expiry: 1, ts: 1 })
//     .sort({ ts: -1 })
//     .limit(1)
//     .toArray();

//   if (tick.length && (tick[0] as any)?.expiry) {
//     return String((tick[0] as any).expiry);
//   }

//   return null;
// }

// /* ========= Compute rows from ticks (per underlying) ========= */
// async function computeRowsFromTicksWindow(params: {
//   db: Db;
//   underlying: number;
//   segment: string;
//   expiry: string;
//   intervalMin: number;
//   since: Date;
//   mode: Mode;
//   unitStore?: Unit;
// }): Promise<{
//   rows: Array<{
//     date: string;
//     time: string;
//     volatility: number;
//     signal: Signal;
//     spot: number;
//   }>;
//   tickCount: number;
// }> {
//   const {
//     db,
//     underlying,
//     segment,
//     expiry,
//     intervalMin,
//     since,
//     mode,
//     unitStore = "bps",
//   } = params;

//   const coll = db.collection<TickDoc>(TICKS_COLL);

//   const ticksAscRaw = await coll
//     .find({
//       underlying_security_id: underlying,
//       underlying_segment: segment,
//       expiry,
//       ts: { $gte: since },
//     } as any)
//     .sort({ ts: 1 })
//     .toArray();

//   const ticksAsc = ticksAscRaw.map((t) => ({
//     ...t,
//     ts: toDateSafe(t.ts) as Date,
//   }));

//   const tickCount = ticksAsc.length;
//   if (tickCount < 2) return { rows: [], tickCount };

//   // last tick per bucket
//   const byBucket = new Map<number, TickDoc & { ts: Date }>();
//   for (const t of ticksAsc) {
//     const ts = t.ts as Date;
//     const key = floorToBucket(ts, intervalMin);
//     const prev = byBucket.get(key);
//     if (!prev || prev.ts < ts) byBucket.set(key, t as any);
//   }

//   const keysAsc = [...byBucket.keys()].sort((a, b) => a - b);
//   if (keysAsc.length < 2) return { rows: [], tickCount };

//   const rows: Array<{
//     date: string;
//     time: string;
//     volatility: number;
//     signal: Signal;
//     spot: number;
//   }> = [];

//   for (let i = 1; i < keysAsc.length; i++) {
//     const bucketMs = keysAsc[i];
//     const bucketUtc = new Date(bucketMs);

//     const curr = byBucket.get(bucketMs)!;
//     const prev = byBucket.get(keysAsc[i - 1])!;

//     const spot = Number(curr.spot ?? curr.last_price ?? 0);
//     const prevSpot = Number(prev.spot ?? prev.last_price ?? 0);

//     const volBps = volatilityValue(prevSpot, spot, "bps");
//     const volatility =
//       unitStore === "bps"
//         ? volBps
//         : unitStore === "pct"
//         ? volatilityValue(prevSpot, spot, "pct")
//         : volatilityValue(prevSpot, spot, "points");

//     let sig: Signal = signalFromPrice(prevSpot, spot);
//     if (mode === "delta") {
//       sig = signalFromWeightedDelta(curr.strikes, spot);
//     }

//     const date = istDateString(bucketUtc);
//     const time = timeIST(bucketUtc);

//     rows.push({ date, time, volatility, signal: sig, spot });
//   }

//   return { rows, tickCount };
// }

// /* ========= Index management ========= */
// async function dropLegacyOcRowsIndexes(db: Db) {
//   const coll = db.collection(CACHE_COLL);

//   try {
//     const idxes = await coll.listIndexes().toArray();
//     for (const idx of idxes) {
//       const name = idx.name || "";
//       const key = idx.key || {};

//       const legacy =
//         name.includes("oc_rows_cache") ||
//         name.includes("bucket_key") ||
//         name.includes("bucket") ||
//         name.includes("tsBucket") ||
//         name.includes("volatility_index_core_v2") ||
//         name.includes("volatility_index_query_v2") ||
//         Object.prototype.hasOwnProperty.call(key, "bucket_key") ||
//         Object.prototype.hasOwnProperty.call(key, "tsBucket");

//       if (legacy) {
//         try {
//           await coll.dropIndex(name);
//           if (VERBOSE) {
//             console.log(`[${CACHE_COLL}] Dropped legacy index: ${name}`);
//           }
//         } catch (e: any) {
//           console.warn(
//             `[${CACHE_COLL}] Failed dropping index ${name}:`,
//             e?.message || e
//           );
//         }
//       }
//     }
//   } catch (e: any) {
//     if (VERBOSE) {
//       console.warn(
//         `[${CACHE_COLL}] listIndexes failed (maybe no collection yet):`,
//         e?.message || e
//       );
//     }
//   }
// }

// export async function ensureVolatilityIndexIndexes(db: Db) {
//   const coll = db.collection<CacheDoc>(CACHE_COLL);

//   await dropLegacyOcRowsIndexes(db);

//   // Unique on (expiry, intervalMin, date, time)
//   try {
//     await coll.createIndex(
//       {
//         expiry: 1,
//         intervalMin: 1,
//         date: 1,
//         time: 1,
//       },
//       {
//         name: "volatility_index_core_v3",
//         unique: true,
//       }
//     );
//   } catch (e: any) {
//     if (
//       !String(e?.message || "")
//         .toLowerCase()
//         .includes("existing index")
//     ) {
//       throw e;
//     }
//     if (VERBOSE) {
//       console.warn("volatility_index_core_v3 already exists (ok)");
//     }
//   }

//   // Query helper index
//   try {
//     await coll.createIndex(
//       {
//         expiry: 1,
//         intervalMin: 1,
//         date: 1,
//         time: -1,
//       },
//       { name: "volatility_index_query_v3" }
//     );
//   } catch (e: any) {
//     if (
//       !String(e?.message || "")
//         .toLowerCase()
//         .includes("existing index")
//     ) {
//       throw e;
//     }
//     if (VERBOSE) {
//       console.warn("volatility_index_query_v3 already exists (ok)");
//     }
//   }
// }

// /* ========= Materializer ========= */
// export async function materializeOcRowsOnce(args: {
//   mongoUri: string;
//   dbName: string;
//   underlying: number;
//   segment?: string;
//   intervals: number[];
//   sinceMs?: number;
//   mode?: Mode;
//   unit?: Unit;
// }): Promise<Record<number, number>> {
//   const {
//     mongoUri,
//     dbName,
//     underlying,
//     segment = "IDX_I",
//     intervals,
//     sinceMs,
//     mode = "level",
//     unit = "bps",
//   } = args;

//   const client = new MongoClient(mongoUri);
//   await client.connect();

//   try {
//     const db = client.db(dbName);

//     await ensureVolatilityIndexIndexes(db);

//     const expiry = await resolveActiveExpiry(client, dbName, underlying, segment);
//     if (!expiry) {
//       if (VERBOSE) {
//         console.warn(
//           `[${CACHE_COLL}] No active expiry for ${underlying}/${segment}`
//         );
//       }
//       return Object.fromEntries(intervals.map((m) => [m, 0]));
//     }

//     const now = new Date();
//     const baseSince = sinceMs
//       ? new Date(now.getTime() - sinceMs)
//       : now;

//     const fallbackWindowsMs = Array.from(
//       new Set<number>([
//         sinceMs ?? 12 * 60 * 60 * 1000,
//         24 * 60 * 60 * 1000,
//         48 * 60 * 60 * 1000,
//         72 * 60 * 60 * 1000,
//       ])
//     ).sort((a, b) => a - b);

//     const results: Record<number, number> = {};

//     for (const intervalMin of intervals) {
//       let usedSince = baseSince;
//       let rowsOut: Array<{
//         date: string;
//         time: string;
//         volatility: number;
//         signal: Signal;
//         spot: number;
//       }> = [];
//       let tickCount = 0;

//       // base window
//       {
//         const { rows, tickCount: tc } = await computeRowsFromTicksWindow({
//           db,
//           underlying,
//           segment,
//           expiry,
//           intervalMin,
//           since: usedSince,
//           mode,
//           unitStore: unit,
//         });
//         rowsOut = rows;
//         tickCount = tc;
//       }

//       // fallbacks
//       if (!rowsOut.length) {
//         for (const ms of fallbackWindowsMs) {
//           const trySince = new Date(now.getTime() - ms);
//           const { rows, tickCount: tc } =
//             await computeRowsFromTicksWindow({
//               db,
//               underlying,
//               segment,
//               expiry,
//               intervalMin,
//               since: trySince,
//               mode,
//               unitStore: unit,
//             });

//           if (rows.length > 0) {
//             rowsOut = rows;
//             usedSince = trySince;
//             tickCount = tc;
//             break;
//           }
//         }
//       }

//       if (!rowsOut.length) {
//         if (VERBOSE) {
//           console.warn(
//             `[${CACHE_COLL}] no rows: u=${underlying}/${segment} exp=${expiry} interval=${intervalMin}m`
//           );
//         }
//         results[intervalMin] = 0;
//         continue;
//       }

//       const coll = db.collection<CacheDoc>(CACHE_COLL);
//       const bulk = coll.initializeUnorderedBulkOp();

//       for (const r of rowsOut) {
//         bulk
//           .find({
//             expiry,
//             intervalMin,
//             date: r.date,
//             time: r.time,
//           } as any)
//           .upsert()
//           .update({
//             $set: {
//               // final minimal shape only:
//               expiry,
//               intervalMin,
//               date: r.date,
//               time: r.time,
//               signal: r.signal,
//               spot: r.spot,
//               volatility: r.volatility,
//             },
//           });
//       }

//       const res = await bulk.execute();
//       const upserts =
//         (res.modifiedCount ?? 0) + (res.upsertedCount ?? 0);

//       if (VERBOSE) {
//         console.log(
//           `[${CACHE_COLL}] u=${underlying}/${segment} exp=${expiry} interval=${intervalMin}m upserts=${upserts} ticks=${tickCount} since=${usedSince.toISOString()}`
//         );
//       }

//       results[intervalMin] = upserts;
//     }

//     return results;
//   } finally {
//     try {
//       await client.close();
//     } catch {
//       // ignore
//     }
//   }
// }

// /* ========= Background scheduler ========= */
// export function startOcRowsMaterializer(opts: {
//   mongoUri: string;
//   dbName: string;
//   underlyings: Array<{ id: number; segment?: string }>;
//   intervals: number[];
//   sinceMs?: number;
//   scheduleMs?: number;
//   mode?: Mode;
//   unit?: Unit;
// }): NodeJS.Timeout {
//   const {
//     mongoUri,
//     dbName,
//     underlyings,
//     intervals,
//     sinceMs = 12 * 60 * 60 * 1000,
//     scheduleMs = 60_000,
//     mode = "level",
//     unit = "bps",
//   } = opts;

//   (async () => {
//     for (const u of underlyings) {
//       try {
//         const res = await materializeOcRowsOnce({
//           mongoUri,
//           dbName,
//           underlying: u.id,
//           segment: u.segment || "IDX_I",
//           intervals,
//           sinceMs,
//           mode,
//           unit,
//         });
//         if (VERBOSE) {
//           console.log(`⛏️ ${CACHE_COLL} initial:`, {
//             underlying: u.id,
//             res,
//           });
//         }
//       } catch (e: any) {
//         console.warn(
//           `${CACHE_COLL} initial fill error:`,
//           e?.message || e
//         );
//       }
//     }
//   })();

//   const timer = setInterval(async () => {
//     for (const u of underlyings) {
//       try {
//         const res = await materializeOcRowsOnce({
//           mongoUri,
//           dbName,
//           underlying: u.id,
//           segment: u.segment || "IDX_I",
//           intervals,
//           sinceMs,
//           mode,
//           unit,
//         });
//         if (VERBOSE) {
//           console.log(`⛏️ ${CACHE_COLL} sweep:`, {
//             underlying: u.id,
//             res,
//           });
//         }
//       } catch (e: any) {
//         console.warn(
//           `${CACHE_COLL} sweep error:`,
//           e?.message || e
//         );
//       }
//     }
//   }, Math.max(15_000, scheduleMs));

//   return timer;
// }

