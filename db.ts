// src/utils/db.ts
import { MongoClient, Db } from "mongodb";

let primaryClient: MongoClient | null = null;
let primaryDb: Db | null = null;

let fnoClient: MongoClient | null = null;
let fnoDb: Db | null = null;

export async function connectPrimary(): Promise<Db> {
  if (primaryDb) return primaryDb;
  const uri = process.env.MONGO_URI;
  const name = process.env.MONGO_DB_NAME;
  if (!uri || !name) {
    throw new Error("Missing MONGO_URI or MONGO_DB_NAME in environment");
  }
  primaryClient = new MongoClient(uri, { maxPoolSize: 20 });
  await primaryClient.connect();
  primaryDb = primaryClient.db(name);
  console.log(`✅ Connected primary MongoDB -> ${name}`);
  return primaryDb;
}

export async function connectFno(): Promise<Db> {
  if (fnoDb) return fnoDb;
  const uri = process.env.MONGO_URII;
  const name = process.env.MONGO_DB_NAMEE;
  if (!uri || !name) {
    throw new Error("Missing MONGO_URII or MONGO_DB_NAMEE in environment");
  }
  fnoClient = new MongoClient(uri, { maxPoolSize: 20 });
  await fnoClient.connect();
  fnoDb = fnoClient.db(name);
  console.log(`✅ Connected FNO MongoDB -> ${name}`);
  return fnoDb;
}

/**
 * Connect both (useful for processes that need both DBs).
 * Will attempt to connect but won't throw if one is already connected.
 */
export async function connectAll(): Promise<{ primary?: Db; fno?: Db }> {
  const result: { primary?: Db; fno?: Db } = {};
  try {
    result.primary = await connectPrimary();
  } catch (e) {
    console.warn("connectPrimary failed:", (e as any)?.message || e);
  }
  try {
    result.fno = await connectFno();
  } catch (e) {
    console.warn("connectFno failed:", (e as any)?.message || e);
  }
  return result;
}

export function getPrimaryDb(): Db {
  if (!primaryDb) throw new Error("Primary DB not connected (call connectPrimary())");
  return primaryDb;
}

export function getFnoDb(): Db {
  if (!fnoDb) throw new Error("FNO DB not connected (call connectFno())");
  return fnoDb;
}

export async function closeAll(): Promise<void> {
  try {
    if (primaryClient) {
      await primaryClient.close();
      primaryClient = null;
      primaryDb = null;
    }
  } catch (e) {
    console.warn("Failed closing primary client:", (e as any)?.message || e);
  }
  try {
    if (fnoClient) {
      await fnoClient.close();
      fnoClient = null;
      fnoDb = null;
    }
  } catch (e) {
    console.warn("Failed closing fno client:", (e as any)?.message || e);
  }
}
