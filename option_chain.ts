// src/services/option_chain.ts
import axios from "axios";
import { scheduleOC } from "./dhanPacer";

/* ========= Env / headers ========= */
const DHAN_ACCESS_TOKEN = (process.env.DHAN_API_KEY || "").trim();
const DHAN_CLIENT_ID    = (process.env.DHAN_CLIENT_ID || "").trim();

function requireEnv() {
  if (!DHAN_ACCESS_TOKEN || !DHAN_CLIENT_ID) {
    throw new Error("Missing DHAN_API_KEY or DHAN_CLIENT_ID in .env");
  }
}
function dhanHeaders() {
  return {
    accept: "application/json",
    "content-type": "application/json",
    "access-token": DHAN_ACCESS_TOKEN,
    "client-id": DHAN_CLIENT_ID,
  };
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/* ========= Types ========= */
export type DhanGreeks = { delta: number; theta: number; gamma: number; vega: number };
export type DhanOptionLeg = {
  greeks: DhanGreeks;
  implied_volatility: number;
  last_price: number;
  oi: number;
  previous_close_price: number;
  previous_oi: number;
  previous_volume: number;
  top_ask_price: number;
  top_ask_quantity: number;
  top_bid_price: number;
  top_bid_quantity: number;
  volume: number;
};
export type DhanStrikeOC = { ce?: DhanOptionLeg; pe?: DhanOptionLeg };
export type DhanOCMap = Record<string, DhanStrikeOC>;
export type DhanOCResponse = { data: { last_price: number; oc: DhanOCMap } };

/* ========= Date helpers ========= */
function istToday(): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const m = Object.fromEntries(parts.map((p) => [p.type, p.value]));
  return `${m.year}-${m.month}-${m.day}`;
}

export function pickNearestExpiry(expiries: string[]): string | null {
  if (!expiries?.length) return null;
  const today = istToday();
  const sorted = expiries.slice().sort();
  for (const e of sorted) if (e >= today) return e;
  return sorted[sorted.length - 1] ?? null;
}

/* ========= Pacer-aware retry (OC bucket) ========= */
async function ocRetry<T>(fn: () => Promise<T>, tries = 4): Promise<T> {
  let lastErr: any;
  for (let i = 0; i < tries; i++) {
    try {
      return await scheduleOC(fn);
    } catch (e: any) {
      lastErr = e;
      const s = e?.response?.status;
      const body = e?.response?.data;
      if (s === 401) {
        console.error("❗ Dhan 401 in OC call. Headers/client-id/token likely invalid. Body:", body);
        throw e;
      }
      if (s === 429 || (s >= 500 && s < 600)) {
        const wait = Math.min(15000, 1000 * (1 << i));
        console.warn(`⏳ OC ${s} retry in ${wait}ms...`);
        await sleep(wait);
        continue;
      }
      throw e;
    }
  }
  throw lastErr;
}

/* ========= API calls ========= */
export async function fetchExpiryList(UnderlyingScrip: number, UnderlyingSeg: string): Promise<string[]> {
  requireEnv();
  try {
    const res = await ocRetry(
      () =>
        axios.post(
          "https://api.dhan.co/v2/optionchain/expirylist",
          { UnderlyingScrip, UnderlyingSeg },
          { headers: dhanHeaders(), timeout: 10000 }
        ),
      3
    );
    return Array.isArray(res.data?.data) ? (res.data.data as string[]) : [];
  } catch (e: any) {
    const s = e?.response?.status;
    console.error("expirylist error:", s, e?.response?.data || e?.message || e);
    return [];
  }
}

export async function fetchOptionChainRaw(
  UnderlyingScrip: number,
  UnderlyingSeg: string,
  Expiry: string
): Promise<DhanOCResponse> {
  requireEnv();
  const res = await ocRetry(
    () =>
      axios.post(
        "https://api.dhan.co/v2/optionchain",
        { UnderlyingScrip, UnderlyingSeg, Expiry },
        { headers: dhanHeaders(), timeout: 15000 }
      ),
    4
  );
  return { data: res.data?.data ?? { last_price: 0, oc: {} } };
}

/* ========= Convenience (SAFE) ========= */
export async function getLiveOptionChain(
  UnderlyingScrip: number,
  UnderlyingSeg: string,
  Expiry?: string
): Promise<{ expiry: string | null; data: { last_price: number; oc: DhanOCMap } }> {
  let expiry = (Expiry || "").trim();

  if (!expiry) {
    const list = await fetchExpiryList(UnderlyingScrip, UnderlyingSeg);
    const picked = pickNearestExpiry(list);

    if (!picked) {
      const fallback = (process.env.OC_EXPIRY || process.env.FUTSTK_REFRESH_EXPIRY || "").trim();
      if (fallback) {
        console.warn(`⚠️ No live expiries; using fallback env expiry: ${fallback}`);
        expiry = fallback;
      } else {
        return { expiry: null, data: { last_price: 0, oc: {} } };
      }
    } else {
      expiry = picked;
      await sleep(3100);
    }
  }

  const { data } = await fetchOptionChainRaw(UnderlyingScrip, UnderlyingSeg, expiry);
  return { expiry, data };
}

/* ========= Normalize ========= */
export function toNormalizedArray(oc: DhanOCMap) {
  return Object.entries(oc)
    .map(([k, v]) => ({ strike: Number(k), ...v }))
    .filter((r) => Number.isFinite(r.strike))
    .sort((a, b) => a.strike - b.strike);
}

