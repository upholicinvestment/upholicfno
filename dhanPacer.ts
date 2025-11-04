// src/utils/dhanPacer.ts

// A bucketed request pacer for Dhan REST calls.
// - Global floor (DHAN_GLOBAL_MIN_MS or DHAN_REST_MIN_MS; min 3000ms)
// - Per-bucket floors:
//     * OC bucket:   OC_ONLY_MIN_MS  (default 12000ms)
//     * Quote bucket:QUOTE_ONLY_MIN_MS (default 5000ms)
//     * Default:      DHAN_DEFAULT_MIN_MS (fallback to global)
// The scheduler always executes the job that becomes eligible the earliest.

type BucketName = "oc" | "quote" | "default";

type Job<T> = {
  fn: () => Promise<T>;
  bucket: BucketName;
  resolve: (v: T) => void;
  reject: (e: any) => void;
  enq: number; // enqueue time (for debugging/ordering if needed)
};

function toNum(v: any, fallback: number): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

// ---- Floors (ms)
let globalMin = Math.max(
  toNum(process.env.DHAN_GLOBAL_MIN_MS, toNum(process.env.DHAN_REST_MIN_MS, 3000)),
  3000
);

const bucketMin: Record<BucketName, number> = {
  default: Math.max(toNum(process.env.DHAN_DEFAULT_MIN_MS, globalMin), 0),
  oc: Math.max(toNum(process.env.OC_ONLY_MIN_MS, 12000), 3000),
  quote: Math.max(toNum(process.env.QUOTE_ONLY_MIN_MS, 5000), 0),
};

// ---- State
const queue: Job<any>[] = [];
let running = false;
let lastGlobal = 0;
const lastBucket: Record<BucketName, number> = { default: 0, oc: 0, quote: 0 };

// ---- Helpers
function delay(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// Exported getters/setters (optional diagnostics)
export function setGlobalMin(ms: number) {
  globalMin = Math.max(ms, 3000);
}
export function setBucketMin(bucket: BucketName, ms: number) {
  // Keep OC at least 3000 to respect provider minimums
  const floor = bucket === "oc" ? 3000 : 0;
  bucketMin[bucket] = Math.max(ms, floor);
}
export function getDhanMinGap() {
  return globalMin;
}
export function getDhanBucketGaps() {
  return { ...bucketMin };
}
export function getDhanLastTimes() {
  return { lastGlobal, lastBucket: { ...lastBucket } };
}

// ---- Core scheduler
export function schedule<T>(
  fn: () => Promise<T>,
  bucket: BucketName = "default"
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    queue.push({ fn, bucket, resolve, reject, enq: Date.now() });
    run();
  });
}

// Back-compat aliases / convenience
export const dhanSchedule = schedule; // default bucket
export const scheduleOC = <T>(fn: () => Promise<T>) => schedule(fn, "oc");
export const scheduleQuote = <T>(fn: () => Promise<T>) => schedule(fn, "quote");

// ---- Earliest-ready job executor (non-FIFO)
async function run() {
  if (running) return;
  running = true;

  function waitFor(job: Job<any>) {
    const now = Date.now();
    const sinceGlobal = now - lastGlobal;
    const sinceBucket = now - lastBucket[job.bucket];
    const needGlobal = Math.max(0, globalMin - sinceGlobal);
    const needBucket = Math.max(0, bucketMin[job.bucket] - sinceBucket);
    return Math.max(needGlobal, needBucket);
  }

  while (queue.length) {
    // pick the job with the smallest remaining wait
    let bestIdx = 0;
    let bestWait = waitFor(queue[0]);
    for (let i = 1; i < queue.length; i++) {
      const w = waitFor(queue[i]);
      if (w < bestWait) {
        bestWait = w;
        bestIdx = i;
      }
    }

    // take that job out of the queue
    const job = queue.splice(bestIdx, 1)[0];

    if (bestWait > 0) {
      await delay(bestWait);
    }

    try {
      const val = await job.fn();
      job.resolve(val);
    } catch (e) {
      job.reject(e);
    } finally {
      const t = Date.now();
      lastGlobal = t;
      lastBucket[job.bucket] = t;
    }
  }

  running = false;
}
