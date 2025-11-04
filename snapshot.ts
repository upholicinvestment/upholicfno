// src/optionchain/snapshot.ts
import { Express, Request, Response, Router } from "express";
import { Db } from "mongodb";

/**
 * Returns the latest option_chain snapshot from Mongo.
 * Adds `updated_at_ist` using $dateToString with Asia/Kolkata so clients see Indian time.
 */
export default function registerOptionChainSnapshot(app: Express, db: Db) {
  const router = Router();

  // GET /api/optionchain/snapshot?id=13&seg=IDX_I[&expiry=YYYY-MM-DD]
  router.get("/snapshot", async (req: Request, res: Response): Promise<void> => {
    try {
      const id = Number(req.query.id ?? 13);
      const seg = String(req.query.seg ?? "IDX_I");
      const expiry = req.query.expiry ? String(req.query.expiry) : undefined;

      const match: any = { underlying_security_id: id, underlying_segment: seg };
      if (expiry) match.expiry = expiry;

      const docs = await db.collection("option_chain").aggregate([
        { $match: match },
        { $sort: { updated_at: -1 } },
        { $limit: 1 },
        {
          $addFields: {
            updated_at_ist: {
              $dateToString: {
                date: "$updated_at",
                timezone: "Asia/Kolkata",
                format: "%Y-%m-%d %H:%M:%S.%L 'IST'",
              },
            },
          },
        },
      ]).toArray();

      if (!docs.length) {
        res.status(404).json({ error: "No snapshot" });
        return;
      }

      res.json(docs[0]);
    } catch (e: any) {
      res.status(500).json({ error: e?.message || "snapshot failed" });
    }
  });

  app.use("/api/optionchain", router);
}
