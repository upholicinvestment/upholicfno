// src/optionchain/expiries.ts
import { Express, Request, Response, Router } from "express";
import { Db } from "mongodb";
import { fetchExpiryList } from "./option_chain";

export default function registerOptionChainExpiries(app: Express, _db: Db) {
  const router = Router();

  // GET /api/optionchain/expiries?id=13&seg=IDX_I
  router.get("/expiries", async (req: Request, res: Response): Promise<void> => {
    try {
      const id = Number(req.query.id ?? 13);
      const seg = String(req.query.seg ?? "IDX_I");
      const list = await fetchExpiryList(id, seg);
      res.json({ data: list });
    } catch (e: any) {
      res.status(500).json({ error: e?.message || "expirylist failed" });
    }
  });

  app.use("/api/optionchain", router);
}


