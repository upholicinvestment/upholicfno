import { Router, Request, Response } from "express";
import { getRecentLTPs } from "../dbfno/ltp.service";

const ltpRoutes = Router();

/**
 * GET /api/ltp/recent
 * Fetch the most recent LTP data.
 */
ltpRoutes.get("/recent", async (req: Request, res: Response): Promise<void> => {
  try {
    const ltpData = await getRecentLTPs();
    if (!ltpData.length) {
      res.status(404).json({ message: "No LTP data found" });
      return;
    }
    res.json(ltpData);
  } catch (err) {
    console.error("❌ Error fetching LTP data:", err);
    res.status(500).json({ message: "Failed to fetch LTP data" });
  }
});

export { ltpRoutes };
