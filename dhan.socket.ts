import WebSocket, { RawData } from "ws";
// import { saveLTP } from "../services/ltp.service";
import dotenv from "dotenv";

dotenv.config();

const MAX_BATCH_SIZE = 100;
const RECONNECT_INTERVAL = 5000;
const PING_INTERVAL = 30000;
const MAX_RECONNECT_ATTEMPTS = 5;

export class DhanSocket {
  private ws: WebSocket | null = null;
  private securityIds: number[] = [];
  private isConnected = false;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private reconnectAttempts = 0;
  private pingInterval: NodeJS.Timeout | null = null;
  private wsUrl: string;

  constructor(private token: string, private clientId: string) {
    this.wsUrl = `wss://api-feed.dhan.co?version=2&token=${this.token}&clientId=${this.clientId}&authType=2`;
    // console.log("WS URL:", this.wsUrl);
  }

  public async connect(securityIds: number[]) {
    this.securityIds = securityIds;
    this.cleanup();
    // console.log(`🔗 Connecting to Dhan WebSocket: ${this.wsUrl}`);
    this.ws = new WebSocket(this.wsUrl);
    this.setupEventHandlers();
  }

  private setupEventHandlers() {
    if (!this.ws) return;

    this.ws.on("open", () => {
      // console.log("✅ Connected to Dhan WebSocket");
      this.isConnected = true;
      this.reconnectAttempts = 0;
      this.setupPing();
      this.subscribeBatch(2, this.securityIds);
    });

    this.ws.on("message", (message: RawData) => this.handleMessage(message));
    this.ws.on("close", (code, reason) => {
      console.error(`❌ WebSocket closed. Code: ${code}, Reason: ${reason}`);
      this.handleDisconnect();
    });
    this.ws.on("error", (err) => {
      console.error("❌ WebSocket error:", err);
      this.handleDisconnect();
    });
    this.ws.on("pong", () => console.debug("🏓 Pong received"));
  }

  private handleMessage(message: RawData) {
    try {
      if (Buffer.isBuffer(message)) {
        const buf = message as Buffer;
        const feedCode = buf.readUInt8(0);
        const secId = buf.readInt32LE(4);

        if (feedCode === 2) {
          const ltp = buf.readFloatLE(8);
          // console.log(`💹 [Ticker] SecID=${secId} LTP=${ltp}`);
          // saveLTP({ securityId: secId, LTP: ltp });
        }
      }
    } catch (err) {
      console.error("Failed to parse incoming message:", err);
    }
  }

  private subscribeBatch(requestCode: number, ids: number[]) {
    if (!this.ws || !this.isConnected) return;

    for (let i = 0; i < ids.length; i += MAX_BATCH_SIZE) {
      const batch = ids.slice(i, i + MAX_BATCH_SIZE);
      const payload = {
        RequestCode: requestCode,
        InstrumentCount: batch.length,
        InstrumentList: batch.map((id) => ({
          ExchangeSegment: "NSE_FNO",
          SecurityId: id.toString(),
        })),
      };
      this.ws.send(JSON.stringify(payload));
      // console.log(`📡 Sent subscription batch:`, batch);
    }
  }

  private setupPing() {
    this.pingInterval = setInterval(() => {
      if (this.ws && this.isConnected) {
        this.ws.ping();
      }
    }, PING_INTERVAL);
  }

  private handleDisconnect() {
    this.isConnected = false;
    this.cleanup();
    if (this.reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
      this.reconnectAttempts++;
      // console.log(`♻ Reconnecting attempt ${this.reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}...`);
      this.reconnectTimeout = setTimeout(() => this.connect(this.securityIds), RECONNECT_INTERVAL);
    } else {
      console.error("❌ Max reconnection attempts reached.");
    }
  }

  private cleanup() {
    if (this.pingInterval) clearInterval(this.pingInterval);
    if (this.reconnectTimeout) clearTimeout(this.reconnectTimeout);
    if (this.ws) this.ws.removeAllListeners();
  }

  public disconnect() {
    if (this.ws) {
      this.ws.close();
    }
    this.cleanup();
  }
}

export default DhanSocket;
