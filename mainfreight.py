#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Odoo XML-RPC job:
1) Search stock.picking without tracking but with completed/sent transfers.
2) For each picking, resolve region_code + service_code from tpl_transfer_ids->tpl_provider->default_tpl_warehouse_id.
3) Call Mainfreight Tracking References API using sale order number (or origin) as reference.
4) Extract tracking_number, tracking_url, and shipping_method.
5) Update transfer state to 'done', create tpl.shipment, and link it to the picking (m2m).
"""

import os
import sys
from typing import Optional, Tuple
import requests
import xmlrpc.client
from collections import defaultdict

import logging
from logging.handlers import RotatingFileHandler
import sqlite3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# ---------- .env ----------
ENV_PATH = Path(__file__).parent / ".env"
loaded = load_dotenv(dotenv_path=ENV_PATH, override=False)

# ------------------------ CONFIG ------------------------
ODOO_URL = os.getenv("ODOO_URL", "https://your-odoo-host.com")
ODOO_DB = os.getenv("ODOO_DB", "your_db")
ODOO_USER = os.getenv("ODOO_USER", "admin@example.com")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "your_password")

MAINFREIGHT_API_KEY = os.getenv("MAINFREIGHT_API_KEY", "YOUR_MAINFREIGHT_KEY")
MAINFREIGHT_BASE = "https://api.mainfreight.com/Tracking/1.1/References"

# Optional: safety limits
MAX_PICKINGS = int(os.getenv("MAX_PICKINGS", "50"))
HTTP_TIMEOUT = 30
COMPANY_IDS = eval(os.getenv("COMPANY_IDS", "[]"))

# Logging config
LOG_PATH = os.getenv("LOG_PATH", "tpl_tracking.log")
DB_PATH = os.getenv("DB_PATH", "tpl_tracking.db")

# Google Sheets (optional)
ENABLE_GSHEETS = os.getenv("ENABLE_GSHEETS", "false").lower() in ("1", "true", "yes", "y")
GSHEETS_SA_FILE = os.getenv("GSHEETS_SA_FILE", "./sa.json")
GSHEETS_SPREADSHEET_ID = os.getenv("GSHEETS_SPREADSHEET_ID", "")
GSHEETS_TAB = os.getenv("GSHEETS_TAB", "Logs")
GSHEETS_BATCH_SIZE = int(os.getenv("GSHEETS_BATCH_SIZE", "1"))

# ------------------------ LOG ------------------------
def setup_logging():
    logger = logging.getLogger("tpl_tracking")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    ch.setFormatter(formatter)

    fh = RotatingFileHandler(LOG_PATH, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

class SQLiteSink:
    def __init__(self, path=DB_PATH):
        self.path = path
        self._ensure_schema()

    def _ensure_schema(self):
        with sqlite3.connect(self.path) as cx:
            cx.execute("""
                CREATE TABLE IF NOT EXISTS tracking_runs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts_utc TEXT NOT NULL,
                  picking_id INTEGER,
                  reference TEXT,
                  region TEXT,
                  service_type TEXT,
                  status TEXT,
                  message TEXT,
                  tracking_number TEXT,
                  tracking_url TEXT,
                  shipping_method TEXT
                )
            """)
            cx.execute("CREATE INDEX IF NOT EXISTS idx_runs_main ON tracking_runs(picking_id, status, ts_utc)")
            cx.commit()

    def log(self, *, ts_utc=None, picking_id=None, reference=None, region=None, service_type=None,
            status:str="", message:str="", tracking_number=None, tracking_url=None, shipping_method=None):
        if ts_utc is None:
            ts_utc = datetime.utcnow().isoformat(timespec="seconds")
        with sqlite3.connect(self.path) as cx:
            cx.execute("""
                INSERT INTO tracking_runs
                  (ts_utc, picking_id, reference, region, service_type, status, message, tracking_number, tracking_url, shipping_method)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ts_utc, picking_id, reference, region, service_type, status, message,
                tracking_number, tracking_url, shipping_method
            ))
            cx.commit()

# --------- Google Sheets sink (optional) ----------
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None

class GSheetsSink:
    def __init__(self, sa_file: str, spreadsheet_id: str, tab_name: str = "Logs", batch_size: int = 1):
        if gspread is None or Credentials is None:
            raise RuntimeError("gspread/google-auth not installed. Run: pip install gspread google-auth")
        
        SA_FILE_PATH = (Path(__file__).parent / sa_file).resolve()
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.creds = Credentials.from_service_account_file(str(SA_FILE_PATH), scopes=scopes)
        self.gc = gspread.authorize(self.creds)
        self.sh = self.gc.open_by_key(spreadsheet_id)
        self.ws = self._ensure_worksheet(tab_name)
        self.batch = []
        self.batch_size = max(1, int(batch_size))

        # header
        if len(self.ws.get_all_values()) == 0:
            self.ws.append_row([
                "ts_utc","picking_id","reference","region","service_type",
                "status","message","tracking_number","tracking_url","shipping_method"
            ])

    def _ensure_worksheet(self, name: str):
        try:
            return self.sh.worksheet(name)
        except gspread.WorksheetNotFound:
            return self.sh.add_worksheet(title=name, rows=1000, cols=12)

    def log(self, *, ts_utc: str, picking_id=None, reference=None, region=None, service_type=None,
            status:str="", message:str="", tracking_number=None, tracking_url=None, shipping_method=None):
        row = [
            ts_utc,
            str(picking_id or ""),
            reference or "",
            region or "",
            service_type or "",
            status or "",
            message or "",
            tracking_number or "",
            tracking_url or "",
            shipping_method or "",
        ]
        self.batch.append(row)
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.batch:
            return
        self.ws.append_rows(self.batch, value_input_option="RAW")
        self.batch.clear()

# Fan-out logger
def log_all(sqlite_sink: SQLiteSink, gsheets_sink: 'GSheetsSink|None', **kwargs):
    ts = datetime.utcnow().isoformat(timespec="seconds")
    sqlite_sink.log(ts_utc=ts, **kwargs)
    if gsheets_sink:
        try:
            gsheets_sink.log(ts_utc=ts, **kwargs)
        except Exception as e:
            logging.getLogger("tpl_tracking").warning(f"GSheets log failed: {e}")

# ------------------------ HELPERS ------------------------
def od(name: str):
    return xmlrpc.client.ServerProxy(f"{ODOO_URL}/{name}")

def mf_get_tracking(region: str, service_type: str, reference: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Call Mainfreight Tracking/References, return (tracking_number, tracking_url, shipping_method, date_shipped_str).
    """
    headers = {"Authorization": f"Secret {MAINFREIGHT_API_KEY}", "Content-Type": "application/json"}
    params = {"serviceType": service_type, "reference": reference, "region": region}
    r = requests.get(MAINFREIGHT_BASE, params=params, headers=headers, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    # data = [
    #   {
    #     "ourReference": "44775816",
    #     "yourReference": "JCA11425-217365",
    #     "serviceType": "WarehousingCA",
    #     "trackingUrl": "https://www.mainfreight.com/track/MIMSAMO/44775816",
    #     "relatedItems": [],
    #     "carrierReferences": [
    #       {
    #         "reference": "1ZE5E8142094546050",
    #         "carrierName": "UPS",
    #         "trackingUrl": "https://wwwapps.ups.com/tracking/tracking.cgi?tracknum=1ZE5E8142094546050"
    #       }
    #     ],
    #     "events": [
    #       {
    #         "sequence": 0,
    #         "eventDateTime": "2025-08-12T14:23:00",
    #         "groupingLevel1Code": "Complete",
    #         "groupingLevel2Code": "OrderComplete",
    #         "code": "Complete",
    #         "displayName": "Order Sent",
    #         "isEstimate": False
    #       }
    #     ],
    #     "relatedReferences": [],
    #     "receiverReferences": [],
    #     "senderReferences": [],
    #     "containerReferences": []
    #   }
    # ]
    if not isinstance(data, list) or not data:
        return (None, None, None, None)
    item = data[0]
    tracking_number = None
    tracking_url = None
    shipping_method = None
    date_shipped = None

    cr = item.get("carrierReferences") or []
    if isinstance(cr, list) and cr:
        tracking_url = (cr[0] or {}).get("trackingUrl")
        shipping_method = (cr[0] or {}).get("carrierName")
        tracking_number = (cr[0] or {}).get("reference")

    api_events = item.get("events") or []
    if isinstance(api_events, list) and api_events:
        date_str = api_events[0].get("eventDateTime")
        if date_str:
            date_shipped = datetime.fromisoformat(date_str).strftime("%Y-%m-%d %H:%M:%S")

    return (tracking_number, tracking_url, shipping_method, date_shipped)

def first(seq):
    return seq[0] if seq else None

# ------------------------ MAIN ------------------------
def main():
    logger = setup_logging()
    db = SQLiteSink()

    # Optional Google Sheets sink
    gs = None
    if ENABLE_GSHEETS:
        try:
            gs = GSheetsSink(
                sa_file=GSHEETS_SA_FILE,
                spreadsheet_id=GSHEETS_SPREADSHEET_ID,
                tab_name=GSHEETS_TAB,
                batch_size=GSHEETS_BATCH_SIZE,
            )
            logger.info("Google Sheets sink: enabled")
        except Exception as e:
            logger.error(f"Google Sheets sink init failed: {e}")
            gs = None

    logger.info("Starting job")
    try:
        common = od("xmlrpc/2/common")
        uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
    except Exception as e:
        logger.error(f"Auth error: {e}")
        log_all(db, gs, status="error", message=f"auth error: {e}")
        sys.exit(1)

    if not uid:
        logger.error("Auth failed (no UID)")
        log_all(db, gs, status="error", message="auth failed (no uid)")
        sys.exit(1)

    models = od("xmlrpc/2/object")

    # 1) Search pickings
    domain = [
        ("tpl_transfer_ids.milestone_state", "=", "Complete"),
        ("tpl_transfer_ids.state", "=", "sent"),
        ("tpl_shipment_ids", "=", False),
        ("company_id","in",COMPANY_IDS)
    ]
    try:
        picking_ids = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "stock.picking", "search",
            [domain],
            {"limit": MAX_PICKINGS}
        )
    except Exception as e:
        logger.error(f"Search error: {e}")
        log_all(db, gs, status="error", message=f"search error: {e}")
        sys.exit(1)

    if not picking_ids:
        logger.info("No pickings to process.")
        log_all(db, gs, status="skip", message="no pickings found")
        if gs:
            try: gs.flush()
            except: pass
        return

    # Read fields we need
    try:
        pickings = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "stock.picking", "read",
            [picking_ids],
            {"fields": ["name", "origin", "sale_id", "tpl_transfer_ids"]}
        )
    except Exception as e:
        logger.error(f"Read pickings error: {e}")
        log_all(db, gs, status="error", message=f"read pickings error: {e}")
        sys.exit(1)

    for p in pickings:
        picking_id = p["id"]
        sale_label = None
        if isinstance(p.get("sale_id"), list) and len(p["sale_id"]) == 2:
            sale_label = p["sale_id"][1]
        reference = sale_label or p.get("origin") or p.get("name")
        if not reference:
            msg = "Skip: no reference (sale/origin/name)."
            logger.warning(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, status="skip", message=msg)
            continue

        transfer_id = first(p.get("tpl_transfer_ids") or [])
        if not transfer_id:
            msg = "Skip: no tpl_transfer_ids."
            logger.warning(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, status="skip", message=msg)
            continue

        # Read queue
        try:
            queue = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "stock.tpl.queue", "read",
                [[transfer_id]], {"fields": ["tpl_provider", "state", "milestone_state"]}
            )[0]
        except Exception as e:
            msg = f"Read tpl.queue error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, status="error", message=msg)
            continue

        provider = queue.get("tpl_provider")
        if not (isinstance(provider, list) and len(provider) == 2):
            msg = f"Skip: missing tpl_provider on queue {transfer_id}."
            logger.warning(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, status="skip", message=msg)
            continue
        provider_id = provider[0]

        # Provider -> warehouse
        try:
            provider_rec = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "stock.tpl.provider", "read",
                [[provider_id]], {"fields": ["default_tpl_warehouse_id"]}
            )[0]
        except Exception as e:
            msg = f"Read tpl.provider error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, status="error", message=msg)
            continue

        wh = provider_rec.get("default_tpl_warehouse_id")
        if not (isinstance(wh, list) and len(wh) == 2):
            msg = f"Skip: missing default_tpl_warehouse_id on provider {provider_id}."
            logger.warning(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, status="skip", message=msg)
            continue
        wh_id = wh[0]

        try:
            wh_rec = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "stock.tpl.warehouse", "read",
                [[wh_id]], {"fields": ["region_code", "service_code"]}
            )[0]
        except Exception as e:
            msg = f"Read tpl.warehouse error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, status="error", message=msg)
            continue

        region = (wh_rec.get("region_code") or "").strip()
        service_type = (wh_rec.get("service_code") or "").strip()
        if not region or not service_type:
            msg = f"Skip: missing region/service (warehouse {wh_id})."
            logger.warning(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, status="skip", message=msg)
            continue

        # 2) Mainfreight
        try:
            tracking_number, tracking_url, shipping_method, date_shipped = mf_get_tracking(region, service_type, reference)
        except requests.HTTPError as e:
            msg = f"Mainfreight HTTP {e.response.status_code}: {e}"
            logger.error(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                    status="error", message=msg)
            continue
        except Exception as e:
            msg = f"Mainfreight error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                    status="error", message=msg)
            continue

        if not tracking_url or not tracking_number:
            msg = f"No tracking info for ref '{reference}' (region={region}, service={service_type})."
            logger.warning(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                    status="error", message=msg)
            continue

        # 3) Mark queue done
        try:
            models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "stock.tpl.queue", "write",
                [[transfer_id], {"state": "done"}]
            )
        except Exception as e:
            msg = f"Write tpl.queue error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                    status="error", message=msg)
            continue

        # Get shipped qty by product from move lines
        try:
            move_lines = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "stock.move.line", "search_read",
                [[("picking_id", "=", picking_id), ("qty_done", ">", 0)]],
                {"fields": ["product_id", "qty_done"], "limit": 10000}
            )
        except Exception as e:
            msg = f"Read move lines error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                    status="error", message=msg)
            continue

        qty_by_product = defaultdict(float)
        for ml in move_lines or []:
            prod = ml.get("product_id")
            pid = prod[0] if isinstance(prod, list) else (prod if isinstance(prod, int) else None)
            if pid:
                qty_by_product[pid] += float(ml.get("qty_done") or 0.0)

        if not qty_by_product:
            msg = "No qty_done > 0 — skip shipment creation."
            logger.warning(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                    status="skip", message=msg)
            continue

        # Build one2many commands
        lines_cmds = [
            (0, 0, {"product_id": pid, "qty": qty})
            for pid, qty in qty_by_product.items()
            if qty > 0
        ]

        # Create shipment + link to picking
        shipment_vals = {
            "name": tracking_number,
            "tracking_url": tracking_url,
            "tpl_code": shipping_method or "",
            "ship_date": date_shipped,           # already string "YYYY-MM-DD HH:MM:SS"
            "shipment_line_ids": lines_cmds,
        }

        try:
            shipment_id = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "tpl.shipment", "create", [shipment_vals]
            )
            models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "stock.picking", "write",
                [[picking_id], {"tpl_shipment_ids": [(4, shipment_id, 0)]}]
            )
        except Exception as e:
            msg = f"Create/link tpl.shipment error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            log_all(db, gs, picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                    status="error", message=msg, tracking_number=tracking_number,
                    tracking_url=tracking_url, shipping_method=shipping_method)
            continue

        logger.info(f"[{picking_id}] OK • tracking={tracking_number} • method={shipping_method} • linked shipment {shipment_id}")
        log_all(db, gs, picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                status="ok", message=f"linked shipment with {len(lines_cmds)} lines",
                tracking_number=tracking_number, tracking_url=tracking_url, shipping_method=shipping_method)

    logger.info("Done.")
    # Flush batched rows to Sheets if enabled
    if gs:
        try:
            gs.flush()
        except Exception as e:
            logger.warning(f"GSheets flush failed: {e}")

if __name__ == "__main__":
    main()