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

import logging
from logging.handlers import RotatingFileHandler
import sqlite3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
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

# log
LOG_PATH = os.getenv("LOG_PATH", "tpl_tracking.log")
DB_PATH = os.getenv("DB_PATH", "tpl_tracking.db")

# ------------------------ LOG ------------------------
def setup_logging():
    logger = logging.getLogger("tpl_tracking")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # avoid duplicate logs if root handlers exist

    # Clean existing handlers if re-run in same process
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    # ISO-like timestamp per line
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    ch.setFormatter(formatter)

    # Rotating file
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
                  status TEXT,                -- 'ok' or 'error' or 'skip'
                  message TEXT,
                  tracking_number TEXT,
                  tracking_url TEXT,
                  shipping_method TEXT
                )
            """)
            cx.execute("CREATE INDEX IF NOT EXISTS idx_runs_main ON tracking_runs(picking_id, status, ts_utc)")
            cx.commit()

    def log(self, *, picking_id=None, reference=None, region=None, service_type=None,
            status:str, message:str="", tracking_number=None, tracking_url=None, shipping_method=None):
        with sqlite3.connect(self.path) as cx:
            cx.execute("""
                INSERT INTO tracking_runs
                  (ts_utc, picking_id, reference, region, service_type, status, message, tracking_number, tracking_url, shipping_method)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.utcnow().isoformat(timespec="seconds"),
                picking_id, reference, region, service_type, status, message,
                tracking_number, tracking_url, shipping_method
            ))
            cx.commit()

# ------------------------ HELPERS ------------------------
def od(name: str):
    return xmlrpc.client.ServerProxy(f"{ODOO_URL}/{name}")

def mf_get_tracking(region: str, service_type: str, reference: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Call Mainfreight Tracking/References, return (tracking_number, tracking_url, shipping_method).
    """
    headers = {"Authorization": f"Secret {MAINFREIGHT_API_KEY}", "Content-Type": "application/json"}
    params = {"serviceType": service_type, "reference": reference, "region": region}
    r = requests.get(MAINFREIGHT_BASE, params=params, headers=headers, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or not data:
        return (None, None, None)
    item = data[0]
    tracking_number = None
    tracking_url = None
    shipping_method = None
    # tracking URL: prefer top-level trackingUrl, else first carrierReferences[].trackingUrl
    cr = item.get("carrierReferences") or []
    if isinstance(cr, list) and cr:
        tracking_url = (cr[0] or {}).get("trackingUrl")
        shipping_method = (cr[0] or {}).get("carrierName")
        tracking_number = (cr[0] or {}).get("reference")
    
    return (tracking_number, tracking_url, shipping_method)

def first(seq):
    return seq[0] if seq else None

# ------------------------ MAIN ------------------------
def main():
    logger = setup_logging()
    db = SQLiteSink()

    logger.info("Starting job")
    try:
        common = od("xmlrpc/2/common")
        uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
    except Exception as e:
        logger.error(f"Auth error: {e}")
        db.log(status="error", message=f"auth error: {e}")
        sys.exit(1)

    if not uid:
        logger.error("Auth failed (no UID)")
        db.log(status="error", message="auth failed (no uid)")
        sys.exit(1)

    models = od("xmlrpc/2/object")

    # 1) Search pickings    
    domain = [
        ("tpl_transfer_ids.milestone_state", "=", "Complete"),
        ("tpl_transfer_ids.state", "=", "sent"),
        ("tpl_shipment_ids", "=", False),
        ("company_id","in",COMPANY_IDS)
    ]
    # domain for specific picking
    # domain = [
    #     ("tpl_transfer_ids.milestone_state", "=", "Complete"),
    #     ("tpl_transfer_ids.state", "=", "sent"),
    #     ("tpl_shipment_ids", "=", False),
    #     ("id","=", 218039)
    # ]    
    try:
        picking_ids = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "stock.picking", "search",
            [domain],
            {"limit": MAX_PICKINGS}
        )
    except Exception as e:
        logger.error(f"Search error: {e}")
        db.log(status="error", message=f"search error: {e}")
        sys.exit(1)

    if not picking_ids:
        logger.info("No pickings to process.")
        db.log(status="skip", message="no pickings found")
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
        db.log(status="error", message=f"read pickings error: {e}")
        sys.exit(1)

    for p in pickings:
        picking_id = p["id"]
        sale_label = None
        if isinstance(p.get("sale_id"), list) and len(p["sale_id"]) == 2:
            sale_label = p["sale_id"][1]  # e.g., "SO01234"
        reference = sale_label or p.get("origin") or p.get("name")
        if not reference:
            msg = "Skip: no reference (sale/origin/name)."
            logger.warning(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, status="skip", message=msg)
            continue

        # 1a) get ONE related transfer to resolve provider -> warehouse -> region/service
        transfer_id = first(p.get("tpl_transfer_ids") or [])
        if not transfer_id:
            msg = "Skip: no tpl_transfer_ids."
            logger.warning(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, status="skip", message=msg)
            continue

        # Read queue record to get tpl_provider
        try:
            queue = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "stock.tpl.queue", "read",
                [[transfer_id]], {"fields": ["tpl_provider", "state", "milestone_state"]}
            )[0]
        except Exception as e:
            msg = f"Read tpl.queue error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, status="error", message=msg)
            continue

        provider = queue.get("tpl_provider")
        if not (isinstance(provider, list) and len(provider) == 2):
            msg = f"Skip: missing tpl_provider on queue {transfer_id}."
            logger.warning(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, status="skip", message=msg)
            continue
        provider_id = provider[0]

        # Read provider -> warehouse
        try:
            provider_rec = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "stock.tpl.provider", "read",
                [[provider_id]], {"fields": ["default_tpl_warehouse_id"]}
            )[0]
        except Exception as e:
            msg = f"Read tpl.provider error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, status="error", message=msg)
            continue

        wh = provider_rec.get("default_tpl_warehouse_id")
        if not (isinstance(wh, list) and len(wh) == 2):
            msg = f"Skip: missing default_tpl_warehouse_id on provider {provider_id}."
            logger.warning(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, status="skip", message=msg)
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
            db.log(picking_id=picking_id, reference=reference, status="error", message=msg)
            continue

        region = (wh_rec.get("region_code") or "").strip()
        service_type = (wh_rec.get("service_code") or "").strip()
        if not region or not service_type:
            msg = f"Skip: missing region/service (warehouse {wh_id})."
            logger.warning(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, status="skip", message=msg)
            continue

        # 2) Call Mainfreight to get tracking info
        try:
            # tracking_number, tracking_url, shipping_method = "1234323142", "https://162504-14-0-all.runbot10.willdooit.net/", "UPS"
            
            # uncomment later the original code to get the tracking url from mainfreight
            tracking_number, tracking_url, shipping_method = mf_get_tracking(region, service_type, reference)
        except requests.HTTPError as e:
            msg = f"Mainfreight HTTP {e.response.status_code}: {e}"
            logger.error(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                   status="error", message=msg)
            continue
        except Exception as e:
            msg = f"Mainfreight error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                   status="error", message=msg)
            continue

        if not tracking_url or not tracking_number:
            msg = f"No tracking info for ref '{reference}' (region={region}, service={service_type})."
            logger.warning(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                   status="error", message=msg)
            continue

        # 3) Update tpl_transfer_ids.state = 'done'
        try:
            models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                "stock.tpl.queue", "write",
                [[transfer_id], {"state": "done"}]
            )
        except Exception as e:
            msg = f"Write tpl.queue error: {e}"
            logger.error(f"[{picking_id}] {msg}")
            db.log(picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                   status="error", message=msg)
            continue

        # Create tpl.shipment and link to picking (m2m)
        shipment_vals = {
            "name": tracking_number,
            "tracking_url": tracking_url,
            "tpl_code": shipping_method or "",
            "ship_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
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
            db.log(picking_id=picking_id, reference=reference, region=region, service_type=service_type,
                   status="error", message=msg, tracking_number=tracking_number,
                   tracking_url=tracking_url, shipping_method=shipping_method)
            continue

        logger.info(f"[{picking_id}] OK • tracking={tracking_number} • method={shipping_method} • linked shipment {shipment_id}")
        db.log(picking_id=picking_id, reference=reference, region=region, service_type=service_type,
               status="ok", message="linked shipment", tracking_number=tracking_number,
               tracking_url=tracking_url, shipping_method=shipping_method)

    logger.info("Done.")

if __name__ == "__main__":
    main()