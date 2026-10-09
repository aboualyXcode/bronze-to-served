"""An independent implementation of Stagedoor's Silver, Gold and ML-feature logic.

The Spark pipeline must produce exactly what this module produces from the same landing files
(tests/spark/test_differential.py). It is written differently on purpose:

    * Silver is an event-by-event replay in plain Python, where the pipeline uses set-based Spark and
      Delta MERGE statements that run once per micro-batch;
    * Gold and the features are SQL over SQLite, where the pipeline uses PySpark DataFrames.

Two formulations that agree on every row are much stronger evidence than tests that restate one of them.
Money is held as integer cents in SQLite and as Decimal elsewhere, so comparisons are exact; timestamps
are UTC and rendered as 'YYYY-MM-DD HH:MM:SS.ffffff' strings so they sort and compare as text.
"""
from __future__ import annotations

import csv
import glob
import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

TS_FMT = "%Y-%m-%d %H:%M:%S.%f"
CENT = Decimal("0.01")

# ---------------------------------------------------------------- Spark-compatible parsing
# Bronze stores every raw value as a string, the way Spark's JSON reader renders it; Silver applies
# try_cast. These helpers reproduce both steps for the values the generator produces.


def spark_str(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return json.dumps(v)
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    return str(v)


def trim(v):
    s = spark_str(v)
    return None if s is None else s.strip(" ")


def try_int(v):
    s = trim(v)
    return int(s) if s is not None and re.fullmatch(r"[+-]?\d+", s) else None


def try_decimal(v, integer_digits=8):
    s = trim(v)
    if s is None or not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", s):
        return None
    try:
        d = Decimal(s).quantize(CENT, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None
    return d if abs(d) < Decimal(10) ** integer_digits else None


def try_ts(v):
    s = trim(v)
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def try_date(v):
    s = trim(v)
    try:
        return date.fromisoformat(s) if s else None
    except ValueError:
        return None


def try_bool(v):
    s = trim(v)
    if s is None:
        return None
    s = s.lower()
    if s in ("t", "true", "y", "yes", "1"):
        return True
    if s in ("f", "false", "n", "no", "0"):
        return False
    return None


def upper(v):
    s = trim(v)
    return None if s is None else s.upper()


def lower(v):
    s = trim(v)
    return None if s is None else s.lower()


def present(v) -> bool:
    return v is not None and v != ""


def fmt_ts(dt):
    return None if dt is None else dt.strftime(TS_FMT)


# ---------------------------------------------------------------- the contracts (rule names match the pipeline)

ORDER_STATUSES = ("PLACED", "PAID", "CANCELLED", "REFUNDED")
CDC_OPS = ("INSERT", "UPDATE", "DELETE")
EVENT_TYPES = ("page_view", "search", "view_event", "add_to_cart", "checkout", "purchase")


def parse_order(raw: dict) -> dict:
    items = raw.get("items")
    parsed_items = None
    if isinstance(items, list):
        parsed_items = [{"line_no": try_int(i.get("line_no")), "ticket_type": trim(i.get("ticket_type")),
                         "quantity": try_int(i.get("quantity")), "unit_price": try_decimal(i.get("unit_price"))}
                        for i in items]
    return {"order_id": trim(raw.get("order_id")), "customer_id": trim(raw.get("customer_id")),
            "event_id": trim(raw.get("event_id")), "order_ts": try_ts(raw.get("order_ts")),
            "updated_at": try_ts(raw.get("updated_at")), "status": upper(raw.get("status")),
            "channel": lower(raw.get("channel")), "currency": upper(raw.get("currency")),
            "promo_code": trim(raw.get("promo_code")), "items": parsed_items}


def order_failures(o: dict) -> list[str]:
    items = o["items"]
    checks = [
        ("order_id_present", present(o["order_id"])),
        ("customer_id_present", present(o["customer_id"])),
        ("event_id_present", present(o["event_id"])),
        ("valid_order_ts", o["order_ts"] is not None),
        ("valid_updated_at", o["updated_at"] is not None),
        ("known_status", o["status"] in ORDER_STATUSES),
        ("has_items", items is not None and len(items) > 0),
        ("valid_items", items is not None and all(
            i["line_no"] is not None and i["quantity"] is not None and i["quantity"] > 0
            and i["unit_price"] is not None and i["unit_price"] >= 0 for i in items)),
    ]
    return [name for name, ok in checks if not ok]


def parse_cdc(raw: dict) -> dict:
    return {"customer_id": trim(raw.get("customer_id")), "lsn": try_int(raw.get("lsn")), "op": upper(raw.get("op")),
            "changed_at": try_ts(raw.get("changed_at")), "email": trim(raw.get("email")),
            "full_name": trim(raw.get("full_name")), "country": upper(raw.get("country")),
            "city": trim(raw.get("city")), "loyalty_tier": lower(raw.get("loyalty_tier")),
            "marketing_opt_in": try_bool(raw.get("marketing_opt_in")), "signup_date": try_date(raw.get("signup_date"))}


def cdc_failures(c: dict) -> list[str]:
    checks = [("customer_id_present", present(c["customer_id"])), ("valid_lsn", c["lsn"] is not None),
              ("known_op", c["op"] in CDC_OPS), ("valid_changed_at", c["changed_at"] is not None)]
    return [name for name, ok in checks if not ok]


def parse_click(raw: dict) -> dict:
    return {"event_id": trim(raw.get("event_id")), "session_id": trim(raw.get("session_id")),
            "customer_id": trim(raw.get("customer_id")), "event_type": lower(raw.get("event_type")),
            "event_ref": trim(raw.get("event_ref")), "device": lower(raw.get("device")),
            "event_ts": try_ts(raw.get("event_ts")), "properties": spark_str(raw.get("properties"))}


def click_failures(e: dict) -> list[str]:
    checks = [("event_id_present", present(e["event_id"])), ("session_present", present(e["session_id"])),
              ("valid_event_ts", e["event_ts"] is not None), ("known_event_type", e["event_type"] in EVENT_TYPES)]
    return [name for name, ok in checks if not ok]


TRACKED = ("email", "full_name", "country", "city", "loyalty_tier", "marketing_opt_in", "signup_date")


# ---------------------------------------------------------------- Silver by replay

@dataclass
class Reference:
    orders: list[dict] = field(default_factory=list)
    order_items: list[dict] = field(default_factory=list)
    customers: list[dict] = field(default_factory=list)
    clickstream: list[dict] = field(default_factory=list)
    events: list[dict] = field(default_factory=list)
    venues: list[dict] = field(default_factory=list)
    quarantine: list[dict] = field(default_factory=list)   # {source, key, rules}
    gold: dict[str, list[dict]] = field(default_factory=dict)
    features: list[dict] = field(default_factory=list)
    labels: list[dict] = field(default_factory=list)
    data_as_of: date | None = None


def _jsonl(landing: str, source: str) -> list[dict]:
    rows = []
    for path in sorted(glob.glob(os.path.join(landing, source, f"{source}_*.json"))):
        with open(path, encoding="utf-8") as f:
            rows.extend(json.loads(line) for line in f if line.strip())
    return rows


def silver_orders(landing: str, ref: Reference) -> None:
    state: dict[str, dict] = {}
    lines: dict[tuple, dict] = {}
    for raw in _jsonl(landing, "orders"):
        o = parse_order(raw)
        failed = order_failures(o)
        if failed:
            ref.quarantine.append({"source": "orders", "key": o["order_id"], "rules": sorted(failed)})
            continue
        s = state.get(o["order_id"])
        if s is None or o["updated_at"] > s["updated_at"]:
            latest = {k: o[k] for k in ("customer_id", "event_id", "order_ts", "status", "channel", "currency",
                                        "promo_code", "updated_at")}
            latest["items"] = o["items"]
            s = {**(s or {}), **latest}
            state[o["order_id"]] = s
        for status, col in (("PAID", "paid_at"), ("CANCELLED", "cancelled_at"), ("REFUNDED", "refunded_at")):
            if o["status"] == status and (s.get(col) is None or o["updated_at"] < s[col]):
                s[col] = o["updated_at"]
        for item in o["items"]:      # order lines are immutable: the first copy wins
            lines.setdefault((o["order_id"], item["line_no"]), {"order_id": o["order_id"], **item})
    for order_id, s in sorted(state.items()):
        amount = sum((i["quantity"] * i["unit_price"] for i in s["items"]), Decimal("0")).quantize(CENT)
        ref.orders.append({
            "order_id": order_id, "customer_id": s["customer_id"], "event_id": s["event_id"],
            "order_ts": s["order_ts"], "order_date": s["order_ts"].date(), "status": s["status"],
            "channel": s["channel"], "currency": s["currency"], "promo_code": s["promo_code"],
            "order_amount": amount, "ticket_count": sum(i["quantity"] for i in s["items"]),
            "paid_at": s.get("paid_at"), "cancelled_at": s.get("cancelled_at"), "refunded_at": s.get("refunded_at"),
            "updated_at": s["updated_at"]})
    for (order_id, line_no), item in sorted(lines.items()):
        ref.order_items.append({**item, "line_amount": (item["quantity"] * item["unit_price"]).quantize(CENT)})


def silver_customers(landing: str, ref: Reference) -> None:
    valid: dict[tuple, dict] = {}
    for raw in _jsonl(landing, "customers_cdc"):
        c = parse_cdc(raw)
        failed = cdc_failures(c)
        if failed:
            ref.quarantine.append({"source": "customers_cdc", "key": c["customer_id"], "rules": sorted(failed)})
            continue
        valid.setdefault((c["customer_id"], c["lsn"]), c)          # exact re-deliveries collapse
    versions: dict[str, list[dict]] = {}
    for (customer_id, lsn), c in sorted(valid.items()):
        history = versions.setdefault(customer_id, [])
        current = history[-1] if history and history[-1]["valid_to"] is None else None
        if c["op"] == "DELETE":
            if current is not None:
                current.update(valid_to=c["changed_at"], is_current=False, _end_seq=lsn)
            continue
        attrs = tuple(c[k] for k in TRACKED)
        if current is not None and current["_attrs"] == attrs:
            continue                                                  # a no-op change is not a new version
        if current is not None:
            current.update(valid_to=c["changed_at"], is_current=False, _end_seq=lsn)
        history.append({"customer_sk": hashlib.sha256(f"{customer_id}|{lsn}".encode()).hexdigest(),
                        "customer_id": customer_id, **{k: c[k] for k in TRACKED},
                        "valid_from": c["changed_at"], "valid_to": None, "is_current": True,
                        "_start_seq": lsn, "_end_seq": None, "_attrs": attrs})
    for customer_id in sorted(versions):
        for v in versions[customer_id]:
            ref.customers.append({k: val for k, val in v.items() if k != "_attrs"})


def silver_clickstream(landing: str, ref: Reference) -> None:
    seen: dict[str, dict] = {}
    for raw in _jsonl(landing, "clickstream"):
        e = parse_click(raw)
        failed = click_failures(e)
        if failed:
            ref.quarantine.append({"source": "clickstream", "key": e["event_id"], "rules": sorted(failed)})
            continue
        seen.setdefault(e["event_id"], e)
    for event_id in sorted(seen):
        e = seen[event_id]
        ref.clickstream.append({**e, "event_date": e["event_ts"].date()})


def _latest_snapshot(landing: str, prefix: str) -> list[dict]:
    paths = sorted(glob.glob(os.path.join(landing, "reference", f"{prefix}_*.csv")))
    if not paths:
        return []
    with open(paths[-1], encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def silver_reference(landing: str, ref: Reference) -> None:
    for row in _latest_snapshot(landing, "venues"):
        cap = try_int(row["capacity"])
        if present(trim(row["venue_id"])) and cap is not None and cap > 0:
            ref.venues.append({"venue_id": trim(row["venue_id"]), "venue_name": trim(row["venue_name"]),
                               "city": trim(row["city"]), "country": upper(row["country"]), "capacity": cap})
    for row in _latest_snapshot(landing, "events"):
        price, day = try_decimal(row["base_price"]), try_date(row["event_date"])
        if present(trim(row["event_id"])) and present(trim(row["venue_id"])) and day and price is not None and price >= 0:
            ref.events.append({"event_id": trim(row["event_id"]), "artist": trim(row["artist"]),
                               "genre": trim(row["genre"]), "event_date": day, "venue_id": trim(row["venue_id"]),
                               "base_price": price})


# ---------------------------------------------------------------- Gold and features in SQLite

def _cents(d):
    return None if d is None else int((d * 100).to_integral_value())


def _load(db: sqlite3.Connection, ref: Reference) -> None:
    db.executescript("""
        CREATE TABLE orders (order_id TEXT PRIMARY KEY, customer_id TEXT, event_id TEXT, order_ts TEXT, order_date TEXT,
            status TEXT, channel TEXT, order_cents INTEGER, paid_at TEXT, refunded_at TEXT, updated_at TEXT);
        CREATE TABLE order_items (order_id TEXT, line_no INTEGER, ticket_type TEXT, quantity INTEGER, unit_cents INTEGER);
        CREATE TABLE customers (customer_sk TEXT, customer_id TEXT, country TEXT, loyalty_tier TEXT,
            marketing_opt_in INTEGER, signup_date TEXT, valid_from TEXT, valid_to TEXT, is_current INTEGER);
        CREATE TABLE clickstream (event_id TEXT, session_id TEXT, customer_id TEXT, event_type TEXT, device TEXT,
            event_ts TEXT, event_date TEXT);
        CREATE TABLE events (event_id TEXT, artist TEXT, genre TEXT, event_date TEXT, venue_id TEXT, base_cents INTEGER);
        CREATE TABLE venues (venue_id TEXT, venue_name TEXT, city TEXT, country TEXT, capacity INTEGER);
        CREATE INDEX ix_items ON order_items(order_id);
        CREATE INDEX ix_cust ON customers(customer_id);
        CREATE INDEX ix_click ON clickstream(customer_id, event_date);
        CREATE INDEX ix_orders_c ON orders(customer_id);
    """)
    db.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
        (o["order_id"], o["customer_id"], o["event_id"], fmt_ts(o["order_ts"]), o["order_date"].isoformat(),
         o["status"], o["channel"], _cents(o["order_amount"]), fmt_ts(o["paid_at"]), fmt_ts(o["refunded_at"]),
         fmt_ts(o["updated_at"])) for o in ref.orders])
    db.executemany("INSERT INTO order_items VALUES (?,?,?,?,?)", [
        (i["order_id"], i["line_no"], i["ticket_type"], i["quantity"], _cents(i["unit_price"])) for i in ref.order_items])
    db.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?,?,?)", [
        (c["customer_sk"], c["customer_id"], c["country"], c["loyalty_tier"],
         None if c["marketing_opt_in"] is None else int(c["marketing_opt_in"]),
         c["signup_date"].isoformat() if c["signup_date"] else None, fmt_ts(c["valid_from"]), fmt_ts(c["valid_to"]),
         int(c["is_current"])) for c in ref.customers])
    db.executemany("INSERT INTO clickstream VALUES (?,?,?,?,?,?,?)", [
        (e["event_id"], e["session_id"], e["customer_id"], e["event_type"], e["device"], fmt_ts(e["event_ts"]),
         e["event_date"].isoformat()) for e in ref.clickstream])
    db.executemany("INSERT INTO events VALUES (?,?,?,?,?,?)", [
        (e["event_id"], e["artist"], e["genre"], e["event_date"].isoformat(), e["venue_id"], _cents(e["base_price"]))
        for e in ref.events])
    db.executemany("INSERT INTO venues VALUES (?,?,?,?,?)", [
        (v["venue_id"], v["venue_name"], v["city"], v["country"], v["capacity"]) for v in ref.venues])


GOLD_SQL = {
    "dim_event": """
        SELECT e.event_id, e.artist, e.genre, e.event_date, e.venue_id, v.venue_name, v.city, v.country, v.capacity,
               e.base_cents
        FROM events e LEFT JOIN venues v ON v.venue_id = e.venue_id""",
    "fct_sales": """
        SELECT o.order_id, i.line_no, o.customer_id, c.customer_sk, o.event_id, o.channel, i.ticket_type, i.quantity,
               i.unit_cents, i.quantity * i.unit_cents AS gross_cents,
               CASE WHEN o.refunded_at IS NOT NULL THEN i.quantity * i.unit_cents ELSE 0 END AS refunded_cents,
               o.order_date, o.paid_at, date(o.paid_at) AS paid_date, o.refunded_at, date(o.refunded_at) AS refunded_date
        FROM orders o
        JOIN order_items i ON i.order_id = o.order_id
        LEFT JOIN customers c ON c.customer_id = o.customer_id AND c.valid_from <= o.paid_at
                             AND (c.valid_to IS NULL OR o.paid_at < c.valid_to)
        WHERE o.paid_at IS NOT NULL""",
    "agg_daily_sales": """
        WITH f AS (SELECT s.*, d.venue_id, d.genre FROM fct_sales s LEFT JOIN dim_event d ON d.event_id = s.event_id),
        paid AS (SELECT paid_date AS sales_date, venue_id, genre, COUNT(DISTINCT order_id) AS paid_orders,
                        SUM(quantity) AS tickets_sold, SUM(gross_cents) AS gross_cents
                 FROM f GROUP BY 1, 2, 3),
        refunds AS (SELECT refunded_date AS sales_date, venue_id, genre, COUNT(DISTINCT order_id) AS refunded_orders,
                           SUM(gross_cents) AS refunded_cents
                    FROM f WHERE refunded_date IS NOT NULL GROUP BY 1, 2, 3),
        k AS (SELECT sales_date, venue_id, genre FROM paid UNION SELECT sales_date, venue_id, genre FROM refunds)
        SELECT k.sales_date, k.venue_id, k.genre, COALESCE(p.paid_orders, 0) AS paid_orders,
               COALESCE(p.tickets_sold, 0) AS tickets_sold, COALESCE(p.gross_cents, 0) AS gross_cents,
               COALESCE(r.refunded_orders, 0) AS refunded_orders, COALESCE(r.refunded_cents, 0) AS refunded_cents,
               COALESCE(p.gross_cents, 0) - COALESCE(r.refunded_cents, 0) AS net_cents
        FROM k
        LEFT JOIN paid p ON p.sales_date = k.sales_date AND p.venue_id IS k.venue_id AND p.genre IS k.genre
        LEFT JOIN refunds r ON r.sales_date = k.sales_date AND r.venue_id IS k.venue_id AND r.genre IS k.genre""",
    "event_performance": """
        SELECT d.event_id, d.artist, d.genre, d.event_date, d.venue_id, d.capacity,
               COALESCE(SUM(CASE WHEN f.refunded_at IS NULL THEN f.quantity END), 0) AS tickets_sold,
               COALESCE(SUM(f.gross_cents), 0) AS gross_cents, COALESCE(SUM(f.refunded_cents), 0) AS refunded_cents,
               COALESCE(SUM(f.gross_cents), 0) - COALESCE(SUM(f.refunded_cents), 0) AS net_cents,
               CASE WHEN d.capacity > 0
                    THEN COALESCE(SUM(CASE WHEN f.refunded_at IS NULL THEN f.quantity END), 0) * 1.0 / d.capacity
               END AS sell_through,
               MIN(f.paid_at) AS first_sale_at, MAX(f.paid_at) AS last_sale_at
        FROM dim_event d LEFT JOIN fct_sales f ON f.event_id = d.event_id
        GROUP BY d.event_id, d.artist, d.genre, d.event_date, d.venue_id, d.capacity""",
    "customer_360": """
        WITH stats AS (
            SELECT customer_id, MIN(paid_at) AS first_purchase_at, MAX(paid_at) AS last_purchase_at,
                   COUNT(DISTINCT order_id) AS lifetime_orders,
                   COUNT(DISTINCT CASE WHEN refunded_at IS NOT NULL THEN order_id END) AS refunded_orders,
                   SUM(gross_cents) - SUM(refunded_cents) AS net_cents
            FROM fct_sales GROUP BY customer_id),
        genre_tickets AS (
            SELECT f.customer_id, d.genre, SUM(f.quantity) AS tickets
            FROM fct_sales f JOIN dim_event d ON d.event_id = f.event_id GROUP BY 1, 2),
        favorite AS (
            SELECT customer_id, genre FROM (
                SELECT customer_id, genre,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY tickets DESC, genre) AS rn
                FROM genre_tickets) WHERE rn = 1),
        clicks AS (
            SELECT customer_id,
                   SUM(CASE WHEN event_date > date(:as_of, '-30 days') AND event_date <= :as_of THEN 1 ELSE 0 END) AS events_30d,
                   MAX(event_ts) AS last_seen_at
            FROM clickstream WHERE customer_id IS NOT NULL GROUP BY customer_id)
        SELECT c.customer_id, c.country, c.loyalty_tier, c.marketing_opt_in, c.signup_date,
               s.first_purchase_at, s.last_purchase_at, COALESCE(s.lifetime_orders, 0) AS lifetime_orders,
               COALESCE(s.refunded_orders, 0) AS refunded_orders, COALESCE(s.net_cents, 0) AS net_cents,
               fav.genre AS favorite_genre, COALESCE(k.events_30d, 0) AS events_30d, k.last_seen_at,
               CAST(julianday(:as_of) - julianday(date(s.last_purchase_at)) AS INTEGER) AS days_since_last_purchase,
               :as_of AS as_of_date
        FROM customers c
        LEFT JOIN stats s ON s.customer_id = c.customer_id
        LEFT JOIN favorite fav ON fav.customer_id = c.customer_id
        LEFT JOIN clicks k ON k.customer_id = c.customer_id
        WHERE c.is_current = 1""",
    "funnel_daily": """
        WITH sessions AS (
            SELECT session_id, MIN(event_ts) AS started_at, MIN(device) AS device,
                   MAX(event_type = 'view_event') AS viewed, MAX(event_type = 'add_to_cart') AS carted,
                   MAX(event_type = 'checkout') AS checked_out, MAX(event_type = 'purchase') AS purchased
            FROM clickstream GROUP BY session_id)
        SELECT date(started_at) AS session_date, device, COUNT(*) AS sessions, SUM(viewed) AS viewed_sessions,
               SUM(carted) AS cart_sessions, SUM(checked_out) AS checkout_sessions, SUM(purchased) AS purchase_sessions,
               CASE WHEN SUM(viewed) > 0 THEN SUM(purchased) * 1.0 / SUM(viewed) END AS view_to_purchase_rate
        FROM sessions GROUP BY 1, 2""",
}

FEATURES_SQL = """
WITH dates AS (SELECT value AS as_of_date FROM json_each(:dates)),
paid AS (SELECT customer_id, order_id, date(paid_at) AS paid_date, date(refunded_at) AS refunded_date, order_cents
         FROM orders WHERE paid_at IS NOT NULL),
pop AS (SELECT DISTINCT d.as_of_date, p.customer_id FROM dates d
        JOIN paid p ON p.paid_date <= d.as_of_date AND p.paid_date > date(d.as_of_date, '-365 days')),
pop_v AS (SELECT pop.as_of_date, pop.customer_id, c.loyalty_tier, c.marketing_opt_in, c.country, c.signup_date
          FROM pop JOIN customers c ON c.customer_id = pop.customer_id
           AND c.valid_from <= date(pop.as_of_date, '+1 day') || ' 00:00:00.000000'
           AND (c.valid_to IS NULL OR c.valid_to > date(pop.as_of_date, '+1 day') || ' 00:00:00.000000')),
ord AS (SELECT v.as_of_date, v.customer_id,
           CAST(julianday(v.as_of_date) - julianday(MAX(CASE WHEN p.paid_date <= v.as_of_date THEN p.paid_date END)) AS INTEGER) AS recency_days,
           COUNT(DISTINCT CASE WHEN p.paid_date > date(v.as_of_date, '-30 days') AND p.paid_date <= v.as_of_date THEN p.order_id END) AS orders_30d,
           COUNT(DISTINCT CASE WHEN p.paid_date > date(v.as_of_date, '-90 days') AND p.paid_date <= v.as_of_date THEN p.order_id END) AS orders_90d,
           COUNT(DISTINCT CASE WHEN p.paid_date > date(v.as_of_date, '-365 days') AND p.paid_date <= v.as_of_date THEN p.order_id END) AS orders_365d,
           SUM(CASE WHEN p.paid_date > date(v.as_of_date, '-90 days') AND p.paid_date <= v.as_of_date THEN p.order_cents ELSE 0 END) AS revenue_90d_cents,
           SUM(CASE WHEN p.paid_date > date(v.as_of_date, '-365 days') AND p.paid_date <= v.as_of_date THEN p.order_cents ELSE 0 END) AS revenue_365d_cents,
           COUNT(DISTINCT CASE WHEN p.refunded_date > date(v.as_of_date, '-365 days') AND p.refunded_date <= v.as_of_date THEN p.order_id END) AS refunds_365d
        FROM pop_v v JOIN paid p ON p.customer_id = v.customer_id GROUP BY 1, 2),
lin AS (SELECT v.as_of_date, v.customer_id, SUM(f.gross_cents) AS gross_cents, SUM(f.quantity) AS tickets,
               COUNT(DISTINCT d.genre) AS genres
        FROM pop_v v JOIN fct_sales f ON f.customer_id = v.customer_id
             AND f.paid_date > date(v.as_of_date, '-365 days') AND f.paid_date <= v.as_of_date
        LEFT JOIN dim_event d ON d.event_id = f.event_id GROUP BY 1, 2),
clk AS (SELECT customer_id, event_date, COUNT(*) AS events, SUM(event_type = 'add_to_cart') AS carts
        FROM clickstream WHERE customer_id IS NOT NULL GROUP BY 1, 2),
eng AS (SELECT v.as_of_date, v.customer_id,
               SUM(CASE WHEN k.event_date > date(v.as_of_date, '-7 days') THEN k.events ELSE 0 END) AS events_7d,
               SUM(COALESCE(k.events, 0)) AS events_30d, SUM(COALESCE(k.carts, 0)) AS cart_adds_30d
        FROM pop_v v LEFT JOIN clk k ON k.customer_id = v.customer_id
             AND k.event_date > date(v.as_of_date, '-30 days') AND k.event_date <= v.as_of_date
        GROUP BY 1, 2)
SELECT v.customer_id, v.as_of_date, o.recency_days, o.orders_30d, o.orders_90d, o.orders_365d,
       o.revenue_90d_cents, o.revenue_365d_cents, o.refunds_365d,
       CASE WHEN l.tickets > 0 THEN l.gross_cents / 100.0 / l.tickets END AS avg_ticket_price_365d,
       l.genres AS genres_365d, e.events_7d, e.events_30d, e.cart_adds_30d,
       CASE WHEN e.events_30d > 0 THEN e.events_7d * 1.0 / e.events_30d END AS engagement_ratio,
       CAST(julianday(v.as_of_date) - julianday(v.signup_date) AS INTEGER) AS tenure_days,
       v.loyalty_tier, v.marketing_opt_in, v.country
FROM pop_v v
JOIN ord o ON o.as_of_date = v.as_of_date AND o.customer_id = v.customer_id
JOIN lin l ON l.as_of_date = v.as_of_date AND l.customer_id = v.customer_id
JOIN eng e ON e.as_of_date = v.as_of_date AND e.customer_id = v.customer_id
"""

LABELS_SQL = """
WITH dates AS (SELECT value AS as_of_date FROM json_each(:dates)),
paid AS (SELECT customer_id, date(paid_at) AS paid_date FROM orders WHERE paid_at IS NOT NULL)
SELECT f.customer_id, f.as_of_date,
       CASE WHEN EXISTS (SELECT 1 FROM paid p WHERE p.customer_id = f.customer_id AND p.paid_date > f.as_of_date
                         AND p.paid_date <= date(f.as_of_date, :horizon)) THEN 0 ELSE 1 END AS churned
FROM features f JOIN dates d ON d.as_of_date = f.as_of_date
WHERE date(f.as_of_date, :horizon) <= :data_as_of
"""

HORIZON_DAYS = 60
EVERY_DAYS = 14
WARMUP_DAYS = 90


def training_dates(first: date, data_as_of: date, every: int = EVERY_DAYS, warmup: int = WARMUP_DAYS,
                   horizon: int = HORIZON_DAYS) -> list[date]:
    """As-of dates whose label window (as_of, as_of + horizon] is complete."""
    out, d = [], first + timedelta(days=warmup)
    while d + timedelta(days=horizon) <= data_as_of:
        out.append(d)
        d += timedelta(days=every)
    return out


def _rows(db: sqlite3.Connection, sql: str, params: dict | None = None) -> list[dict]:
    cur = db.execute(sql, params or {})
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def build(landing: str, as_of_dates: list[date] | None = None) -> Reference:
    """Run the reference Silver -> Gold -> features over a landing folder."""
    ref = Reference()
    silver_orders(landing, ref)
    silver_customers(landing, ref)
    silver_clickstream(landing, ref)
    silver_reference(landing, ref)
    ref.data_as_of = max(o["updated_at"] for o in ref.orders).date()
    db = sqlite3.connect(":memory:")
    _load(db, ref)
    for name in ("dim_event", "fct_sales"):
        db.execute(f"CREATE TABLE {name} AS {GOLD_SQL[name]}")
    db.execute("CREATE INDEX ix_fct_c ON fct_sales(customer_id, paid_date)")
    as_of = {"as_of": ref.data_as_of.isoformat()}
    for name, sql in GOLD_SQL.items():
        ref.gold[name] = _rows(db, f"SELECT * FROM {name}" if name in ("dim_event", "fct_sales") else sql, as_of)
    first = min(o["order_date"] for o in ref.orders)
    dates = as_of_dates if as_of_dates is not None else training_dates(first, ref.data_as_of)
    params = {"dates": json.dumps([d.isoformat() for d in dates])}
    ref.features = _rows(db, FEATURES_SQL, params)
    db.execute("CREATE TABLE features (customer_id TEXT, as_of_date TEXT)")
    db.executemany("INSERT INTO features VALUES (?, ?)", [(f["customer_id"], f["as_of_date"]) for f in ref.features])
    ref.labels = _rows(db, LABELS_SQL, {**params, "horizon": f"+{HORIZON_DAYS} days",
                                        "data_as_of": ref.data_as_of.isoformat()})
    return ref
