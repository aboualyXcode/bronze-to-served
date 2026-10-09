"""Deterministic generator for Stagedoor's raw landing files.

Stagedoor is a fictional ticketing company. Its upstream systems drop one file per source per day into the
landing volume, and the files look like real exports: mostly right, sometimes wrong.

    orders/orders_YYYYMMDD.json                 order events (JSON lines): PLACED -> PAID -> REFUNDED, or CANCELLED
    customers_cdc/customers_cdc_YYYYMMDD.json   CRM change data capture: INSERT / UPDATE / DELETE with an LSN
    clickstream/clickstream_YYYYMMDD.json       web and app events, including anonymous sessions
    reference/events_YYYYMMDD.csv               full snapshot of the show calendar (weekly)
    reference/venues_YYYYMMDD.csv               full snapshot of venues (quarterly)

What makes it realistic (and worth a medallion architecture):
    * duplicates (at-least-once delivery), late arrivals (a record lands in the next day's file),
      invalid records (missing keys, bad timestamps, impossible quantities, unknown codes),
      unnormalized values (' de', 'Gold', 'paid'), timestamps in local time zones, schema drift (a new
      `seat_section` field appears halfway through), CDC events shuffled inside each file;
    * behaviour with signal: every customer has a latent purchase rate and a churn date; purchases and app
      engagement decline in the weeks before churn, refunds make churn likelier and loyalty tiers follow
      purchase counts, so the churn model has something real to learn.

Contracts the pipeline relies on (and the tests check):
    * order lines never change after placement, and an order's status timestamps strictly increase;
    * duplicates are exact copies; an (customer_id, lsn) pair is unique;
    * CDC events for one customer never arrive in a file earlier than an event with a lower LSN.

Everything is driven by seeded random streams, so a given seed always produces byte-identical files, and
writing days 0-9 then 10-19 produces exactly the files of writing days 0-19 at once.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import io
import json
import os
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable, Iterable

SOURCES = ("orders", "customers_cdc", "clickstream", "reference")

COUNTRIES = (
    ("DE", ("Berlin", "Hamburg", "Munich"), 1),
    ("PT", ("Lisbon", "Porto"), 0),
    ("JP", ("Osaka", "Tokyo"), 9),
    ("US", ("Austin", "Chicago", "Seattle"), -6),
    ("GB", ("London", "Leeds"), 0),
    ("ES", ("Madrid", "Seville"), 1),
    ("KE", ("Nairobi",), 3),
    ("CA", ("Toronto", "Vancouver"), -5),
)
GENRES = ("Rock", "Pop", "Jazz", "Electronic", "Indie", "Metal", "R&B", "Classical")
FIRST = ("Amara", "Diego", "Hana", "Layla", "Noah", "Priya", "Tomas", "Wanjiru", "Yuki", "Elena",
         "Sam", "Omar", "Lucia", "Kenji", "Zara", "Felix", "Ines", "Malik", "Sofia", "Arjun")
LAST = ("Okafor", "Ramos", "Sato", "Hassan", "Fischer", "Nair", "Silva", "Kamau", "Tanaka", "Ruiz",
        "Carter", "Haddad", "Moreno", "Ito", "Ali", "Weber", "Costa", "Owusu", "Rossi", "Mehta")
CHANNELS = ("web", "app", "partner", "box_office")
DEVICES = ("web", "ios", "android")
TIER_THRESHOLDS = ((15, "platinum"), (8, "gold"), (3, "silver"))
PROMOS = ("SPRING10", "FAN20", "STUDENT15")
VENUE_KINDS = ("Arena", "Hall", "Theatre", "Club", "Pavilion")
DOMAINS = ("example.com", "mail.example", "inbox.example")
SCHEMA_DRIFT_SHARE = 0.5          # `seat_section` appears in orders from this share of the timeline onwards
DAY = 86400


@dataclass(frozen=True)
class GeneratorSettings:
    seed: int = 20250101
    customers: int = 5000
    days: int = 400
    start: date = date(2025, 1, 1)
    venues: int = 40
    shows_per_week: int = 12
    existing_share: float = 0.6       # customers who already exist when the history starts
    clickstream_scale: float = 1.0    # multiplies browsing sessions (0.2 makes much smaller test data)
    messy: bool = True                # inject duplicates, late arrivals and invalid records

    def day_date(self, d: int) -> date:
        return self.start + timedelta(days=d)


@dataclass
class _Customer:
    idx: int
    customer_id: str
    first: str
    last: str
    email: str
    country: str
    city: str
    offset: int
    tier: str
    opt_in: bool
    signup_date: date
    active_from: int          # first day with activity
    rate: float               # purchases per month while engaged
    churn_day: float          # day the customer disengages (inf = never within the horizon)
    genre: str
    channel: str
    device: str
    paid_orders: int = 0
    deleted: bool = False
    last_cdc: datetime | None = None   # CDC timestamps strictly increase per customer


# ---------------------------------------------------------------- time and formatting helpers

def _utc(settings: GeneratorSettings, d: int, seconds: float) -> datetime:
    s = settings.start
    return datetime(s.year, s.month, s.day, tzinfo=timezone.utc) + timedelta(days=d, seconds=seconds)


def _day_of(settings: GeneratorSettings, ts: datetime) -> int:
    return (ts.astimezone(timezone.utc).date() - settings.start).days


def iso_utc(ts: datetime, millis: bool = False) -> str:
    ts = ts.astimezone(timezone.utc)
    if millis:
        return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_local(ts: datetime, offset_hours: int) -> str:
    return ts.astimezone(timezone(timedelta(hours=offset_hours))).isoformat(timespec="seconds")


def _money(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01")))


# ---------------------------------------------------------------- the simulation

class _Pending:
    """Records waiting for the file of a given day. `fresh` records still get messiness applied once."""

    def __init__(self) -> None:
        self.fresh: dict[int, dict[str, list]] = {}
        self.ready: dict[int, dict[str, list]] = {}

    def add(self, day: int, source: str, ts: datetime, record: dict, fresh: bool = True) -> None:
        bucket = self.fresh if fresh else self.ready
        bucket.setdefault(day, {}).setdefault(source, []).append((ts, record))

    def pop(self, day: int, source: str) -> tuple[list, list]:
        return (self.fresh.get(day, {}).pop(source, []), self.ready.get(day, {}).pop(source, []))

    def drop_day(self, day: int) -> None:
        self.fresh.pop(day, None)
        self.ready.pop(day, None)


class StagedoorSimulation:
    """Simulates Stagedoor day by day and hands each finished day's records to `emit`."""

    def __init__(self, settings: GeneratorSettings) -> None:
        self.s = settings
        seed = settings.seed
        # Independent streams, so changing one behaviour does not reshuffle every other one.
        self.r_world = random.Random(f"{seed}:world")
        self.r_buy = random.Random(f"{seed}:purchases")
        self.r_click = random.Random(f"{seed}:clickstream")
        self.r_crm = random.Random(f"{seed}:crm")
        self.r_mess = random.Random(f"{seed}:messiness")
        self.pending = _Pending()
        self.lsn = 1_000_000
        self.order_seq = 0
        self.session_seq = 0
        self.venues: list[dict] = []
        self.shows: list[dict] = []
        self.shows_by_genre: dict[str, list[tuple[int, int]]] = {g: [] for g in GENRES}
        self.customers: list[_Customer] = []
        self.signups: dict[int, list[_Customer]] = {}
        self._build_world()

    # ---------- world ----------
    def _build_world(self) -> None:
        r, s = self.r_world, self.s
        for i in range(1, s.venues + 1):
            cc, cities, _ = COUNTRIES[i % len(COUNTRIES)]
            city = cities[i % len(cities)]
            self.venues.append({"venue_id": f"V{i:02d}", "venue_name": f"{city} {VENUE_KINDS[i % 5]} {i}",
                                "city": city, "country": cc, "capacity": r.randint(8, 90) * 100})
        existing = int(s.customers * s.existing_share)
        for i in range(1, s.customers + 1):
            cc, cities, offset = r.choice(COUNTRIES)
            first, last = r.choice(FIRST), r.choice(LAST)
            if i <= existing:
                signup = s.start - timedelta(days=r.randint(1, 700))
                active_from = 0
            else:
                active_from = r.randint(1, s.days - 1)
                signup = s.day_date(active_from)
            rate = min(6.0, r.lognormvariate(-0.85, 0.75))
            loyal = r.random() < 0.3
            churn = float("inf") if loyal else active_from + 20 + r.expovariate(1 / 260)
            tier = "standard"
            if i <= existing:
                tier = "platinum" if rate > 2.2 else "gold" if rate > 1.2 else "silver" if rate > 0.6 else "standard"
            c = _Customer(idx=i, customer_id=f"C{i:06d}", first=first, last=last,
                          email=f"{first.lower()}.{last.lower()}{i}@{DOMAINS[0]}", country=cc,
                          city=r.choice(cities), offset=offset, tier=tier, opt_in=r.random() < 0.7,
                          signup_date=signup, active_from=active_from, rate=rate, churn_day=churn,
                          genre=r.choice(GENRES), channel=r.choice(CHANNELS[:3]), device=r.choice(DEVICES))
            self.customers.append(c)
            self.signups.setdefault(active_from, []).append(c)

    def _announce_shows(self, d: int, count: int, lo: int, hi: int) -> None:
        r = self.r_world
        for _ in range(count):
            n = len(self.shows) + 1
            event_day = d + r.randint(lo, hi)
            show = {"event_id": f"E{n:04d}", "artist": f"Artist {1 + (n * 7) % 150}", "genre": r.choice(GENRES),
                    "event_date": self.s.day_date(event_day).isoformat(), "event_day": event_day,
                    "venue_id": r.choice(self.venues)["venue_id"],
                    "base_price": Decimal(r.randint(50, 440)) / 2}
            self.shows.append(show)
            bisect.insort(self.shows_by_genre[show["genre"]], (event_day, n - 1))

    def _upcoming_show(self, d: int, genre: str, r: random.Random) -> dict | None:
        genres = [genre] if r.random() < 0.6 else list(GENRES)
        r.shuffle(genres)
        for g in genres + list(GENRES):
            idx = self.shows_by_genre[g]
            lo = bisect.bisect_right(idx, (d, 1 << 30))
            hi = bisect.bisect_right(idx, (d + 120, 1 << 30))
            if hi > lo:
                return self.shows[idx[r.randrange(lo, hi)][1]]
        return None

    # ---------- behaviour ----------
    @staticmethod
    def _decline(c: _Customer, d: int, ramp: int, floor: float, drop: float) -> float:
        if d >= c.churn_day:
            return floor
        start = c.churn_day - ramp
        if d >= start:
            return 1.0 - drop * (d - start) / ramp
        return 1.0

    def _cdc(self, c: _Customer, op: str, ts: datetime) -> None:
        # A change is generated in the order it happens, so its after-image is the customer's state now.
        if c.last_cdc is not None and ts <= c.last_cdc:
            ts = c.last_cdc + timedelta(seconds=1)
        c.last_cdc = ts
        rec = {"lsn": None, "op": op, "changed_at": iso_utc(ts), "customer_id": c.customer_id}
        if op != "DELETE":
            rec.update({"email": c.email, "full_name": f"{c.first} {c.last}", "country": c.country, "city": c.city,
                        "loyalty_tier": c.tier, "marketing_opt_in": c.opt_in,
                        "signup_date": c.signup_date.isoformat()})
        self.pending.add(_day_of(self.s, ts), "customers_cdc", ts, rec)

    def _order_record(self, c: _Customer, order: dict, status: str, ts: datetime) -> dict:
        rec = {"order_id": order["order_id"], "customer_id": c.customer_id, "event_id": order["event_id"],
               "order_ts": iso_local(order["placed"], c.offset), "status": status,
               "updated_at": iso_local(ts, c.offset), "channel": order["channel"], "currency": "EUR",
               "promo_code": order["promo"], "items": [dict(item) for item in order["items"]],
               "source_system": "ticketing-oltp"}
        if _day_of(self.s, ts) >= int(self.s.days * SCHEMA_DRIFT_SHARE):
            rec["seat_section"] = order["seat_section"]
        return rec

    def _session(self, c: _Customer | None, device: str, ts: datetime, steps: list[tuple[str, dict | None]]) -> None:
        self.session_seq += 1
        session_id = f"S{self.session_seq:08x}"
        t = ts
        for event_type, show in steps:
            props: dict = {"page": "/"}
            if event_type == "search":
                props = {"query": (show["genre"] if show else self.r_click.choice(GENRES)).lower()}
            elif event_type in ("view_event", "add_to_cart", "checkout", "purchase") and show:
                props = {"page": f"/events/{show['event_id']}"}
                if event_type == "add_to_cart":
                    props["quantity"] = self.r_click.randint(1, 4)
            rec = {"event_id": str(uuid.UUID(int=self.r_click.getrandbits(128), version=4)), "session_id": session_id,
                   "customer_id": c.customer_id if c else None, "event_type": event_type,
                   "event_ref": show["event_id"] if show else None, "device": device,
                   "event_ts": iso_utc(t, millis=True), "properties": props}
            self.pending.add(_day_of(self.s, t), "clickstream", t, rec)
            t = t + timedelta(seconds=self.r_click.randint(5, 240), milliseconds=self.r_click.randint(0, 999))

    def _purchase(self, c: _Customer, d: int) -> None:
        r = self.r_buy
        show = self._upcoming_show(d, c.genre, r)
        if show is None:
            return
        self.order_seq += 1
        placed = _utc(self.s, d, r.randint(6 * 3600, DAY - 1))
        base = show["base_price"]
        items = [{"line_no": 1, "ticket_type": "GA", "quantity": r.randint(1, 4), "unit_price": _money(base)}]
        if r.random() < 0.25:
            items.append({"line_no": 2, "ticket_type": "VIP", "quantity": r.randint(1, 2),
                          "unit_price": _money(base * Decimal("2.5"))})
        order = {"order_id": f"O{self.order_seq:08d}", "event_id": show["event_id"], "placed": placed,
                 "channel": c.channel if r.random() < 0.8 else r.choice(CHANNELS), "items": items,
                 "promo": r.choice(PROMOS) if r.random() < 0.08 else None,
                 "seat_section": f"{r.choice('ABCDEFGH')}{r.randint(1, 30)}"}
        self.pending.add(_day_of(self.s, placed), "orders", placed, self._order_record(c, order, "PLACED", placed))
        # The purchase session ends with the order being placed.
        start = placed - timedelta(seconds=r.randint(240, 1500))
        device = c.device if r.random() < 0.8 else r.choice(DEVICES)
        self._session(c, device, start, [("view_event", show), ("add_to_cart", show), ("checkout", show),
                                         ("purchase", show)])
        if r.random() < 0.06:
            cancelled = placed + timedelta(seconds=r.randint(600, 7200))
            self.pending.add(_day_of(self.s, cancelled), "orders", cancelled,
                             self._order_record(c, order, "CANCELLED", cancelled))
            return
        paid = placed + timedelta(seconds=r.randint(30, 1800))
        self.pending.add(_day_of(self.s, paid), "orders", paid, self._order_record(c, order, "PAID", paid))
        c.paid_orders += 1
        for threshold, tier in TIER_THRESHOLDS:
            if c.paid_orders >= threshold:
                if tier != c.tier and _tier_rank(tier) > _tier_rank(c.tier):
                    c.tier = tier
                    self._cdc(c, "UPDATE", paid + timedelta(seconds=r.randint(60, 2 * DAY)))
                break
        if r.random() < 0.035:
            refunded = paid + timedelta(days=r.randint(1, 25), seconds=r.randint(0, DAY - 1))
            self.pending.add(_day_of(self.s, refunded), "orders", refunded,
                             self._order_record(c, order, "REFUNDED", refunded))
            if r.random() < 0.35:   # a bad experience makes churn likelier
                c.churn_day = min(c.churn_day, _day_of(self.s, refunded) + r.expovariate(1 / 40))

    def _crm_changes(self, c: _Customer, d: int) -> None:
        r = self.r_crm
        x = r.random()
        if x < 0.0004:                               # moved city or changed email
            if r.random() < 0.5:
                cc, cities, _ = next(entry for entry in COUNTRIES if entry[0] == c.country)
                c.city = r.choice(cities)
            else:
                c.email = f"{c.first.lower()}.{c.last.lower()}{c.idx}@{r.choice(DOMAINS)}"
            self._cdc(c, "UPDATE", _utc(self.s, d, r.randint(0, DAY - 1)))
        elif x < 0.0006:                             # marketing preference toggled
            c.opt_in = not c.opt_in
            self._cdc(c, "UPDATE", _utc(self.s, d, r.randint(0, DAY - 1)))
        elif x < 0.0008:                             # CRM touched the record without changing it
            self._cdc(c, "UPDATE", _utc(self.s, d, r.randint(0, DAY - 1)))
        elif d > c.churn_day + 60 and x < 0.0013:    # long-gone customer asks to delete the account
            c.deleted = True       # no activity from now on; the DELETE follows any pending change
            self._cdc(c, "DELETE", _utc(self.s, d, r.randint(0, DAY - 1)))

    def _browse(self, c: _Customer | None, d: int) -> None:
        r = self.r_click
        show = self._upcoming_show(d, c.genre if c else r.choice(GENRES), r) if r.random() < 0.8 else None
        steps: list[tuple[str, dict | None]] = [("page_view", None)]
        if r.random() < 0.5:
            steps.append(("search", show))
        if show:
            steps.append(("view_event", show))
            if r.random() < 0.25:
                steps.append(("add_to_cart", show))
                if r.random() < 0.3:
                    steps.append(("checkout", show))
        device = (c.device if c and r.random() < 0.8 else r.choice(DEVICES))
        self._session(c, device, _utc(self.s, d, r.randint(0, DAY - 1)), steps)

    def _simulate_day(self, d: int) -> None:
        s = self.s
        if d == 0:
            self._announce_shows(0, s.shows_per_week * 22, 1, 154)
        elif s.day_date(d).weekday() == 0:
            self._announce_shows(d, s.shows_per_week, 30, 154)
        for c in self.signups.get(d, []):
            # Signups (and, on day 0, the initial CRM load) arrive in the early hours, before any activity.
            self._cdc(c, "INSERT", _utc(s, d, self.r_crm.randint(0, 6 * 3600)))
        weekend = s.day_date(d).weekday() in (4, 5)
        for c in self.customers:
            if c.active_from > d or c.deleted:
                continue
            buy = self._decline(c, d, ramp=45, floor=0.04, drop=0.65)
            if self.r_buy.random() < c.rate / 30 * buy * (1.25 if weekend else 0.93):
                self._purchase(c, d)
            engage = self._decline(c, d, ramp=60, floor=0.06, drop=0.75)
            if self.r_click.random() < (0.012 + 0.045 * c.rate) * engage * s.clickstream_scale:
                self._browse(c, d)
            if d > c.active_from:
                self._crm_changes(c, d)
        anonymous = int(len(self.customers) * 0.01 * s.clickstream_scale)
        for _ in range(anonymous):
            self._browse(None, d)

    # ---------- messiness ----------
    def _corrupt_order(self, rec: dict) -> dict:
        rec = json.loads(json.dumps(rec))
        kind = self.r_mess.randrange(6)
        if kind == 0:
            rec["customer_id"] = None
        elif kind == 1:
            rec["order_ts"] = self.r_mess.choice(["not-a-date", "31/12/2025 10:00", ""])
        elif kind == 2:
            rec["items"][0]["quantity"] = self.r_mess.choice([0, -1, "two"])
        elif kind == 3:
            rec["items"] = []
        elif kind == 4:
            rec["status"] = "PENDING_REVIEW"
        else:
            rec["items"][-1]["unit_price"] = "abc"
        return rec

    def _corrupt_click(self, rec: dict) -> dict:
        rec = dict(rec)
        kind = self.r_mess.randrange(4)
        if kind == 0:
            rec["event_type"] = "debug_ping"
        elif kind == 1:
            rec["event_id"] = None
        elif kind == 2:
            rec["event_ts"] = ""
        else:
            rec["session_id"] = None
        return rec

    def _corrupt_cdc(self, rec: dict) -> dict:
        rec = dict(rec)
        kind = self.r_mess.randrange(3)
        if kind == 0:
            rec["op"] = "MERGE"
        elif kind == 1:
            rec["lsn"] = "n/a"
        else:
            rec["customer_id"] = None
        return rec

    def _finish_stream(self, d: int, source: str, invalid: float, dup: float, late: float,
                       corrupt: Callable[[dict], dict], noise: Callable[[dict], dict] | None = None) -> list[dict]:
        fresh, ready = self.pending.pop(d, source)
        out = list(ready)
        r = self.r_mess
        for ts, rec in fresh:
            if self.s.messy:
                if noise is not None:
                    rec = noise(rec)
                if r.random() < invalid:
                    rec = corrupt(rec)
                if r.random() < dup:                  # an exact copy, in this file or the next one
                    if r.random() < 0.5:
                        out.append((ts, rec))
                    else:
                        self.pending.add(d + 1, source, ts, rec, fresh=False)
                if late and r.random() < late:
                    self.pending.add(d + 1, source, ts, rec, fresh=False)
                    continue
            out.append((ts, rec))
        out.sort(key=lambda item: item[0])
        return [rec for _, rec in out]

    def _order_noise(self, rec: dict) -> dict:
        if self.r_mess.random() < 0.01:
            rec = dict(rec, status=rec["status"].lower())
        if self.r_mess.random() < 0.002:
            rec = dict(rec, channel="kiosk")
        return rec

    def _click_noise(self, rec: dict) -> dict:
        if self.r_mess.random() < 0.003:
            rec = dict(rec, device="smart_tv")
        return rec

    def _finish_cdc(self, d: int) -> list[dict]:
        fresh, ready = self.pending.pop(d, "customers_cdc")
        fresh.sort(key=lambda item: (item[0], item[1]["customer_id"]))
        records = []
        r = self.r_mess
        for ts, rec in fresh:
            self.lsn += 1
            rec = dict(rec, lsn=self.lsn)
            if self.s.messy:
                if rec["op"] != "DELETE" and r.random() < 0.01:
                    rec["country"] = f" {rec['country'].lower()}"
                if rec["op"] != "DELETE" and r.random() < 0.01:
                    rec["loyalty_tier"] = rec["loyalty_tier"].capitalize()
                if r.random() < 0.0015:
                    rec = self._corrupt_cdc(rec)
                if r.random() < 0.005:                # re-delivered, in this file or the next one
                    if r.random() < 0.5:
                        records.append(dict(rec))
                    else:
                        self.pending.add(d + 1, "customers_cdc", ts, dict(rec), fresh=False)
            records.append(rec)
        records.extend(rec for _, rec in ready)
        if self.s.messy:
            r.shuffle(records)        # file order is not change order: the LSN is
        return records

    def finish_day(self, d: int) -> dict[str, list[dict]]:
        out = {
            "orders": self._finish_stream(d, "orders", 0.003, 0.004, 0.03, self._corrupt_order, self._order_noise),
            "customers_cdc": self._finish_cdc(d),
            "clickstream": self._finish_stream(d, "clickstream", 0.003, 0.01, 0.02, self._corrupt_click,
                                               self._click_noise),
        }
        self.pending.drop_day(d)
        return out

    def reference_files(self, d: int) -> dict[str, str]:
        files: dict[str, str] = {}
        stamp = self.s.day_date(d).strftime("%Y%m%d")
        if d == 0 or self.s.day_date(d).weekday() == 0:
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\n")
            w.writerow(["event_id", "artist", "genre", "event_date", "venue_id", "base_price"])
            for show in self.shows:
                w.writerow([show["event_id"], show["artist"], show["genre"], show["event_date"], show["venue_id"],
                            _money(show["base_price"])])
            files[f"events_{stamp}.csv"] = buf.getvalue()
        if d % 91 == 0:
            if d > 0:
                for venue in self.r_world.sample(self.venues, 2):
                    venue["capacity"] += self.r_world.choice([-200, 300, 500])
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\n")
            w.writerow(["venue_id", "venue_name", "city", "country", "capacity"])
            for v in self.venues:
                w.writerow([v["venue_id"], v["venue_name"], v["city"], v["country"], v["capacity"]])
            files[f"venues_{stamp}.csv"] = buf.getvalue()
        return files

    def run(self, last_day: int, emit: Callable[[int, dict[str, list[dict]], dict[str, str]], None]) -> None:
        """Simulate days 0..last_day (inclusive) and call emit(day, stream_records, reference_files)."""
        for d in range(0, min(last_day, self.s.days - 1) + 1):
            self._simulate_day(d)
            reference = self.reference_files(d)
            emit(d, self.finish_day(d), reference)


def _tier_rank(tier: str) -> int:
    return ("standard", "silver", "gold", "platinum").index(tier)


# ---------------------------------------------------------------- writing files

STATE_FILE = "_landing_state.json"


def landed_through(landing_root: str) -> int | None:
    """Last day index already written to `landing_root`, from its state file."""
    path = os.path.join(landing_root, STATE_FILE)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return int(json.load(f)["landed_through_day"])


def write_days(settings: GeneratorSettings, landing_root: str, first_day: int, last_day: int) -> list[str]:
    """Write the files of days first_day..last_day (inclusive) and return their paths.

    The whole timeline is always simulated from day 0, so the files of a day never depend on which days
    were written before. Works on local folders and on Unity Catalog volumes (/Volumes/... is a POSIX path).
    """
    if first_day < 0 or last_day < first_day:
        raise ValueError(f"invalid day range {first_day}..{last_day}")
    last_day = min(last_day, settings.days - 1)
    written: list[str] = []

    def emit(d: int, streams: dict[str, list[dict]], reference: dict[str, str]) -> None:
        if d < first_day:
            return
        stamp = settings.day_date(d).strftime("%Y%m%d")
        for source, records in streams.items():
            path = os.path.join(landing_root, source, f"{source}_{stamp}.json")
            _write_text(path, "".join(json.dumps(rec, separators=(",", ":"), ensure_ascii=False) + "\n"
                                      for rec in records))
            written.append(path)
        for name, text in reference.items():
            path = os.path.join(landing_root, "reference", name)
            _write_text(path, text)
            written.append(path)

    StagedoorSimulation(settings).run(last_day, emit)
    _write_text(os.path.join(landing_root, STATE_FILE),
                json.dumps({"landed_through_day": last_day,
                            "landed_through_date": settings.day_date(last_day).isoformat(),
                            "seed": settings.seed, "days": settings.days}, indent=2))
    return written


def _write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(text)
    os.replace(tmp, path)     # readers (Auto Loader) never see a half-written file


def read_jsonl(paths: Iterable[str]) -> list[dict]:
    rows = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            rows.extend(json.loads(line) for line in f if line.strip())
    return rows


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Write Stagedoor's raw landing files.")
    p.add_argument("--out", required=True, help="landing folder, e.g. .local/landing or /Volumes/c/s/raw")
    p.add_argument("--first-day", type=int, default=0)
    p.add_argument("--last-day", type=int, default=None, help="inclusive; default: the whole timeline")
    p.add_argument("--customers", type=int, default=GeneratorSettings.customers)
    p.add_argument("--days", type=int, default=GeneratorSettings.days)
    p.add_argument("--seed", type=int, default=GeneratorSettings.seed)
    p.add_argument("--clickstream-scale", type=float, default=1.0)
    a = p.parse_args(argv)
    settings = GeneratorSettings(seed=a.seed, customers=a.customers, days=a.days, clickstream_scale=a.clickstream_scale)
    last = settings.days - 1 if a.last_day is None else a.last_day
    paths = write_days(settings, a.out, a.first_day, last)
    print(f"wrote {len(paths)} files for days {a.first_day}..{min(last, settings.days - 1)} to {a.out}")


if __name__ == "__main__":
    main()
