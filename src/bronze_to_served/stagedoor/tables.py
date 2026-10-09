"""Every Stagedoor table's contract. `python -m bronze_to_served.stagedoor.tables` regenerates the data dictionary."""
from __future__ import annotations

import sys

from ..core.contracts import Column, TableSpec, data_dictionary
from . import sources


def C(name: str, type_: str, comment: str = "") -> Column:
    return Column(name, type_, comment)


def K(name: str, type_: str, comment: str = "") -> Column:
    return Column(name, type_, comment, nullable=False)


def _raw(names) -> tuple[Column, ...]:
    return tuple(C(n, "STRING", "Raw value as delivered.") for n in names)


INGESTION = (C("_rescued_data", "STRING", "Fields that did not fit the declared schema (Auto Loader rescue)."),
             C("_source_file", "STRING", "File (or Kafka topic/partition/offset) the record came from."),
             C("_file_modified_at", "TIMESTAMP", "When the source file was written."),
             C("_ingested_at", "TIMESTAMP", "When the record was ingested."))
DIM_CUSTOMER_COLS = (
    C("email", "STRING", "Email address (PII)."), C("full_name", "STRING", "Full name (PII)."),
    C("country", "STRING", "ISO country code, upper case."), C("city", "STRING", "City."),
    C("loyalty_tier", "STRING", "standard, silver, gold or platinum."),
    C("marketing_opt_in", "BOOLEAN", "Consents to marketing."), C("signup_date", "DATE", "Account creation date."))
SCD2_COLS = (C("valid_from", "TIMESTAMP", "Start of this version (the change time)."),
             C("valid_to", "TIMESTAMP", "End of this version; NULL while current."),
             C("is_current", "BOOLEAN", "True for the version in effect now."))
FEATURES = (
    C("recency_days", "INT", "Days since the last paid order (as of the date)."),
    C("orders_30d", "INT", "Paid orders in the 30 days up to the as-of date."), C("orders_90d", "INT", "Paid orders, 90 days."),
    C("orders_365d", "INT", "Paid orders, 365 days."), C("revenue_90d", "DECIMAL(12,2)", "Gross paid amount, 90 days."),
    C("revenue_365d", "DECIMAL(12,2)", "Gross paid amount, 365 days."),
    C("refunds_365d", "INT", "Orders refunded in the 365 days up to the as-of date."),
    C("avg_ticket_price_365d", "DOUBLE", "Average price per ticket bought, 365 days."),
    C("genres_365d", "INT", "Distinct genres bought, 365 days."), C("events_7d", "INT", "App and web events, 7 days."),
    C("events_30d", "INT", "App and web events, 30 days."), C("cart_adds_30d", "INT", "Add-to-cart events, 30 days."),
    C("engagement_ratio", "DOUBLE", "events_7d / events_30d: recent engagement versus the month."),
    C("tenure_days", "INT", "Days since signup."), C("loyalty_tier", "STRING", "Tier in effect on the as-of date (SCD2)."),
    C("marketing_opt_in", "BOOLEAN", "Opt-in in effect on the as-of date."), C("country", "STRING", "Country on the as-of date."))

ALL_TABLES: list[TableSpec] = [
    # ------------------------------------------------------------------ bronze
    TableSpec("bronze", "orders_raw", "Order events from the ticketing system, as delivered (JSON lines).",
              _raw(sources.ORDERS_RAW) + INGESTION),
    TableSpec("bronze", "customers_cdc_raw", "CRM change data capture events, as delivered.",
              _raw(sources.CUSTOMERS_CDC_RAW) + INGESTION),
    TableSpec("bronze", "clickstream_raw", "Web and app events, as delivered (files or Kafka).",
              _raw(sources.CLICKSTREAM_RAW) + INGESTION),
    TableSpec("bronze", "events_raw", "Show calendar snapshots (CSV), loaded with COPY INTO.",
              _raw(sources.EVENTS_RAW) + INGESTION[1:]),
    TableSpec("bronze", "venues_raw", "Venue snapshots (CSV), loaded with COPY INTO.",
              _raw(sources.VENUES_RAW) + INGESTION[1:]),
    # ------------------------------------------------------------------ silver
    TableSpec("silver", "orders", "One row per order: latest status and the time of each status change (SCD1).",
              (K("order_id", "STRING", "Order id."), C("customer_id", "STRING", "Customer who placed the order."),
               C("event_id", "STRING", "Show the tickets are for."), C("order_ts", "TIMESTAMP", "When the order was placed (UTC)."),
               C("order_date", "DATE", "UTC date of order_ts."), C("status", "STRING", "PLACED, PAID, CANCELLED or REFUNDED."),
               C("channel", "STRING", "Sales channel."), C("currency", "STRING", "ISO currency."),
               C("promo_code", "STRING", "Promotion used, if any."), C("order_amount", "DECIMAL(12,2)", "Sum of line amounts."),
               C("ticket_count", "INT", "Tickets in the order."), C("paid_at", "TIMESTAMP", "First time the order was PAID."),
               C("cancelled_at", "TIMESTAMP", "When it was cancelled."), C("refunded_at", "TIMESTAMP", "When it was refunded."),
               C("updated_at", "TIMESTAMP", "Time of the latest event applied."),
               C("_first_ingested_at", "TIMESTAMP", "First ingestion of any event of this order."),
               C("_last_ingested_at", "TIMESTAMP", "Latest ingestion of any event of this order.")),
              primary_key=("order_id",), cluster_by=("order_date",), change_data_feed=True,
              checks=(("valid_status", "status IN ('PLACED', 'PAID', 'CANCELLED', 'REFUNDED')"),)),
    TableSpec("silver", "order_items", "Order lines. Immutable after placement, inserted exactly once.",
              (K("order_id", "STRING", "Order id."), K("line_no", "INT", "Line number within the order."),
               C("ticket_type", "STRING", "GA or VIP."), C("quantity", "INT", "Tickets on the line."),
               C("unit_price", "DECIMAL(10,2)", "Price per ticket."), C("line_amount", "DECIMAL(12,2)", "quantity * unit_price.")),
              primary_key=("order_id", "line_no"), checks=(("positive_quantity", "quantity > 0"),)),
    TableSpec("silver", "customers", "Customer history from CRM CDC: one row per version (SCD Type 2).",
              (K("customer_sk", "STRING", "Surrogate key of this version: sha256(customer_id|lsn)."),
               C("customer_id", "STRING", "Business key."), *DIM_CUSTOMER_COLS, *SCD2_COLS,
               C("_start_seq", "BIGINT", "LSN of the change that created this version."),
               C("_end_seq", "BIGINT", "LSN of the change that closed it."),
               C("_row_hash", "STRING", "Hash of the tracked attributes, for change detection.")),
              primary_key=("customer_sk",), cluster_by=("customer_id",), change_data_feed=True),
    TableSpec("silver", "clickstream", "Validated, deduplicated web and app events (exactly once per event_id).",
              (K("event_id", "STRING", "Event id (UUID)."), C("session_id", "STRING", "Session."),
               C("customer_id", "STRING", "Customer, NULL for anonymous sessions."), C("event_type", "STRING", "Event type."),
               C("event_ref", "STRING", "Show the event is about, if any."), C("device", "STRING", "web, ios or android."),
               C("event_ts", "TIMESTAMP", "When it happened (UTC)."), C("event_date", "DATE", "UTC date of event_ts."),
               C("properties", "STRING", "Event properties as JSON."), C("_ingested_at", "TIMESTAMP", "Bronze ingestion time.")),
              primary_key=("event_id",), cluster_by=("event_date", "customer_id")),
    TableSpec("silver", "events", "Shows, from the latest calendar snapshot.",
              (K("event_id", "STRING", "Show id."), C("artist", "STRING", "Artist."), C("genre", "STRING", "Genre."),
               C("event_date", "DATE", "Date of the show."), C("venue_id", "STRING", "Venue."),
               C("base_price", "DECIMAL(10,2)", "GA ticket price."), C("_snapshot_date", "DATE", "Snapshot the row came from.")),
              primary_key=("event_id",)),
    TableSpec("silver", "venues", "Venues, from the latest snapshot.",
              (K("venue_id", "STRING", "Venue id."), C("venue_name", "STRING", "Name."), C("city", "STRING", "City."),
               C("country", "STRING", "ISO country code."), C("capacity", "INT", "Seats."),
               C("_snapshot_date", "DATE", "Snapshot the row came from.")),
              primary_key=("venue_id",)),
    # ------------------------------------------------------------------ gold
    TableSpec("gold", "dim_customer", "Customer dimension with full history (SCD2): join facts on customer_sk.",
              (K("customer_sk", "STRING", "Surrogate key."), C("customer_id", "STRING", "Business key."),
               *DIM_CUSTOMER_COLS, *SCD2_COLS), primary_key=("customer_sk",)),
    TableSpec("gold", "dim_event", "Show dimension, with its venue.",
              (K("event_id", "STRING", "Show id."), C("artist", "STRING", "Artist."), C("genre", "STRING", "Genre."),
               C("event_date", "DATE", "Show date."), C("venue_id", "STRING", "Venue id."), C("venue_name", "STRING", "Venue."),
               C("city", "STRING", "City."), C("country", "STRING", "Country."), C("capacity", "INT", "Venue capacity."),
               C("base_price", "DECIMAL(10,2)", "GA price.")), primary_key=("event_id",)),
    TableSpec("gold", "fct_sales", "Sales fact at order-line grain for paid orders, with the customer version at payment time.",
              (K("order_id", "STRING", "Order id."), K("line_no", "INT", "Line number."), C("customer_id", "STRING", "Customer."),
               C("customer_sk", "STRING", "dim_customer version in effect when the order was paid."),
               C("event_id", "STRING", "Show."), C("channel", "STRING", "Sales channel."), C("ticket_type", "STRING", "GA or VIP."),
               C("quantity", "INT", "Tickets."), C("unit_price", "DECIMAL(10,2)", "Price per ticket."),
               C("gross_amount", "DECIMAL(12,2)", "quantity * unit_price."),
               C("refunded_amount", "DECIMAL(12,2)", "gross_amount if the order was refunded, else 0."),
               C("order_date", "DATE", "Order date."), C("paid_at", "TIMESTAMP", "Payment time."), C("paid_date", "DATE", "Payment date."),
               C("refunded_at", "TIMESTAMP", "Refund time."), C("refunded_date", "DATE", "Refund date.")),
              primary_key=("order_id", "line_no"), cluster_by=("paid_date",)),
    TableSpec("gold", "agg_daily_sales", "Daily sales by venue and genre: revenue booked on payment date, refunds on refund date.",
              (C("sales_date", "DATE", "Day."), C("venue_id", "STRING", "Venue."), C("genre", "STRING", "Genre."),
               C("paid_orders", "BIGINT", "Orders paid that day."), C("tickets_sold", "BIGINT", "Tickets paid that day."),
               C("gross_revenue", "DECIMAL(18,2)", "Amount paid that day."), C("refunded_orders", "BIGINT", "Orders refunded that day."),
               C("refunded_amount", "DECIMAL(18,2)", "Amount refunded that day."),
               C("net_revenue", "DECIMAL(18,2)", "gross_revenue - refunded_amount.")), cluster_by=("sales_date",)),
    TableSpec("gold", "event_performance", "One row per show: tickets sold (net of refunds), revenue and sell-through.",
              (K("event_id", "STRING", "Show."), C("artist", "STRING", "Artist."), C("genre", "STRING", "Genre."),
               C("event_date", "DATE", "Show date."), C("venue_id", "STRING", "Venue."), C("capacity", "INT", "Capacity."),
               C("tickets_sold", "BIGINT", "Tickets on paid, not refunded orders."), C("gross_revenue", "DECIMAL(18,2)", "Gross."),
               C("refunded_amount", "DECIMAL(18,2)", "Refunded."), C("net_revenue", "DECIMAL(18,2)", "Net."),
               C("sell_through", "DOUBLE", "tickets_sold / capacity."), C("first_sale_at", "TIMESTAMP", "First payment."),
               C("last_sale_at", "TIMESTAMP", "Latest payment.")), primary_key=("event_id",)),
    TableSpec("gold", "customer_360", "One row per current customer: profile, purchases, favourite genre and engagement.",
              (K("customer_id", "STRING", "Customer."), *DIM_CUSTOMER_COLS,
               C("first_purchase_at", "TIMESTAMP", "First payment."), C("last_purchase_at", "TIMESTAMP", "Latest payment."),
               C("lifetime_orders", "BIGINT", "Paid orders."), C("refunded_orders", "BIGINT", "Refunded orders."),
               C("lifetime_net_revenue", "DECIMAL(18,2)", "Paid minus refunded."),
               C("favorite_genre", "STRING", "Genre with the most tickets (ties: alphabetical)."),
               C("events_30d", "BIGINT", "App and web events in the 30 days to as_of_date."),
               C("last_seen_at", "TIMESTAMP", "Latest app or web event."),
               C("days_since_last_purchase", "INT", "as_of_date - date of last purchase."),
               C("as_of_date", "DATE", "Latest data date (not the wall clock), so rebuilds are reproducible.")),
              primary_key=("customer_id",)),
    TableSpec("gold", "funnel_daily", "Conversion funnel by session start date and device.",
              (C("session_date", "DATE", "Date the session started."), C("device", "STRING", "Device."),
               C("sessions", "BIGINT", "Sessions."), C("viewed_sessions", "BIGINT", "Sessions that viewed a show."),
               C("cart_sessions", "BIGINT", "...that added to cart."), C("checkout_sessions", "BIGINT", "...that reached checkout."),
               C("purchase_sessions", "BIGINT", "...that purchased."),
               C("view_to_purchase_rate", "DOUBLE", "purchase_sessions / viewed_sessions.")), cluster_by=("session_date",)),
    TableSpec("gold", "live_engagement", "Near-real-time event counts in 5-minute windows (stateful streaming).",
              (C("window_start", "TIMESTAMP", "Window start."), C("window_end", "TIMESTAMP", "Window end."),
               C("event_type", "STRING", "Event type."), C("events", "BIGINT", "Events in the window."),
               C("sessions", "BIGINT", "Approximate distinct sessions.")), cluster_by=("window_start",)),
    # ------------------------------------------------------------------ ml
    TableSpec("ml", "customer_features", "Churn features per customer and as-of date, computed only from data up to that date.",
              (K("customer_id", "STRING", "Customer."), K("as_of_date", "DATE", "Features describe the customer at the end of this day."),
               *FEATURES), primary_key=("customer_id", "as_of_date"), timeseries_key="as_of_date", cluster_by=("as_of_date",)),
    TableSpec("ml", "churn_labels", "1 if the customer made no paid order in the 60 days after as_of_date.",
              (K("customer_id", "STRING", "Customer."), K("as_of_date", "DATE", "As-of date."), C("churned", "INT", "Label.")),
              primary_key=("customer_id", "as_of_date")),
    TableSpec("ml", "churn_training_set", "Features joined to labels, split by time (the latest as-of dates are the test set).",
              (C("customer_id", "STRING", "Customer."), C("as_of_date", "DATE", "As-of date."), *FEATURES,
               C("churned", "INT", "Label."), C("split", "STRING", "train or test."))),
    TableSpec("ml", "model_evaluations", "Every evaluation of a challenger and the champion, with the promotion decision.",
              (C("run_id", "STRING", "Job run."), C("model_name", "STRING", "Unity Catalog model."), C("model_version", "STRING", "Version."),
               C("role", "STRING", "challenger or champion."), C("test_rows", "BIGINT", "Rows scored."),
               C("roc_auc", "DOUBLE", "ROC AUC."), C("pr_auc", "DOUBLE", "Average precision."), C("log_loss", "DOUBLE", "Log loss."),
               C("brier", "DOUBLE", "Brier score."), C("top_decile_lift", "DOUBLE", "Churn rate in the top 10% / base rate."),
               C("promote", "BOOLEAN", "Decision (challenger rows)."), C("reasons", "ARRAY<STRING>", "Why."),
               C("evaluated_at", "TIMESTAMP", "When."))),
    TableSpec("ml", "churn_predictions", "Churn scores from the champion model, one row per customer and as-of date.",
              (K("customer_id", "STRING", "Customer."), K("as_of_date", "DATE", "Features date."),
               C("churn_probability", "DOUBLE", "Probability of no purchase in the next 60 days."),
               C("risk_band", "STRING", "high (>= 0.6), medium (>= 0.3) or low."), C("model_name", "STRING", "Model."),
               C("model_version", "STRING", "Version that scored."), C("scored_at", "TIMESTAMP", "When.")),
              primary_key=("customer_id", "as_of_date"), change_data_feed=True),
    TableSpec("ml", "feature_drift", "Population stability index of each feature: latest scoring set versus training.",
              (C("as_of_date", "DATE", "Scoring date."), C("feature", "STRING", "Feature."), C("psi", "DOUBLE", "PSI."),
               C("status", "STRING", "stable, moderate or significant."), C("computed_at", "TIMESTAMP", "When."))),
    # ------------------------------------------------------------------ ops
    TableSpec("ops", "quarantine", "Rows that broke a drop rule, with the raw record, for triage and replay.",
              (C("source_table", "STRING", "Table the row was headed for."), C("record_key", "STRING", "Business key, if any."),
               C("failed_rules", "ARRAY<STRING>", "Rules broken."), C("payload", "STRING", "The raw record as JSON."),
               C("source_file", "STRING", "Source file."), C("run_id", "STRING", "Job run."),
               C("quarantined_at", "TIMESTAMP", "When.")), cluster_by=("source_table",)),
    TableSpec("ops", "quality_metrics", "Rows checked and rows failing each rule, per batch.",
              (C("run_id", "STRING", "Job run."), C("source_table", "STRING", "Table."), C("rule", "STRING", "Rule."),
               C("action", "STRING", "drop, warn or fail."), C("failed_rows", "BIGINT", "Failing rows."),
               C("total_rows", "BIGINT", "Rows checked."), C("measured_at", "TIMESTAMP", "When."))),
    TableSpec("ops", "task_runs", "One row per task run: status, duration and metrics.",
              (C("run_id", "STRING", "Job run."), C("task", "STRING", "Component id."), C("status", "STRING", "SUCCEEDED or FAILED."),
               C("started_at", "TIMESTAMP", "Start."), C("finished_at", "TIMESTAMP", "End."), C("metrics", "STRING", "JSON."),
               C("error", "STRING", "Error, if failed."))),
    TableSpec("ops", "export_bookmarks", "Last change-data-feed version each export has sent.",
              (K("export_name", "STRING", "Export."), C("table_name", "STRING", "Source table."),
               C("last_version", "BIGINT", "Last Delta version exported."), C("updated_at", "TIMESTAMP", "When.")),
              primary_key=("export_name",)),
]

_BY_REF = {s.ref: s for s in ALL_TABLES}


def spec(ref: str) -> TableSpec:
    return _BY_REF[ref]


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "docs/data-dictionary.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(data_dictionary(ALL_TABLES) + "\n")
    print(f"wrote {out}")
