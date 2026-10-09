# Data dictionary

Generated from `src/bronze_to_served/stagedoor/tables.py` by `python -m bronze_to_served.stagedoor.tables`. Do not edit by hand.

## Bronze

### `bronze.orders_raw`

Order events from the ticketing system, as delivered (JSON lines).

| Column | Type | Description |
|---|---|---|
| `order_id` | `STRING` | Raw value as delivered. |
| `customer_id` | `STRING` | Raw value as delivered. |
| `event_id` | `STRING` | Raw value as delivered. |
| `order_ts` | `STRING` | Raw value as delivered. |
| `status` | `STRING` | Raw value as delivered. |
| `updated_at` | `STRING` | Raw value as delivered. |
| `channel` | `STRING` | Raw value as delivered. |
| `currency` | `STRING` | Raw value as delivered. |
| `promo_code` | `STRING` | Raw value as delivered. |
| `items` | `STRING` | Raw value as delivered. |
| `source_system` | `STRING` | Raw value as delivered. |
| `_rescued_data` | `STRING` | Fields that did not fit the declared schema (Auto Loader rescue). |
| `_source_file` | `STRING` | File (or Kafka topic/partition/offset) the record came from. |
| `_file_modified_at` | `TIMESTAMP` | When the source file was written. |
| `_ingested_at` | `TIMESTAMP` | When the record was ingested. |

### `bronze.customers_cdc_raw`

CRM change data capture events, as delivered.

| Column | Type | Description |
|---|---|---|
| `lsn` | `STRING` | Raw value as delivered. |
| `op` | `STRING` | Raw value as delivered. |
| `changed_at` | `STRING` | Raw value as delivered. |
| `customer_id` | `STRING` | Raw value as delivered. |
| `email` | `STRING` | Raw value as delivered. |
| `full_name` | `STRING` | Raw value as delivered. |
| `country` | `STRING` | Raw value as delivered. |
| `city` | `STRING` | Raw value as delivered. |
| `loyalty_tier` | `STRING` | Raw value as delivered. |
| `marketing_opt_in` | `STRING` | Raw value as delivered. |
| `signup_date` | `STRING` | Raw value as delivered. |
| `_rescued_data` | `STRING` | Fields that did not fit the declared schema (Auto Loader rescue). |
| `_source_file` | `STRING` | File (or Kafka topic/partition/offset) the record came from. |
| `_file_modified_at` | `TIMESTAMP` | When the source file was written. |
| `_ingested_at` | `TIMESTAMP` | When the record was ingested. |

### `bronze.clickstream_raw`

Web and app events, as delivered (files or Kafka).

| Column | Type | Description |
|---|---|---|
| `event_id` | `STRING` | Raw value as delivered. |
| `session_id` | `STRING` | Raw value as delivered. |
| `customer_id` | `STRING` | Raw value as delivered. |
| `event_type` | `STRING` | Raw value as delivered. |
| `event_ref` | `STRING` | Raw value as delivered. |
| `device` | `STRING` | Raw value as delivered. |
| `event_ts` | `STRING` | Raw value as delivered. |
| `properties` | `STRING` | Raw value as delivered. |
| `_rescued_data` | `STRING` | Fields that did not fit the declared schema (Auto Loader rescue). |
| `_source_file` | `STRING` | File (or Kafka topic/partition/offset) the record came from. |
| `_file_modified_at` | `TIMESTAMP` | When the source file was written. |
| `_ingested_at` | `TIMESTAMP` | When the record was ingested. |

### `bronze.events_raw`

Show calendar snapshots (CSV), loaded with COPY INTO.

| Column | Type | Description |
|---|---|---|
| `event_id` | `STRING` | Raw value as delivered. |
| `artist` | `STRING` | Raw value as delivered. |
| `genre` | `STRING` | Raw value as delivered. |
| `event_date` | `STRING` | Raw value as delivered. |
| `venue_id` | `STRING` | Raw value as delivered. |
| `base_price` | `STRING` | Raw value as delivered. |
| `_source_file` | `STRING` | File (or Kafka topic/partition/offset) the record came from. |
| `_file_modified_at` | `TIMESTAMP` | When the source file was written. |
| `_ingested_at` | `TIMESTAMP` | When the record was ingested. |

### `bronze.venues_raw`

Venue snapshots (CSV), loaded with COPY INTO.

| Column | Type | Description |
|---|---|---|
| `venue_id` | `STRING` | Raw value as delivered. |
| `venue_name` | `STRING` | Raw value as delivered. |
| `city` | `STRING` | Raw value as delivered. |
| `country` | `STRING` | Raw value as delivered. |
| `capacity` | `STRING` | Raw value as delivered. |
| `_source_file` | `STRING` | File (or Kafka topic/partition/offset) the record came from. |
| `_file_modified_at` | `TIMESTAMP` | When the source file was written. |
| `_ingested_at` | `TIMESTAMP` | When the record was ingested. |

## Silver

### `silver.orders`

One row per order: latest status and the time of each status change (SCD1). (primary key `order_id`; clustered by `order_date`; change data feed on.)

| Column | Type | Description |
|---|---|---|
| `order_id` | `STRING` NOT NULL | Order id. |
| `customer_id` | `STRING` | Customer who placed the order. |
| `event_id` | `STRING` | Show the tickets are for. |
| `order_ts` | `TIMESTAMP` | When the order was placed (UTC). |
| `order_date` | `DATE` | UTC date of order_ts. |
| `status` | `STRING` | PLACED, PAID, CANCELLED or REFUNDED. |
| `channel` | `STRING` | Sales channel. |
| `currency` | `STRING` | ISO currency. |
| `promo_code` | `STRING` | Promotion used, if any. |
| `order_amount` | `DECIMAL(12,2)` | Sum of line amounts. |
| `ticket_count` | `INT` | Tickets in the order. |
| `paid_at` | `TIMESTAMP` | First time the order was PAID. |
| `cancelled_at` | `TIMESTAMP` | When it was cancelled. |
| `refunded_at` | `TIMESTAMP` | When it was refunded. |
| `updated_at` | `TIMESTAMP` | Time of the latest event applied. |
| `_first_ingested_at` | `TIMESTAMP` | First ingestion of any event of this order. |
| `_last_ingested_at` | `TIMESTAMP` | Latest ingestion of any event of this order. |

### `silver.order_items`

Order lines. Immutable after placement, inserted exactly once. (primary key `order_id`, `line_no`.)

| Column | Type | Description |
|---|---|---|
| `order_id` | `STRING` NOT NULL | Order id. |
| `line_no` | `INT` NOT NULL | Line number within the order. |
| `ticket_type` | `STRING` | GA or VIP. |
| `quantity` | `INT` | Tickets on the line. |
| `unit_price` | `DECIMAL(10,2)` | Price per ticket. |
| `line_amount` | `DECIMAL(12,2)` | quantity * unit_price. |

### `silver.customers`

Customer history from CRM CDC: one row per version (SCD Type 2). (primary key `customer_sk`; clustered by `customer_id`; change data feed on.)

| Column | Type | Description |
|---|---|---|
| `customer_sk` | `STRING` NOT NULL | Surrogate key of this version: sha256(customer_id\|lsn). |
| `customer_id` | `STRING` | Business key. |
| `email` | `STRING` | Email address (PII). |
| `full_name` | `STRING` | Full name (PII). |
| `country` | `STRING` | ISO country code, upper case. |
| `city` | `STRING` | City. |
| `loyalty_tier` | `STRING` | standard, silver, gold or platinum. |
| `marketing_opt_in` | `BOOLEAN` | Consents to marketing. |
| `signup_date` | `DATE` | Account creation date. |
| `valid_from` | `TIMESTAMP` | Start of this version (the change time). |
| `valid_to` | `TIMESTAMP` | End of this version; NULL while current. |
| `is_current` | `BOOLEAN` | True for the version in effect now. |
| `_start_seq` | `BIGINT` | LSN of the change that created this version. |
| `_end_seq` | `BIGINT` | LSN of the change that closed it. |
| `_row_hash` | `STRING` | Hash of the tracked attributes, for change detection. |

### `silver.clickstream`

Validated, deduplicated web and app events (exactly once per event_id). (primary key `event_id`; clustered by `event_date`, `customer_id`.)

| Column | Type | Description |
|---|---|---|
| `event_id` | `STRING` NOT NULL | Event id (UUID). |
| `session_id` | `STRING` | Session. |
| `customer_id` | `STRING` | Customer, NULL for anonymous sessions. |
| `event_type` | `STRING` | Event type. |
| `event_ref` | `STRING` | Show the event is about, if any. |
| `device` | `STRING` | web, ios or android. |
| `event_ts` | `TIMESTAMP` | When it happened (UTC). |
| `event_date` | `DATE` | UTC date of event_ts. |
| `properties` | `STRING` | Event properties as JSON. |
| `_ingested_at` | `TIMESTAMP` | Bronze ingestion time. |

### `silver.events`

Shows, from the latest calendar snapshot. (primary key `event_id`.)

| Column | Type | Description |
|---|---|---|
| `event_id` | `STRING` NOT NULL | Show id. |
| `artist` | `STRING` | Artist. |
| `genre` | `STRING` | Genre. |
| `event_date` | `DATE` | Date of the show. |
| `venue_id` | `STRING` | Venue. |
| `base_price` | `DECIMAL(10,2)` | GA ticket price. |
| `_snapshot_date` | `DATE` | Snapshot the row came from. |

### `silver.venues`

Venues, from the latest snapshot. (primary key `venue_id`.)

| Column | Type | Description |
|---|---|---|
| `venue_id` | `STRING` NOT NULL | Venue id. |
| `venue_name` | `STRING` | Name. |
| `city` | `STRING` | City. |
| `country` | `STRING` | ISO country code. |
| `capacity` | `INT` | Seats. |
| `_snapshot_date` | `DATE` | Snapshot the row came from. |

## Gold

### `gold.dim_customer`

Customer dimension with full history (SCD2): join facts on customer_sk. (primary key `customer_sk`.)

| Column | Type | Description |
|---|---|---|
| `customer_sk` | `STRING` NOT NULL | Surrogate key. |
| `customer_id` | `STRING` | Business key. |
| `email` | `STRING` | Email address (PII). |
| `full_name` | `STRING` | Full name (PII). |
| `country` | `STRING` | ISO country code, upper case. |
| `city` | `STRING` | City. |
| `loyalty_tier` | `STRING` | standard, silver, gold or platinum. |
| `marketing_opt_in` | `BOOLEAN` | Consents to marketing. |
| `signup_date` | `DATE` | Account creation date. |
| `valid_from` | `TIMESTAMP` | Start of this version (the change time). |
| `valid_to` | `TIMESTAMP` | End of this version; NULL while current. |
| `is_current` | `BOOLEAN` | True for the version in effect now. |

### `gold.dim_event`

Show dimension, with its venue. (primary key `event_id`.)

| Column | Type | Description |
|---|---|---|
| `event_id` | `STRING` NOT NULL | Show id. |
| `artist` | `STRING` | Artist. |
| `genre` | `STRING` | Genre. |
| `event_date` | `DATE` | Show date. |
| `venue_id` | `STRING` | Venue id. |
| `venue_name` | `STRING` | Venue. |
| `city` | `STRING` | City. |
| `country` | `STRING` | Country. |
| `capacity` | `INT` | Venue capacity. |
| `base_price` | `DECIMAL(10,2)` | GA price. |

### `gold.fct_sales`

Sales fact at order-line grain for paid orders, with the customer version at payment time. (primary key `order_id`, `line_no`; clustered by `paid_date`.)

| Column | Type | Description |
|---|---|---|
| `order_id` | `STRING` NOT NULL | Order id. |
| `line_no` | `INT` NOT NULL | Line number. |
| `customer_id` | `STRING` | Customer. |
| `customer_sk` | `STRING` | dim_customer version in effect when the order was paid. |
| `event_id` | `STRING` | Show. |
| `channel` | `STRING` | Sales channel. |
| `ticket_type` | `STRING` | GA or VIP. |
| `quantity` | `INT` | Tickets. |
| `unit_price` | `DECIMAL(10,2)` | Price per ticket. |
| `gross_amount` | `DECIMAL(12,2)` | quantity * unit_price. |
| `refunded_amount` | `DECIMAL(12,2)` | gross_amount if the order was refunded, else 0. |
| `order_date` | `DATE` | Order date. |
| `paid_at` | `TIMESTAMP` | Payment time. |
| `paid_date` | `DATE` | Payment date. |
| `refunded_at` | `TIMESTAMP` | Refund time. |
| `refunded_date` | `DATE` | Refund date. |

### `gold.agg_daily_sales`

Daily sales by venue and genre: revenue booked on payment date, refunds on refund date. (clustered by `sales_date`.)

| Column | Type | Description |
|---|---|---|
| `sales_date` | `DATE` | Day. |
| `venue_id` | `STRING` | Venue. |
| `genre` | `STRING` | Genre. |
| `paid_orders` | `BIGINT` | Orders paid that day. |
| `tickets_sold` | `BIGINT` | Tickets paid that day. |
| `gross_revenue` | `DECIMAL(18,2)` | Amount paid that day. |
| `refunded_orders` | `BIGINT` | Orders refunded that day. |
| `refunded_amount` | `DECIMAL(18,2)` | Amount refunded that day. |
| `net_revenue` | `DECIMAL(18,2)` | gross_revenue - refunded_amount. |

### `gold.event_performance`

One row per show: tickets sold (net of refunds), revenue and sell-through. (primary key `event_id`.)

| Column | Type | Description |
|---|---|---|
| `event_id` | `STRING` NOT NULL | Show. |
| `artist` | `STRING` | Artist. |
| `genre` | `STRING` | Genre. |
| `event_date` | `DATE` | Show date. |
| `venue_id` | `STRING` | Venue. |
| `capacity` | `INT` | Capacity. |
| `tickets_sold` | `BIGINT` | Tickets on paid, not refunded orders. |
| `gross_revenue` | `DECIMAL(18,2)` | Gross. |
| `refunded_amount` | `DECIMAL(18,2)` | Refunded. |
| `net_revenue` | `DECIMAL(18,2)` | Net. |
| `sell_through` | `DOUBLE` | tickets_sold / capacity. |
| `first_sale_at` | `TIMESTAMP` | First payment. |
| `last_sale_at` | `TIMESTAMP` | Latest payment. |

### `gold.customer_360`

One row per current customer: profile, purchases, favourite genre and engagement. (primary key `customer_id`.)

| Column | Type | Description |
|---|---|---|
| `customer_id` | `STRING` NOT NULL | Customer. |
| `email` | `STRING` | Email address (PII). |
| `full_name` | `STRING` | Full name (PII). |
| `country` | `STRING` | ISO country code, upper case. |
| `city` | `STRING` | City. |
| `loyalty_tier` | `STRING` | standard, silver, gold or platinum. |
| `marketing_opt_in` | `BOOLEAN` | Consents to marketing. |
| `signup_date` | `DATE` | Account creation date. |
| `first_purchase_at` | `TIMESTAMP` | First payment. |
| `last_purchase_at` | `TIMESTAMP` | Latest payment. |
| `lifetime_orders` | `BIGINT` | Paid orders. |
| `refunded_orders` | `BIGINT` | Refunded orders. |
| `lifetime_net_revenue` | `DECIMAL(18,2)` | Paid minus refunded. |
| `favorite_genre` | `STRING` | Genre with the most tickets (ties: alphabetical). |
| `events_30d` | `BIGINT` | App and web events in the 30 days to as_of_date. |
| `last_seen_at` | `TIMESTAMP` | Latest app or web event. |
| `days_since_last_purchase` | `INT` | as_of_date - date of last purchase. |
| `as_of_date` | `DATE` | Latest data date (not the wall clock), so rebuilds are reproducible. |

### `gold.funnel_daily`

Conversion funnel by session start date and device. (clustered by `session_date`.)

| Column | Type | Description |
|---|---|---|
| `session_date` | `DATE` | Date the session started. |
| `device` | `STRING` | Device. |
| `sessions` | `BIGINT` | Sessions. |
| `viewed_sessions` | `BIGINT` | Sessions that viewed a show. |
| `cart_sessions` | `BIGINT` | ...that added to cart. |
| `checkout_sessions` | `BIGINT` | ...that reached checkout. |
| `purchase_sessions` | `BIGINT` | ...that purchased. |
| `view_to_purchase_rate` | `DOUBLE` | purchase_sessions / viewed_sessions. |

### `gold.live_engagement`

Near-real-time event counts in 5-minute windows (stateful streaming). (clustered by `window_start`.)

| Column | Type | Description |
|---|---|---|
| `window_start` | `TIMESTAMP` | Window start. |
| `window_end` | `TIMESTAMP` | Window end. |
| `event_type` | `STRING` | Event type. |
| `events` | `BIGINT` | Events in the window. |
| `sessions` | `BIGINT` | Approximate distinct sessions. |

## Ml

### `ml.customer_features`

Churn features per customer and as-of date, computed only from data up to that date. (primary key `customer_id`, `as_of_date` (time series); clustered by `as_of_date`.)

| Column | Type | Description |
|---|---|---|
| `customer_id` | `STRING` NOT NULL | Customer. |
| `as_of_date` | `DATE` NOT NULL | Features describe the customer at the end of this day. |
| `recency_days` | `INT` | Days since the last paid order (as of the date). |
| `orders_30d` | `INT` | Paid orders in the 30 days up to the as-of date. |
| `orders_90d` | `INT` | Paid orders, 90 days. |
| `orders_365d` | `INT` | Paid orders, 365 days. |
| `revenue_90d` | `DECIMAL(12,2)` | Gross paid amount, 90 days. |
| `revenue_365d` | `DECIMAL(12,2)` | Gross paid amount, 365 days. |
| `refunds_365d` | `INT` | Orders refunded in the 365 days up to the as-of date. |
| `avg_ticket_price_365d` | `DOUBLE` | Average price per ticket bought, 365 days. |
| `genres_365d` | `INT` | Distinct genres bought, 365 days. |
| `events_7d` | `INT` | App and web events, 7 days. |
| `events_30d` | `INT` | App and web events, 30 days. |
| `cart_adds_30d` | `INT` | Add-to-cart events, 30 days. |
| `engagement_ratio` | `DOUBLE` | events_7d / events_30d: recent engagement versus the month. |
| `tenure_days` | `INT` | Days since signup. |
| `loyalty_tier` | `STRING` | Tier in effect on the as-of date (SCD2). |
| `marketing_opt_in` | `BOOLEAN` | Opt-in in effect on the as-of date. |
| `country` | `STRING` | Country on the as-of date. |

### `ml.churn_labels`

1 if the customer made no paid order in the 60 days after as_of_date. (primary key `customer_id`, `as_of_date`.)

| Column | Type | Description |
|---|---|---|
| `customer_id` | `STRING` NOT NULL | Customer. |
| `as_of_date` | `DATE` NOT NULL | As-of date. |
| `churned` | `INT` | Label. |

### `ml.churn_training_set`

Features joined to labels, split by time (the latest as-of dates are the test set).

| Column | Type | Description |
|---|---|---|
| `customer_id` | `STRING` | Customer. |
| `as_of_date` | `DATE` | As-of date. |
| `recency_days` | `INT` | Days since the last paid order (as of the date). |
| `orders_30d` | `INT` | Paid orders in the 30 days up to the as-of date. |
| `orders_90d` | `INT` | Paid orders, 90 days. |
| `orders_365d` | `INT` | Paid orders, 365 days. |
| `revenue_90d` | `DECIMAL(12,2)` | Gross paid amount, 90 days. |
| `revenue_365d` | `DECIMAL(12,2)` | Gross paid amount, 365 days. |
| `refunds_365d` | `INT` | Orders refunded in the 365 days up to the as-of date. |
| `avg_ticket_price_365d` | `DOUBLE` | Average price per ticket bought, 365 days. |
| `genres_365d` | `INT` | Distinct genres bought, 365 days. |
| `events_7d` | `INT` | App and web events, 7 days. |
| `events_30d` | `INT` | App and web events, 30 days. |
| `cart_adds_30d` | `INT` | Add-to-cart events, 30 days. |
| `engagement_ratio` | `DOUBLE` | events_7d / events_30d: recent engagement versus the month. |
| `tenure_days` | `INT` | Days since signup. |
| `loyalty_tier` | `STRING` | Tier in effect on the as-of date (SCD2). |
| `marketing_opt_in` | `BOOLEAN` | Opt-in in effect on the as-of date. |
| `country` | `STRING` | Country on the as-of date. |
| `churned` | `INT` | Label. |
| `split` | `STRING` | train or test. |

### `ml.model_evaluations`

Every evaluation of a challenger and the champion, with the promotion decision.

| Column | Type | Description |
|---|---|---|
| `run_id` | `STRING` | Job run. |
| `model_name` | `STRING` | Unity Catalog model. |
| `model_version` | `STRING` | Version. |
| `role` | `STRING` | challenger or champion. |
| `test_rows` | `BIGINT` | Rows scored. |
| `roc_auc` | `DOUBLE` | ROC AUC. |
| `pr_auc` | `DOUBLE` | Average precision. |
| `log_loss` | `DOUBLE` | Log loss. |
| `brier` | `DOUBLE` | Brier score. |
| `top_decile_lift` | `DOUBLE` | Churn rate in the top 10% / base rate. |
| `promote` | `BOOLEAN` | Decision (challenger rows). |
| `reasons` | `ARRAY<STRING>` | Why. |
| `evaluated_at` | `TIMESTAMP` | When. |

### `ml.churn_predictions`

Churn scores from the champion model, one row per customer and as-of date. (primary key `customer_id`, `as_of_date`; change data feed on.)

| Column | Type | Description |
|---|---|---|
| `customer_id` | `STRING` NOT NULL | Customer. |
| `as_of_date` | `DATE` NOT NULL | Features date. |
| `churn_probability` | `DOUBLE` | Probability of no purchase in the next 60 days. |
| `risk_band` | `STRING` | high (>= 0.6), medium (>= 0.3) or low. |
| `model_name` | `STRING` | Model. |
| `model_version` | `STRING` | Version that scored. |
| `scored_at` | `TIMESTAMP` | When. |

### `ml.feature_drift`

Population stability index of each feature: latest scoring set versus training.

| Column | Type | Description |
|---|---|---|
| `as_of_date` | `DATE` | Scoring date. |
| `feature` | `STRING` | Feature. |
| `psi` | `DOUBLE` | PSI. |
| `status` | `STRING` | stable, moderate or significant. |
| `computed_at` | `TIMESTAMP` | When. |

## Ops

### `ops.quarantine`

Rows that broke a drop rule, with the raw record, for triage and replay. (clustered by `source_table`.)

| Column | Type | Description |
|---|---|---|
| `source_table` | `STRING` | Table the row was headed for. |
| `record_key` | `STRING` | Business key, if any. |
| `failed_rules` | `ARRAY<STRING>` | Rules broken. |
| `payload` | `STRING` | The raw record as JSON. |
| `source_file` | `STRING` | Source file. |
| `run_id` | `STRING` | Job run. |
| `quarantined_at` | `TIMESTAMP` | When. |

### `ops.quality_metrics`

Rows checked and rows failing each rule, per batch.

| Column | Type | Description |
|---|---|---|
| `run_id` | `STRING` | Job run. |
| `source_table` | `STRING` | Table. |
| `rule` | `STRING` | Rule. |
| `action` | `STRING` | drop, warn or fail. |
| `failed_rows` | `BIGINT` | Failing rows. |
| `total_rows` | `BIGINT` | Rows checked. |
| `measured_at` | `TIMESTAMP` | When. |

### `ops.task_runs`

One row per task run: status, duration and metrics.

| Column | Type | Description |
|---|---|---|
| `run_id` | `STRING` | Job run. |
| `task` | `STRING` | Component id. |
| `status` | `STRING` | SUCCEEDED or FAILED. |
| `started_at` | `TIMESTAMP` | Start. |
| `finished_at` | `TIMESTAMP` | End. |
| `metrics` | `STRING` | JSON. |
| `error` | `STRING` | Error, if failed. |

### `ops.export_bookmarks`

Last change-data-feed version each export has sent. (primary key `export_name`.)

| Column | Type | Description |
|---|---|---|
| `export_name` | `STRING` NOT NULL | Export. |
| `table_name` | `STRING` | Source table. |
| `last_version` | `BIGINT` | Last Delta version exported. |
| `updated_at` | `TIMESTAMP` | When. |
