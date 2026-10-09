"""The raw fields each upstream source delivers (the Bronze contract: every field kept, as a string)."""

ORDERS_RAW = ("order_id", "customer_id", "event_id", "order_ts", "status", "updated_at", "channel", "currency",
              "promo_code", "items", "source_system")
CUSTOMERS_CDC_RAW = ("lsn", "op", "changed_at", "customer_id", "email", "full_name", "country", "city",
                     "loyalty_tier", "marketing_opt_in", "signup_date")
CLICKSTREAM_RAW = ("event_id", "session_id", "customer_id", "event_type", "event_ref", "device", "event_ts",
                   "properties")
EVENTS_RAW = ("event_id", "artist", "genre", "event_date", "venue_id", "base_price")
VENUES_RAW = ("venue_id", "venue_name", "city", "country", "capacity")
