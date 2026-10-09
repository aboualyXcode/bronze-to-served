"""Silver data contracts as expectations. A row that breaks a `drop` rule is quarantined with the rule names.

Conditions are evaluated on the typed columns (after try_cast), so 'valid_order_ts' catches both missing and
unparseable timestamps.
"""
from ..core.quality import Expectation as E

ORDER_RULES = [
    E("order_id_present", "order_id IS NOT NULL AND order_id <> ''"),
    E("customer_id_present", "customer_id IS NOT NULL AND customer_id <> ''"),
    E("event_id_present", "event_id IS NOT NULL AND event_id <> ''"),
    E("valid_order_ts", "order_ts IS NOT NULL", description="present and parseable as a timestamp"),
    E("valid_updated_at", "updated_at IS NOT NULL"),
    E("known_status", "status IN ('PLACED', 'PAID', 'CANCELLED', 'REFUNDED')"),
    E("has_items", "items IS NOT NULL AND size(items) > 0"),
    E("valid_items", "forall(items, x -> x.line_no IS NOT NULL AND x.quantity > 0 AND x.unit_price >= 0)",
      description="every line has a number, a positive quantity and a non-negative price"),
    E("known_channel", "channel IN ('web', 'app', 'partner', 'box_office')", action="warn"),
]

CDC_RULES = [
    E("customer_id_present", "customer_id IS NOT NULL AND customer_id <> ''"),
    E("valid_lsn", "lsn IS NOT NULL", description="the CDC sequence number orders changes per customer"),
    E("known_op", "op IN ('INSERT', 'UPDATE', 'DELETE')"),
    E("valid_changed_at", "changed_at IS NOT NULL"),
    E("email_format", "op = 'DELETE' OR instr(email, '@') > 1", action="warn"),
]

CLICKSTREAM_RULES = [
    E("event_id_present", "event_id IS NOT NULL AND event_id <> ''"),
    E("session_present", "session_id IS NOT NULL AND session_id <> ''"),
    E("valid_event_ts", "event_ts IS NOT NULL"),
    E("known_event_type", "event_type IN ('page_view', 'search', 'view_event', 'add_to_cart', 'checkout', 'purchase')"),
    E("known_device", "device IN ('web', 'ios', 'android')", action="warn"),
]

EVENT_RULES = [
    E("event_id_present", "event_id IS NOT NULL AND event_id <> ''"),
    E("venue_id_present", "venue_id IS NOT NULL AND venue_id <> ''"),
    E("valid_event_date", "event_date IS NOT NULL"),
    E("valid_base_price", "base_price IS NOT NULL AND base_price >= 0"),
]

VENUE_RULES = [
    E("venue_id_present", "venue_id IS NOT NULL AND venue_id <> ''"),
    E("valid_capacity", "capacity IS NOT NULL AND capacity > 0"),
]
