import logging
from config import EVENT_MAPPING, FLAT_METRICS

def safe_cast(value, target_type):
    try:
        if target_type == int:
            return int(float(value)) if value else 0
        elif target_type == float:
            return float(value) if value else 0.0
    except (ValueError, TypeError):
        logging.warning(f"Cannot cast {value} to {target_type}, defaulting to 0")
        return 0 if target_type == int else 0.0

def extract_action_value(actions, action_type):
    """Return value for given action_type, default 0."""
    for a in actions or []:
        if a.get("action_type") == action_type:
            return safe_cast(a.get("value"), int)
    return 0

def extract_action_value_float(actions, action_type):
    """Return float value (for revenue) for given action_type, default 0.0."""
    for a in actions or []:
        if a.get("action_type") == action_type:
            return safe_cast(a.get("value"), float)
    return 0.0

def transform_record(record):
    # Basic flat metrics
    row = {}
    for col in FLAT_METRICS:
        row[col] = safe_cast(record.get(col), type(record.get(col)) if record.get(col) is not None else str)
        # If col is string, keep as string; but numeric should be cast
        if col in ["impressions", "clicks", "reach", "unique_clicks"]:
            row[col] = safe_cast(record.get(col), int)
        elif col in ["frequency", "spend", "cpm", "cpc", "ctr", "cpp"]:
            row[col] = safe_cast(record.get(col), float)

    # Extract funnel events from actions
    actions = record.get("actions", [])
    for event_key, action_type in EVENT_MAPPING.items():
        row[event_key] = extract_action_value(actions, action_type)

    # Extract purchase revenue from action_values (choose "purchase" as canonical)
    action_values = record.get("action_values", [])
    purchase_revenue = extract_action_value_float(action_values, "purchase")
    row["purchase_revenue"] = purchase_revenue

    # Derived ROAS
    spend = row.get("spend", 0.0)
    row["roas"] = purchase_revenue / spend if spend > 0 else 0.0

    # Add identifiers for grouping
    row["ad_id"] = record.get("ad_id")
    row["date_start"] = record.get("date_start")
    row["age"] = record.get("age")
    row["gender"] = record.get("gender")

    return row