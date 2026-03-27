EVENT_MAPPING = {
    "purchases": "purchase",
    "add_to_cart": "add_to_cart",
    "initiate_checkout": "initiate_checkout",
    "add_payment_info": "add_payment_info"
}

# Columns to keep from the original record
FLAT_METRICS = [
    "campaign_id", "campaign_name", "adset_id", "adset_name",
    "ad_id", "ad_name", "impressions", "clicks", "reach",
    "frequency", "spend", "cpm", "cpc", "ctr", "cpp", "unique_clicks"
]