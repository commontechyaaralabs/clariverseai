# Configuration for EU Banking Data Generators with OpenRouter

# OpenRouter Configuration
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"

# Processing Configuration
BATCH_SIZE = 3
MAX_WORKERS = 2
REQUEST_TIMEOUT = 120
MAX_RETRIES = 5
RETRY_DELAY = 3
BATCH_DELAY = 5.0
API_CALL_DELAY = 1.0

def get_rate_limit_config():
    """Get rate limiting configuration based on model and endpoint"""
    return {
        "batch_size": BATCH_SIZE,
        "max_workers": MAX_WORKERS,
        "batch_delay": BATCH_DELAY,
        "api_call_delay": API_CALL_DELAY
    }
