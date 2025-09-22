# Configuration for EU Banking Data Generators with OpenRouter

# OpenRouter Configuration
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"

# Processing Configuration - Optimized for sequential processing to avoid rate limiting
BATCH_SIZE = 1  # Process one ticket at a time
MAX_WORKERS = 1  # Single worker for sequential processing
REQUEST_TIMEOUT = 120
MAX_RETRIES = 5
RETRY_DELAY = 10  # Increased retry delay
BATCH_DELAY = 15.0  # Increased batch delay
API_CALL_DELAY = 5.0  # Increased API call delay to reduce rate limiting

def get_rate_limit_config():
    """Get rate limiting configuration based on model and endpoint"""
    return {
        "batch_size": BATCH_SIZE,
        "max_workers": MAX_WORKERS,
        "batch_delay": BATCH_DELAY,
        "api_call_delay": API_CALL_DELAY
    }
