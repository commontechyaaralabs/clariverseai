# Configuration for EU Banking Email Generator with Ollama

# Ollama Configuration
OLLAMA_BASE_URL = "https://quiet-accompanied-holland-candidate.trycloudflare.com"
OLLAMA_TOKEN = "8e5ded514b6e62c4b3faaf7e9f6d4179e0190ca9523fba2ce0ba4ebdf6cd0842"
OLLAMA_MODEL = "gemma3:27b"

# Processing Configuration
BATCH_SIZE = 3
MAX_WORKERS = 5
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
