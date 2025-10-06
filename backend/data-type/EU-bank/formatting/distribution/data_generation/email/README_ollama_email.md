# Ollama Email Generator

This is the Ollama version of the EU Banking Email Thread Generation and Analysis System, converted from the OpenRouter version.

## Key Changes from OpenRouter Version

1. **API Provider**: Changed from OpenRouter to Ollama
2. **API Endpoint**: Uses local Ollama instance at `http://20.66.111.167:7651/api/chat`
3. **Model**: Uses `gemma3:27b` model
4. **Authentication**: Uses Bearer token authentication
5. **Request Format**: Adapted to Ollama's API format
6. **Response Parsing**: Updated to handle Ollama's response structure

## Configuration

The system uses the same configuration system as the chat generator:

- **Ollama Model**: `gemma3:27b`
- **Batch Size**: 3 (configurable)
- **Max Workers**: CPU count (configurable)
- **Request Timeout**: 300 seconds
- **Max Retries**: 5
- **API Delay**: 0.5 seconds between calls

## Usage

### Basic Usage
```python
from ollama_email import main

# Run the email generator
main()
```

### Test Connection
```python
from ollama_email import test_ollama_connection

if test_ollama_connection():
    print("Ollama connection successful")
else:
    print("Ollama connection failed")
```

### Generate Single Email
```python
from ollama_email import generate_email_content

email_data = {
    'dominant_topic': 'Account Management',
    'subtopics': 'Account balance inquiry',
    'messages': [...],
    'stages': 'Receive',
    'category': 'External',
    # ... other fields
}

result = generate_email_content(email_data)
```

## Features

- **Same Prompt Logic**: Uses identical prompt generation as OpenRouter version
- **Same Processing Flow**: Maintains all batch processing, retry logic, and error handling
- **Intermediate Results**: Supports resuming from interruptions
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Database Integration**: Full MongoDB integration with bulk operations
- **Graceful Shutdown**: Proper signal handling and resource cleanup

## Database Schema

Works with the same `email_new` collection structure:
- Thread data with subject normalization
- Message content generation
- Analysis fields (email_summary, follow_up_date, etc.)
- LLM processing tracking

## Logging

Creates timestamped log files:
- `email_generator_ollama_{timestamp}.log` - Main log
- `successful_generations_{timestamp}.log` - Success details
- `failed_generations_{timestamp}.log` - Failure details
- `progress_{timestamp}.log` - Progress tracking
- `intermediate_results_{timestamp}.json` - Recovery data

## Error Handling

- Exponential backoff for API failures
- Retry mechanism for failed records
- Circuit breaker pattern for rate limiting
- Graceful degradation on database errors
- Intermediate results for resuming interrupted sessions

## Performance

- Parallel processing with ThreadPoolExecutor
- Batch database operations
- Optimized memory usage
- Real-time progress monitoring
- System resource tracking

## Dependencies

- `requests` for HTTP calls
- `pymongo` for database operations
- `backoff` for retry logic
- `psutil` for system monitoring
- Standard library modules for threading, logging, etc.

## Environment Variables

- `MONGO_CONNECTION_STRING` - MongoDB connection string
- `OLLAMA_API_KEY` - Ollama API key (optional, has default)

## Testing

Run the test script to verify functionality:
```bash
python test_ollama_email.py
```

This will test:
1. Ollama connection
2. Prompt generation
3. API call functionality
