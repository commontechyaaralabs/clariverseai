# EU Banking Ticket Generator - Troubleshooting Guide

## Connection Issues

The main issue preventing the script from running is a **network connectivity problem** to the remote Ollama endpoint at `http://80.188.223.202:11467`.

## Quick Diagnosis

### 1. Test Network Connectivity Only
```bash
cd tm2.0/backend/data-type/EU-bank/ticket/
python data_generation_ticket.py --test-network
```

### 2. Run Comprehensive Network Test
```bash
python troubleshoot_connection.py
```

## Common Issues and Solutions

### ❌ Connection Timeout Error
**Error**: `Connection to 80.188.223.202 timed out. (connect timeout=30)`

**Possible Causes**:
- Remote Ollama server is down
- Firewall blocking outbound connections
- Network routing issues
- Server IP address has changed

**Solutions**:
1. **Verify server status**: Check if the remote Ollama server is running
2. **Check firewall**: Ensure outbound connections to port 11467 are allowed
3. **Test from different network**: Try running from another location/network
4. **Update endpoint**: If the server IP has changed, update `OLLAMA_BASE_URL`

### ❌ DNS Resolution Issues
**Error**: `Name or service not known`

**Solutions**:
1. Check DNS configuration
2. Try using IP address directly instead of hostname
3. Verify network DNS settings

### ❌ Authentication Issues
**Error**: `401 Unauthorized` or similar

**Solutions**:
1. Verify `OLLAMA_TOKEN` is correct
2. Check if token has expired
3. Confirm token permissions

## Configuration Updates Made

The script has been enhanced with:

1. **Better timeout handling**: Increased connection timeout from 30s to 60s
2. **Connection retries**: Added 5 retry attempts with exponential backoff
3. **Network diagnostics**: Comprehensive connectivity testing
4. **Better error messages**: More helpful troubleshooting information

## Manual Testing Steps

### Step 1: Basic Network Test
```bash
# Test if you can reach the server
telnet 80.188.223.202 11467
# or
nc -zv 80.188.223.202 11467
```

### Step 2: HTTP Test
```bash
# Test basic HTTP connectivity
curl -v http://80.188.223.202:11467
```

### Step 3: Ollama API Test
```bash
# Test the generate endpoint
curl -X POST http://80.188.223.202:11467/api/generate \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer d5823ebcd546e7c6b61a0abebe1d8481d6acb2587b88d1cadfbe651fc4f6c6d5" \
  -d '{"model":"gemma3:27b","prompt":"test","stream":false}'
```

## Environment Variables

Ensure these are set in your `.env` file:
```bash
MONGO_CONNECTION_STRING=your_mongodb_connection_string
```

## Next Steps

1. **Run the troubleshooting script** to get detailed diagnostics
2. **Check network connectivity** from your current location
3. **Verify the remote server** is accessible
4. **Update configuration** if the endpoint has changed
5. **Contact network administrator** if firewall issues persist

## Alternative Solutions

If the remote Ollama endpoint remains inaccessible:

1. **Use local Ollama**: Install Ollama locally and update the configuration
2. **Different endpoint**: Use an alternative Ollama service
3. **Cloud service**: Consider using a cloud-based LLM service instead

## Log Files

The script generates detailed logs in the `logs/` directory:
- `ticket_generator_*.log` - Main execution log
- `successful_generations_*.log` - Successful ticket generations
- `failed_generations_*.log` - Failed generations with error details
- `progress_*.log` - Processing progress and statistics
- `status_report_*.json` - Final session summary

Check these logs for detailed error information and processing statistics.
