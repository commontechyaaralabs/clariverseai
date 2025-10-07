"""
Simple OpenRouter API Test Script
Tests multiple models to see which ones are available on your account
"""
import os
import requests
import json
from dotenv import load_dotenv

# Load environment
load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# List of models to test (from free to paid)
MODELS_TO_TEST = [
    "deepseek/deepseek-r1:free",
    "google/gemini-flash-1.5:free",
    "meta-llama/llama-3.2-3b-instruct:free",
    "google/gemma-2-9b-it:free",
    "openai/gpt-3.5-turbo",
    "anthropic/claude-3.5-sonnet",
]

def test_model(model_name):
    """Test a single model with a simple request"""
    print(f"\n{'='*60}")
    print(f"Testing: {model_name}")
    print(f"{'='*60}")
    
    headers = {
        'Authorization': f'Bearer {OPENROUTER_API_KEY}',
        'Content-Type': 'application/json',
        'HTTP-Referer': 'http://localhost:3000',
        'X-Title': 'OpenRouter Model Test'
    }
    
    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "user",
                "content": "Say 'Hello! I am working.' in exactly 5 words."
            }
        ],
        "max_tokens": 50
    }
    
    try:
        response = requests.post(
            OPENROUTER_URL,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            print(f"✅ SUCCESS!")
            print(f"Response: {content}")
            return True
        else:
            error_data = response.json()
            print(f"❌ FAILED")
            print(f"Error: {error_data.get('error', {}).get('message', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False

def main():
    print("="*60)
    print("OpenRouter API Model Availability Test")
    print("="*60)
    
    if not OPENROUTER_API_KEY:
        print("❌ ERROR: OPENROUTER_API_KEY not found in environment!")
        print("Please set it in your .env file")
        return
    
    print(f"API Key: {OPENROUTER_API_KEY[:10]}...")
    print(f"\nTesting {len(MODELS_TO_TEST)} models...")
    
    results = {}
    for model in MODELS_TO_TEST:
        results[model] = test_model(model)
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    working_models = [m for m, works in results.items() if works]
    failed_models = [m for m, works in results.items() if not works]
    
    if working_models:
        print(f"\n✅ Working Models ({len(working_models)}):")
        for model in working_models:
            print(f"   - {model}")
    
    if failed_models:
        print(f"\n❌ Failed Models ({len(failed_models)}):")
        for model in failed_models:
            print(f"   - {model}")
    
    # Recommendation
    if working_models:
        print(f"\n🎯 RECOMMENDATION: Use '{working_models[0]}' in your script")
        print(f"\nUpdate v2openrouter.py line 83:")
        print(f'   OPENROUTER_MODEL = "{working_models[0]}"')
    else:
        print("\n⚠️  No models worked. Possible issues:")
        print("   1. API key may be invalid")
        print("   2. Account may need verification")
        print("   3. Need to add credits (even for free models)")
        print("   4. Check https://openrouter.ai/settings")

if __name__ == "__main__":
    main()


