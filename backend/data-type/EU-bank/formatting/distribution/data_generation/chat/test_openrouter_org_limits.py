#!/usr/bin/env python3
"""
OpenRouter Organization API Key Tester
Tests organization API keys with proper headers and credit access
"""
import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

# Load environment variables from root .env file
root_dir = Path(__file__).parent.parent.parent.parent.parent.parent.parent.parent
env_path = root_dir / '.env'
print(f"Loading .env from: {env_path}")
print(f"File exists: {env_path.exists()}")
load_dotenv(env_path)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

def test_basic_api_call():
    """Test if API key works with a basic small model"""
    print("\n" + "="*70)
    print("STEP 1: Testing API Key with Basic Call")
    print("="*70)
    
    if not OPENROUTER_API_KEY:
        print("❌ ERROR: OPENROUTER_API_KEY not found in environment variables")
        return False
    
    try:
        # Proper headers for organization API keys
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://localhost:3000',  # Required for org keys
            'X-Title': 'OpenRouter API Test',          # Alternative identifier
            'User-Agent': 'OpenRouter-Python-Client/1.0'
        }
        
        test_payload = {
            "model": "google/gemma-3-27b-it:free",  # Your actual model
            "messages": [{"role": "user", "content": "Say 'API works!'"}],
            "max_tokens": 10
        }
        
        print(f"API Key (last 8 chars): ...{OPENROUTER_API_KEY[-8:]}")
        print("Making test API call with free model...")
        
        response = requests.post('https://openrouter.ai/api/v1/chat/completions', 
                                json=test_payload, 
                                headers=headers,
                                timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            print("✅ API Key is VALID and working!")
            if "choices" in result and result["choices"]:
                print(f"✅ Test response: {result['choices'][0]['message']['content']}")
            return True
        elif response.status_code == 401:
            print(f"❌ API Key is INVALID (401 Unauthorized)")
            print(f"Response: {response.text}")
            print("\n💡 Possible issues:")
            print("   1. Wrong API key in .env file")
            print("   2. API key has been revoked")
            print("   3. Organization key format incorrect")
            return False
        elif response.status_code == 429:
            print(f"⚠️  API Key is valid but RATE LIMITED (429)")
            print("✅ This means your key works but you're hitting rate limits")
            print(f"Response: {response.text}")
            return True
        else:
            print(f"⚠️  Got status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing API: {e}")
        return False

def test_gemma_27b_model():
    """Test the actual model used in your script"""
    print("\n" + "="*70)
    print("STEP 2: Testing Gemma 3 27B Model (Your Production Model)")
    print("="*70)
    
    try:
        # Proper headers for organization API keys
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://localhost:3000',
            'X-Title': 'EU Banking Chat Generator',
            'User-Agent': 'OpenRouter-Python-Client/1.0'
        }
        
        # Test with :free suffix (uses free tier)
        test_payload_free = {
            "model": "google/gemma-3-27b-it:free",
            "messages": [{"role": "user", "content": "Generate: {\"test\": \"success\"}"}],
            "max_tokens": 20
        }
        
        print("\nTesting with FREE model (google/gemma-3-27b-it:free)...")
        response_free = requests.post('https://openrouter.ai/api/v1/chat/completions', 
                                     json=test_payload_free, 
                                     headers=headers,
                                     timeout=60)
        
        if response_free.status_code == 200:
            result = response_free.json()
            print("✅ FREE model works!")
            if "choices" in result and result["choices"]:
                print(f"   Response: {result['choices'][0]['message']['content'][:50]}...")
            
            # Check if we can see usage info
            if "usage" in result:
                print(f"   Tokens used: {result['usage']}")
        elif response_free.status_code == 429:
            print(f"⚠️  FREE model RATE LIMITED (429)")
            print(f"   Response: {response_free.text}")
            print("\n   💡 This explains why you're seeing rate limits!")
            print("   💡 Consider using PAID model without :free suffix if you have credits")
        elif response_free.status_code == 401:
            print(f"❌ Authentication failed for FREE model")
            print(f"   Response: {response_free.text}")
        else:
            print(f"⚠️  FREE model returned status: {response_free.status_code}")
            print(f"   Response: {response_free.text}")
        
        # Test with PAID model (uses organization credits)
        print("\nTesting with PAID model (google/gemma-3-27b-it - uses your $18 credits)...")
        test_payload_paid = {
            "model": "google/gemma-3-27b-it",  # Without :free suffix
            "messages": [{"role": "user", "content": "Generate: {\"test\": \"success\"}"}],
            "max_tokens": 20
        }
        
        response_paid = requests.post('https://openrouter.ai/api/v1/chat/completions', 
                                     json=test_payload_paid, 
                                     headers=headers,
                                     timeout=60)
        
        if response_paid.status_code == 200:
            result = response_paid.json()
            print("✅ PAID model works with your organization credits!")
            if "choices" in result and result["choices"]:
                print(f"   Response: {result['choices'][0]['message']['content'][:50]}...")
            
            # Check usage info
            if "usage" in result:
                print(f"   Tokens used: {result['usage']}")
            
            print("\n   💡 RECOMMENDATION: Use 'google/gemma-3-27b-it' (WITHOUT :free)")
            print("   💡 This will use your $18 org credits with BETTER rate limits!")
            
        elif response_paid.status_code == 429:
            print(f"⚠️  PAID model also RATE LIMITED (429)")
            print(f"   Response: {response_paid.text}")
        elif response_paid.status_code == 401:
            print(f"⚠️  PAID model authentication issue")
            print(f"   Response: {response_paid.text}")
        elif response_paid.status_code == 402:
            print(f"⚠️  Insufficient credits (402)")
            print(f"   Response: {response_paid.text}")
            print("\n   💡 You may need to add more credits to your organization")
        else:
            print(f"⚠️  PAID model returned status: {response_paid.status_code}")
            print(f"   Response: {response_paid.text}")
            
    except Exception as e:
        print(f"❌ Error testing Gemma model: {e}")
        import traceback
        traceback.print_exc()

def check_key_info():
    """Try to get key info from API"""
    print("\n" + "="*70)
    print("STEP 3: Fetching Key Info (Credits & Limits)")
    print("="*70)
    
    try:
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'HTTP-Referer': 'https://localhost:3000',
            'X-Title': 'OpenRouter API Test',
            'User-Agent': 'OpenRouter-Python-Client/1.0'
        }
        
        print("\nMaking request to key info endpoint...")
        response = requests.get('https://openrouter.ai/api/v1/key', headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'data' not in data:
                print(f"⚠️  Unexpected response structure: {json.dumps(data, indent=2)}")
                return
            
            key_info = data['data']
            
            print("\n" + "="*70)
            print("ACCOUNT & CREDIT INFORMATION")
            print("="*70)
            
            is_free = key_info.get('is_free_tier', True)
            print(f"\nAccount Type: {'🆓 FREE TIER' if is_free else '💳 PAID TIER'}")
            
            limit = key_info.get('limit')
            limit_remaining = key_info.get('limit_remaining')
            
            if limit is None:
                print("Credit Limit: ♾️  UNLIMITED")
            else:
                print(f"Credit Limit: ${limit:.2f}")
            
            if limit_remaining is None:
                print("Credits Remaining: ♾️  UNLIMITED")
            else:
                print(f"Credits Remaining: ${limit_remaining:.2f}")
                if limit:
                    used_pct = ((limit - limit_remaining) / limit) * 100
                    print(f"Credits Used: {used_pct:.1f}%")
            
            print("\n" + "="*70)
            print("USAGE STATISTICS")
            print("="*70)
            
            usage = key_info.get('usage', 0)
            usage_daily = key_info.get('usage_daily', 0)
            usage_weekly = key_info.get('usage_weekly', 0)
            usage_monthly = key_info.get('usage_monthly', 0)
            
            print(f"\nTotal Usage (all time): ${usage:.4f}")
            print(f"Today's Usage (UTC):    ${usage_daily:.4f}")
            print(f"This Week's Usage:      ${usage_weekly:.4f}")
            print(f"This Month's Usage:     ${usage_monthly:.4f}")
            
            print("\n" + "="*70)
            print("FREE MODEL LIMITS")
            print("="*70)
            
            print(f"\n📊 Rate Limit: 20 requests/minute (for :free models)")
            
            if is_free and limit_remaining and limit_remaining < 10:
                print(f"📊 Daily Limit: 50 :free requests/day (you have ${limit_remaining:.2f} < $10)")
                print("\n⚠️  To unlock 1000 :free requests/day, add $10+ in credits")
            else:
                print(f"📊 Daily Limit: 1000 :free requests/day")
                print("✅ You have enough credits for increased limits!")
            
            # Save to file
            output_file = "openrouter_key_info.json"
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"\n📄 Full response saved to: {output_file}")
            
        elif response.status_code == 401:
            print(f"\n❌ Key info endpoint: 401 Unauthorized")
            print(f"Response: {response.text}")
            print("\n💡 This is common for organization keys - they work but don't expose key info endpoint")
        else:
            print(f"\n⚠️  Key info endpoint returned: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"\n⚠️  Could not fetch key info: {e}")
        print("💡 This is OK - some organization keys don't expose this endpoint")

def main():
    """Run all tests"""
    print("="*70)
    print("OpenRouter Organization API Key Tester")
    print("="*70)
    print(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not OPENROUTER_API_KEY:
        print("\n❌ ERROR: OPENROUTER_API_KEY not found in .env file")
        print(f"Expected location: {env_path}")
        print("\nPlease add to your .env file:")
        print("OPENROUTER_API_KEY=sk-or-v1-your-key-here")
        return
    
    print(f"API Key found (last 8 chars): ...{OPENROUTER_API_KEY[-8:]}")
    
    # Step 1: Test basic API functionality
    api_works = test_basic_api_call()
    
    if not api_works:
        print("\n" + "="*70)
        print("TROUBLESHOOTING STEPS")
        print("="*70)
        print("\n1. Check your .env file at: E:\\office\\clariverseai\\.env")
        print("2. Ensure OPENROUTER_API_KEY is set correctly")
        print("3. Try your organization API key (sk-or-v1-...)")
        print("4. Verify the key is active in OpenRouter dashboard")
        print("5. Make sure there are no extra spaces or quotes around the key")
        return
    
    # Step 2: Test the production model
    test_gemma_27b_model()
    
    # Step 3: Try to get key info
    check_key_info()
    
    # Final recommendations
    print("\n" + "="*70)
    print("RECOMMENDATIONS FOR v2openrouter.py")
    print("="*70)
    
    print("\n📋 Current Configuration:")
    print("   Model: google/gemma-3-27b-it:free (uses free tier)")
    print("   BASE_REQUEST_DELAY: 40 seconds")
    print("   Rate Limit: 20 requests/minute for :free models")
    
    print("\n💡 To Use Organization Credits ($18) and Get Better Rates:")
    print("   1. Change model to: 'google/gemma-3-27b-it' (remove :free suffix)")
    print("   2. This will use your org credits instead of free tier")
    print("   3. May have better rate limits for paid usage")
    
    print("\n⚠️  If Staying with :free Model:")
    print("   Keep BASE_REQUEST_DELAY = 40s to avoid constant rate limits")
    print("   Expect ~20 requests/minute maximum")
    print("   Daily limit: 50 or 1000 requests (depending on credit balance)")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    main()

