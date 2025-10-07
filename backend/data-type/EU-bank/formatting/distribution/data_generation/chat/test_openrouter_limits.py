#!/usr/bin/env python3
"""
OpenRouter API Rate Limit and Credit Checker
Tests API key limits, credits, usage, and rate limits
"""
import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

# Load environment variables from root .env file
# Current file is at: backend/data-type/EU-bank/formatting/distribution/data_generation/chat/test_openrouter_limits.py
# Root is at: clariverseai/
# So we need to go up 8 levels: chat -> data_generation -> distribution -> formatting -> EU-bank -> data-type -> backend -> clariverseai
root_dir = Path(__file__).parent.parent.parent.parent.parent.parent.parent.parent
env_path = root_dir / '.env'
print(f"Loading .env from: {env_path}")
print(f"File exists: {env_path.exists()}")
load_dotenv(env_path)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_KEY_URL = "https://openrouter.ai/api/v1/key"

def test_api_call():
    """Test if API key works with a simple call"""
    print("\n" + "="*70)
    print("Testing API Key with Simple Call")
    print("="*70)
    
    try:
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'http://localhost:3000',
            'X-Title': 'API Test'
        }
        
        test_payload = {
            "model": "google/gemma-3-27b-it:free",
            "messages": [{"role": "user", "content": "Say 'API works!'"}],
            "max_tokens": 10
        }
        
        print("Making test API call...")
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
            return False
        elif response.status_code == 429:
            print(f"⚠️  API Key is valid but RATE LIMITED (429)")
            print("✅ This means your key works but you're hitting rate limits")
            return True
        else:
            print(f"⚠️  Got status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing API: {e}")
        return False

def check_api_limits():
    """Check OpenRouter API rate limits and credits"""
    
    if not OPENROUTER_API_KEY:
        print("❌ ERROR: OPENROUTER_API_KEY not found in environment variables")
        print("Please set OPENROUTER_API_KEY in your .env file")
        return
    
    print("="*70)
    print("OpenRouter API Rate Limit & Credit Checker")
    print("="*70)
    print(f"\nAPI Key (last 8 chars): ...{OPENROUTER_API_KEY[-8:]}")
    print(f"Checking limits at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # First test if the API key works at all
    api_works = test_api_call()
    
    if not api_works:
        print("\n⚠️  Skipping limit check since API key appears invalid")
        print("Please check your OPENROUTER_API_KEY in the .env file")
        return
    
    try:
        # Make GET request to check limits
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
        }
        
        print("\n" + "="*70)
        print("Fetching Rate Limit & Credit Information")
        print("="*70)
        print("\nMaking request to OpenRouter key info endpoint...")
        response = requests.get(OPENROUTER_KEY_URL, headers=headers)
        
        if response.status_code != 200:
            print(f"\n⚠️  Note: Key info endpoint returned status {response.status_code}")
            print(f"Response: {response.text}")
            print("\n💡 Your API key works (confirmed by test call above)")
            print("   but the key info endpoint may not be available for your account type")
            return
        
        data = response.json()
        
        if 'data' not in data:
            print(f"\n❌ ERROR: Unexpected response structure")
            print(f"Response: {json.dumps(data, indent=2)}")
            return
        
        key_info = data['data']
        
        # Display results
        print("\n" + "="*70)
        print("ACCOUNT STATUS")
        print("="*70)
        
        is_free = key_info.get('is_free_tier', True)
        print(f"Account Type: {'🆓 FREE TIER' if is_free else '💳 PAID TIER (has purchased credits)'}")
        
        print("\n" + "="*70)
        print("CREDIT INFORMATION")
        print("="*70)
        
        limit = key_info.get('limit')
        limit_remaining = key_info.get('limit_remaining')
        limit_reset = key_info.get('limit_reset')
        
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
        
        if limit_reset:
            print(f"Limit Reset: {limit_reset}")
        
        print("\n" + "="*70)
        print("USAGE STATISTICS")
        print("="*70)
        
        usage = key_info.get('usage', 0)
        usage_daily = key_info.get('usage_daily', 0)
        usage_weekly = key_info.get('usage_weekly', 0)
        usage_monthly = key_info.get('usage_monthly', 0)
        
        print(f"Total Usage (all time): ${usage:.4f}")
        print(f"Today's Usage (UTC):    ${usage_daily:.4f}")
        print(f"This Week's Usage:      ${usage_weekly:.4f}")
        print(f"This Month's Usage:     ${usage_monthly:.4f}")
        
        # BYOK usage
        byok_usage = key_info.get('byok_usage', 0)
        if byok_usage > 0:
            print(f"\nBYOK Usage (all time):  ${byok_usage:.4f}")
            print(f"BYOK Usage (daily):     ${key_info.get('byok_usage_daily', 0):.4f}")
        
        print("\n" + "="*70)
        print("FREE MODEL RATE LIMITS (for :free models)")
        print("="*70)
        
        if is_free:
            # Free tier limits
            if limit_remaining and limit_remaining < 10:
                print("📊 Rate Limit: 20 requests/minute")
                print("📊 Daily Limit: 50 :free model requests/day (less than $10 credits)")
                print(f"\n⚠️  WARNING: You have less than $10 credits (${limit_remaining:.2f} remaining)")
                print("   To unlock 1000 :free requests/day, add at least $10 in credits")
            else:
                print("📊 Rate Limit: 20 requests/minute")
                print("📊 Daily Limit: 1000 :free model requests/day (at least $10 credits)")
                print("\n✅ You have enough credits for increased daily limits!")
        else:
            print("📊 Rate Limit: 20 requests/minute")
            print("📊 Daily Limit: 1000 :free model requests/day")
            print("\n✅ Paid tier - you have increased limits!")
        
        print("\n" + "="*70)
        print("RECOMMENDATIONS FOR YOUR SCRIPT")
        print("="*70)
        
        # Calculate safe request rate
        max_rpm = 20  # requests per minute for free models
        safe_rpm = max_rpm * 0.6  # 60% of max to avoid hitting limits
        safe_delay = 60 / safe_rpm  # seconds between requests
        
        print(f"\n📈 Maximum Rate: {max_rpm} requests/minute")
        print(f"✅ Safe Rate: {safe_rpm:.1f} requests/minute (60% of max)")
        print(f"⏱️  Recommended Delay: {safe_delay:.1f} seconds between requests")
        
        # Check current daily usage against limits
        if is_free and limit_remaining and limit_remaining < 10:
            daily_limit = 50
        else:
            daily_limit = 1000
        
        if usage_daily > 0:
            requests_today = usage_daily * 100  # Rough estimate (depends on model cost)
            print(f"\n📊 Estimated Requests Today: ~{requests_today:.0f}")
            print(f"📊 Daily Limit: {daily_limit} requests")
            if requests_today > daily_limit * 0.8:
                print("⚠️  WARNING: You're approaching your daily limit!")
        
        # Current script settings check
        current_delay = 40.0  # BASE_REQUEST_DELAY from your script
        print(f"\n🔧 Your Current Script Settings:")
        print(f"   BASE_REQUEST_DELAY = {current_delay}s")
        print(f"   Requests per minute: ~{60/current_delay:.1f} RPM")
        
        if (60/current_delay) > safe_rpm:
            print(f"   ⚠️  WARNING: Current delay may cause rate limits!")
            print(f"   ✅ Recommended: Increase to {safe_delay:.1f}s or higher")
        else:
            print(f"   ✅ Safe: Well below rate limit threshold")
        
        print("\n" + "="*70)
        print("KEY INSIGHTS")
        print("="*70)
        
        insights = []
        
        if is_free and limit_remaining and limit_remaining < 10:
            insights.append("⚠️  Add at least $10 credits to increase daily limit from 50 to 1000 requests")
        
        if limit_remaining and limit_remaining < 1:
            insights.append("🔴 CRITICAL: You're out of credits! Add more credits to continue")
        elif limit_remaining and limit_remaining < 5:
            insights.append("⚠️  WARNING: Low credits remaining. Consider adding more")
        
        if usage_daily > 0:
            insights.append(f"📊 You've used ${usage_daily:.4f} in credits today")
        
        if insights:
            for insight in insights:
                print(f"   {insight}")
        else:
            print("   ✅ Everything looks good!")
        
        print("\n" + "="*70)
        
        # Save full response to file for reference
        output_file = "openrouter_limits_check.json"
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"📄 Full response saved to: {output_file}")
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Network Error: {e}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_api_limits()

