#!/usr/bin/env python3
"""
Simple test script for the Ollama email generator
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ollama_email import test_ollama_connection, generate_optimized_email_prompt, call_ollama_with_backoff

def test_ollama_email_generator():
    """Test the Ollama email generator components"""
    print("Testing Ollama Email Generator Components...")
    print("=" * 50)
    
    # Test 1: Connection test
    print("1. Testing Ollama connection...")
    if test_ollama_connection():
        print("✅ Ollama connection successful")
    else:
        print("❌ Ollama connection failed")
        return False
    
    # Test 2: Prompt generation
    print("\n2. Testing prompt generation...")
    test_email_data = {
        'dominant_topic': 'Account Management',
        'subtopics': 'Account balance inquiry',
        'messages': [
            {
                'headers': {
                    'from': [{'name': 'John Smith', 'email': 'john.smith@customer.com'}],
                    'to': [{'name': 'Support Team', 'email': 'support@eubank.com'}]
                }
            }
        ],
        'stages': 'Receive',
        'category': 'External',
        'overall_sentiment': 2,
        'urgency': False,
        'follow_up_required': 'yes',
        'action_pending_status': 'yes',
        'action_pending_from': 'customer',
        'priority': 'P3-Medium',
        'resolution_status': 'open'
    }
    
    try:
        prompt = generate_optimized_email_prompt(test_email_data)
        print("✅ Prompt generation successful")
        print(f"Prompt length: {len(prompt)} characters")
        print(f"Prompt preview: {prompt[:200]}...")
    except Exception as e:
        print(f"❌ Prompt generation failed: {e}")
        return False
    
    # Test 3: API call test
    print("\n3. Testing API call with simple prompt...")
    try:
        simple_prompt = "Generate a JSON object with 'test': 'success'"
        response = call_ollama_with_backoff(simple_prompt, timeout=30)
        if response:
            print("✅ API call successful")
            print(f"Response preview: {response[:100]}...")
        else:
            print("❌ API call returned empty response")
            return False
    except Exception as e:
        print(f"❌ API call failed: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("✅ All tests passed! Ollama email generator is working correctly.")
    return True

if __name__ == "__main__":
    success = test_ollama_email_generator()
    sys.exit(0 if success else 1)
