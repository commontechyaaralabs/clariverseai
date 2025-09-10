#!/usr/bin/env python3
"""
Ollama Connection Troubleshooting Script
This script helps diagnose connection issues with the remote Ollama endpoint.
"""

import os
import sys
import socket
import requests
import time
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ollama configuration (same as main script)
OLLAMA_BASE_URL = "https://sleeve-applying-sri-tells.trycloudflare.com"
OLLAMA_TOKEN = "d5823ebcd546e7c6b61a0abebe1d8481d6acb2587b88d1cadfbe651fc4f6c6d5"
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
OLLAMA_TAGS_URL = f"{OLLAMA_BASE_URL}/api/tags"
OLLAMA_MODEL = "gemma3:27b"

def test_basic_connectivity():
    """Test basic network connectivity"""
    print("🔍 Testing basic network connectivity...")
    
    try:
        parsed_url = urlparse(OLLAMA_BASE_URL)
        host = parsed_url.hostname
        port = parsed_url.port or 80
        
        print(f"  Host: {host}")
        print(f"  Port: {port}")
        
        # Test socket connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print("✅ Basic network connectivity: SUCCESS")
            return True
        else:
            print(f"❌ Basic network connectivity: FAILED (error code: {result})")
            return False
            
    except Exception as e:
        print(f"❌ Network test error: {e}")
        return False

def test_http_endpoint():
    """Test if the HTTP endpoint is reachable"""
    print("\n🌐 Testing HTTP endpoint reachability...")
    
    try:
        response = requests.get(OLLAMA_BASE_URL, timeout=10)
        print(f"✅ HTTP endpoint reachable: {response.status_code}")
        return True
    except requests.exceptions.Timeout:
        print("❌ HTTP endpoint timeout")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"❌ HTTP connection error: {e}")
        return False
    except Exception as e:
        print(f"❌ HTTP test error: {e}")
        return False

def test_ollama_api():
    """Test Ollama API endpoints"""
    print("\n🤖 Testing Ollama API endpoints...")
    
    # Test tags endpoint
    try:
        headers = {'Authorization': f'Bearer {OLLAMA_TOKEN}'} if OLLAMA_TOKEN else {}
        response = requests.get(OLLAMA_TAGS_URL, headers=headers, timeout=15)
        print(f"✅ Tags endpoint: HTTP {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"  Response keys: {list(data.keys())}")
            except:
                print("  Response is not valid JSON")
    except Exception as e:
        print(f"❌ Tags endpoint error: {e}")
    
    # Test generate endpoint
    try:
        test_payload = {
            "model": OLLAMA_MODEL,
            "prompt": "test",
            "stream": False,
            "options": {"num_predict": 10}
        }
        
        headers = {'Authorization': f'Bearer {OLLAMA_TOKEN}'} if OLLAMA_TOKEN else {}
        response = requests.post(OLLAMA_URL, json=test_payload, headers=headers, timeout=30)
        print(f"✅ Generate endpoint: HTTP {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"  Response keys: {list(data.keys())}")
                if 'response' in data:
                    print(f"  Response preview: {data['response'][:50]}...")
            except:
                print("  Response is not valid JSON")
        else:
            print(f"  Response text: {response.text[:200]}...")
            
    except Exception as e:
        print(f"❌ Generate endpoint error: {e}")

def test_dns_resolution():
    """Test DNS resolution"""
    print("\n🔍 Testing DNS resolution...")
    
    try:
        parsed_url = urlparse(OLLAMA_BASE_URL)
        host = parsed_url.hostname
        
        import socket
        ip_address = socket.gethostbyname(host)
        print(f"✅ DNS resolution: {host} -> {ip_address}")
        return True
    except socket.gaierror as e:
        print(f"❌ DNS resolution failed: {e}")
        return False
    except Exception as e:
        print(f"❌ DNS test error: {e}")
        return False

def test_port_scan():
    """Test common ports on the target host"""
    print("\n🔌 Testing common ports...")
    
    try:
        parsed_url = urlparse(OLLAMA_BASE_URL)
        host = parsed_url.hostname
        
        common_ports = [22, 80, 443, 11467, 8080, 9000]
        
        for port in common_ports:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    print(f"✅ Port {port}: OPEN")
                else:
                    print(f"❌ Port {port}: CLOSED")
            except:
                print(f"❌ Port {port}: ERROR")
                
    except Exception as e:
        print(f"❌ Port scan error: {e}")

def run_comprehensive_test():
    """Run all tests"""
    print("🚀 Ollama Connection Troubleshooting")
    print("=" * 50)
    print(f"Target: {OLLAMA_BASE_URL}")
    print(f"Model: {OLLAMA_MODEL}")
    print(f"Token: {'Configured' if OLLAMA_TOKEN else 'Not configured'}")
    print("=" * 50)
    
    # Run tests
    dns_ok = test_dns_resolution()
    network_ok = test_basic_connectivity()
    http_ok = test_http_endpoint()
    
    if network_ok and http_ok:
        test_ollama_api()
    
    test_port_scan()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    if dns_ok and network_ok and http_ok:
        print("✅ Basic connectivity is working")
        print("🔍 Check Ollama service status and configuration")
    elif dns_ok and not network_ok:
        print("❌ Network connectivity issue")
        print("🔍 Check firewall settings and network routing")
    elif not dns_ok:
        print("❌ DNS resolution issue")
        print("🔍 Check DNS configuration and network settings")
    else:
        print("❌ Multiple connectivity issues detected")
        print("🔍 Check network configuration and server status")
    
    print("\n💡 Troubleshooting tips:")
    print("  1. Verify the remote Ollama server is running")
    print("  2. Check firewall rules for outbound connections")
    print("  3. Verify network routing to the target IP")
    print("  4. Test with a different network if possible")
    print("  5. Contact network administrator if issues persist")

if __name__ == "__main__":
    run_comprehensive_test()
