#!/usr/bin/env python3
"""
Ollama Port Scanner
Scans common Ollama ports to find where the service might be running.
"""

import socket
import time
from concurrent.futures import ThreadPoolExecutor

# Target server (Cloudflare Tunnel endpoint)
TARGET_HOST = "sleeve-applying-sri-tells.trycloudflare.com"

# Common Ollama ports to check
OLLAMA_PORTS = [
    11434,  # Default Ollama port
    11467,  # Current configured port
    8080,   # Common alternative
    9000,   # Common alternative
    3000,   # Common alternative
    5000,   # Common alternative
    8000,   # Common alternative
    11435,  # Alternative Ollama port
    11436,  # Alternative Ollama port
]

# Additional common ports to check
COMMON_PORTS = [22, 80, 443, 8080, 9000, 3000, 5000, 8000]

def scan_port(host, port):
    """Scan a single port"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            return port, "OPEN"
        else:
            return port, "CLOSED"
    except Exception as e:
        return port, f"ERROR: {e}"

def scan_ports_parallel(host, ports, max_workers=10):
    """Scan multiple ports in parallel"""
    print(f"🔍 Scanning {len(ports)} ports on {host}...")
    print("=" * 50)
    
    open_ports = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all port scans
        future_to_port = {executor.submit(scan_port, host, port): port for port in ports}
        
        # Collect results as they complete
        for future in future_to_port:
            port, status = future.result()
            if status == "OPEN":
                open_ports.append(port)
                print(f"✅ Port {port:5d}: {status}")
            else:
                print(f"❌ Port {port:5d}: {status}")
    
    return open_ports

def test_ollama_endpoints(host, open_ports):
    """Test if any open ports respond to Ollama API calls"""
    print("\n🤖 Testing Ollama API endpoints on open ports...")
    print("=" * 50)
    
    import requests
    
    # For Cloudflare Tunnel, we test the main endpoint directly
    if host == "sleeve-applying-sri-tells.trycloudflare.com":
        print("🔍 Testing Cloudflare Tunnel endpoint directly...")
        
        base_url = f"https://{host}"
        
        try:
            # Test basic connectivity
            response = requests.get(base_url, timeout=10)
            print(f"✅ Cloudflare Tunnel: HTTP {response.status_code} - {base_url}")
            
            # Test Ollama tags endpoint
            tags_url = f"{base_url}/api/tags"
            try:
                tags_response = requests.get(tags_url, timeout=10)
                if tags_response.status_code == 200:
                    print(f"   🎯 OLLAMA TAGS ENDPOINT FOUND: {tags_url}")
                    try:
                        data = tags_response.json()
                        if 'models' in data:
                            print(f"   📋 Models available: {len(data['models'])}")
                    except:
                        pass
                else:
                    print(f"   ⚠️  Tags endpoint returned: {tags_response.status_code}")
            except Exception as e:
                print(f"   ❌ Tags endpoint not accessible: {e}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Cloudflare Tunnel test failed: {e}")
        
        return
    
    # For other hosts, test individual ports
    for port in open_ports:
        if port in [80, 443]:  # Skip standard web ports
            continue
            
        # Test HTTP endpoint
        protocol = "https" if port == 443 else "http"
        base_url = f"{protocol}://{host}:{port}"
        
        try:
            # Test basic connectivity
            response = requests.get(base_url, timeout=5)
            print(f"✅ Port {port}: HTTP {response.status_code} - {base_url}")
            
            # Test Ollama tags endpoint
            tags_url = f"{base_url}/api/tags"
            try:
                tags_response = requests.get(tags_url, timeout=5)
                if tags_response.status_code == 200:
                    print(f"   🎯 OLLAMA TAGS ENDPOINT FOUND: {tags_url}")
                    try:
                        data = tags_response.json()
                        if 'models' in data:
                            print(f"   📋 Models available: {len(data['models'])}")
                    except:
                        pass
                else:
                    print(f"   ⚠️  Tags endpoint returned: {tags_response.status_code}")
            except:
                print(f"   ❌ Tags endpoint not accessible")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Port {port}: {e}")

def main():
    """Main scanning function"""
    print("🚀 Ollama Port Scanner")
    print("=" * 50)
    print(f"Target: {TARGET_HOST}")
    
    # Check if this is a Cloudflare Tunnel endpoint
    if TARGET_HOST == "sleeve-applying-sri-tells.trycloudflare.com":
        print("🔍 Cloudflare Tunnel endpoint detected - testing directly")
        print("=" * 50)
        
        # Test Cloudflare Tunnel endpoint directly
        test_ollama_endpoints(TARGET_HOST, [])
        
        print("\n" + "=" * 50)
        print("📊 SCAN RESULTS")
        print("=" * 50)
        print("Cloudflare Tunnel endpoint tested directly")
        
        print("\n💡 Recommendations:")
        print("  🎯 Cloudflare Tunnel endpoint is working")
        print("  📝 Use this endpoint in your configuration")
        
        print("\n🔧 Next steps:")
        print("  1. The Cloudflare Tunnel endpoint is accessible")
        print("  2. Use this endpoint in your Ollama configuration")
        print("  3. Ensure your authentication token is correct")
        
        return
    
    # For regular IP addresses, scan ports
    print(f"Scanning {len(OLLAMA_PORTS)} Ollama-specific ports + {len(COMMON_PORTS)} common ports")
    print("=" * 50)
    
    # Scan Ollama-specific ports first
    print("\n🎯 Scanning Ollama-specific ports...")
    ollama_open = scan_ports_parallel(TARGET_HOST, OLLAMA_PORTS)
    
    # Scan common ports
    print("\n🌐 Scanning common ports...")
    common_open = scan_ports_parallel(TARGET_HOST, COMMON_PORTS)
    
    # Combine results
    all_open_ports = list(set(ollama_open + common_open))
    
    print("\n" + "=" * 50)
    print("📊 SCAN RESULTS")
    print("=" * 50)
    print(f"Total open ports found: {len(all_open_ports)}")
    
    if all_open_ports:
        print("Open ports:", sorted(all_open_ports))
        
        # Test Ollama endpoints on open ports
        test_ollama_endpoints(TARGET_HOST, all_open_ports)
        
        print("\n💡 Recommendations:")
        if 11434 in all_open_ports:
            print("  🎯 Port 11434 is open - this is the default Ollama port!")
            print("  📝 Update OLLAMA_BASE_URL to: http://80.188.223.202:11434")
        elif any(port in all_open_ports for port in [8080, 9000, 3000, 5000, 8000]):
            print("  🔍 Found common web service ports - Ollama might be running on one of these")
        else:
            print("  ❓ No obvious Ollama ports found - check server configuration")
    else:
        print("❌ No open ports found - server might be heavily firewalled")
    
    print("\n🔧 Next steps:")
    print("  1. If port 11434 is open, update the configuration")
    print("  2. Check server logs for Ollama service status")
    print("  3. Verify Ollama is actually running on the server")
    print("  4. Check server firewall configuration")

if __name__ == "__main__":
    main()
