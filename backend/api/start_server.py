#!/usr/bin/env python3
"""
Startup script for the Statistics API backend server
"""

import os
import sys
import subprocess
import uvicorn
from pathlib import Path

def main():
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    print("🚀 Starting Statistics API Backend Server...")
    print(f"📁 Working directory: {os.getcwd()}")
    
    # Check if main.py exists
    if not Path("main.py").exists():
        print("❌ Error: main.py not found in current directory")
        print("Please run this script from the backend/ranjith/api directory")
        sys.exit(1)
    
    # Set default environment variables
    os.environ.setdefault("API_HOST", "0.0.0.0")
    os.environ.setdefault("API_PORT", "8000")
    os.environ.setdefault("API_RELOAD", "True")
    os.environ.setdefault("LOG_LEVEL", "info")
    
    print(f"🌐 Server will run on: {os.environ['API_HOST']}:{os.environ['API_PORT']}")
    print(f"🔄 Auto-reload: {os.environ['API_RELOAD']}")
    print(f"📝 Log level: {os.environ['LOG_LEVEL']}")
    print("\n📋 Available endpoints:")
    print("   - Health check: http://localhost:8000/health")
    print("   - API docs: http://localhost:8000/docs")
    print("   - Stats endpoint: http://localhost:8000/api/v1/api/home/stats")
    print("\n⏳ Starting server...")
    print("=" * 50)
    
    try:
        uvicorn.run(
            "main:app",
            host=os.environ["API_HOST"],
            port=int(os.environ["API_PORT"]),
            reload=bool(os.environ["API_RELOAD"]),
            log_level=os.environ["LOG_LEVEL"]
        )
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 