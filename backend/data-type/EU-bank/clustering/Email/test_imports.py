#!/usr/bin/env python3
"""
Test script to verify imports work correctly before running the main clustering script.
"""

print("Testing imports for clustering script...")
print("=" * 50)

# Test basic imports
try:
    import warnings
    print("✓ warnings imported successfully")
except ImportError as e:
    print(f"✗ warnings import failed: {e}")

try:
    import re
    print("✓ re imported successfully")
except ImportError as e:
    print(f"✗ re import failed: {e}")

try:
    import numpy as np
    print("✓ numpy imported successfully")
except ImportError as e:
    print(f"✗ numpy import failed: {e}")

try:
    import pandas as pd
    print("✓ pandas imported successfully")
except ImportError as e:
    print(f"✗ pandas import failed: {e}")

try:
    import os
    print("✓ os imported successfully")
except ImportError as e:
    print(f"✗ os import failed: {e}")

try:
    import time
    print("✓ time imported successfully")
except ImportError as e:
    print(f"✗ time import failed: {e}")

try:
    import umap
    print("✓ umap imported successfully")
except ImportError as e:
    print(f"✗ umap import failed: {e}")

try:
    from sklearn.metrics import silhouette_score
    print("✓ sklearn.metrics imported successfully")
except ImportError as e:
    print(f"✗ sklearn.metrics import failed: {e}")

try:
    from sklearn.metrics.pairwise import cosine_similarity
    print("✓ sklearn.metrics.pairwise imported successfully")
except ImportError as e:
    print(f"✗ sklearn.metrics.pairwise import failed: {e}")

try:
    from pymongo import MongoClient
    print("✓ pymongo imported successfully")
except ImportError as e:
    print(f"✗ pymongo import failed: {e}")

try:
    from bson import ObjectId
    print("✓ bson imported successfully")
except ImportError as e:
    print(f"✗ bson import failed: {e}")

try:
    from dotenv import load_dotenv
    print("✓ python-dotenv imported successfully")
except ImportError as e:
    print(f"✗ python-dotenv import failed: {e}")

try:
    import json
    print("✓ json imported successfully")
except ImportError as e:
    print(f"✗ json import failed: {e}")

print("\n" + "=" * 50)

# Test optional imports
print("\nTesting optional imports...")
print("-" * 30)

try:
    import spacy
    print("✓ spacy imported successfully")
    try:
        nlp = spacy.load("en_core_web_sm")
        print("✓ spaCy English model loaded successfully")
    except OSError:
        print("⚠ spaCy imported but English model not found")
        print("  Run: python -m spacy download en_core_web_sm")
except ImportError as e:
    print(f"✗ spacy import failed: {e}")

try:
    import gensim
    from gensim.corpora.dictionary import Dictionary
    from gensim.models.coherencemodel import CoherenceModel
    print("✓ gensim imported successfully")
except ImportError as e:
    print(f"✗ gensim import failed: {e}")

try:
    import hdbscan
    print("✓ hdbscan imported successfully")
except ImportError as e:
    print(f"✗ hdbscan import failed: {e}")

print("\n" + "=" * 50)
print("Import test completed!")
print("\nIf you see any ✗ marks above, you need to install those packages.")
print("Run the fix_environment.bat or fix_environment.ps1 script to fix issues.")
