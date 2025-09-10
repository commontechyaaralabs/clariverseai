# Installing spaCy for Clustering Script

## Why spaCy is Required

The clustering script uses spaCy for advanced text processing:
- **Lemmatization** - converts words to their base form (e.g., "running" → "run")
- **Stop word removal** - removes common words like "the", "and", "is"
- **Punctuation filtering** - removes punctuation marks
- **Better text preprocessing** - improves clustering quality

## Installation Methods

### Method 1: Automated Installation (Recommended)

**PowerShell:**
```powershell
.\install_with_spacy.ps1
```

**Windows Batch:**
```cmd
install_with_spacy.bat
```

### Method 2: Manual Installation

```bash
# Step 1: Upgrade pip
python -m pip install --upgrade pip

# Step 2: Install core packages
pip install numpy pandas scikit-learn

# Step 3: Install clustering packages
pip install hdbscan umap-learn

# Step 4: Install data packages
pip install pymongo python-dotenv

# Step 5: Install spaCy (specific version for Python 3.12 compatibility)
pip install spacy==3.7.2

# Step 6: Install gensim
pip install gensim==4.3.2

# Step 7: Download English language model
python -m spacy download en_core_web_sm
```

## Troubleshooting Common Issues

### Issue 1: "Microsoft Visual C++ 14.0 is required"

**Solution:**
```bash
# Install pre-compiled wheel instead
pip install spacy --only-binary=all
```

### Issue 2: "spaCy model not found"

**Solution:**
```bash
# Download the English model
python -m spacy download en_core_web_sm

# Or download a smaller model if space is limited
python -m spacy download en_core_web_sm --direct
```

### Issue 3: "Permission denied" errors

**Solution:**
```bash
# Use user installation
pip install --user spacy==3.7.2
python -m spacy download --user en_core_web_sm
```

### Issue 4: Version conflicts with existing packages

**Solution:**
```bash
# Create a fresh virtual environment
python -m venv clustering_env
clustering_env\Scripts\activate  # Windows
pip install -r requirements_with_spacy.txt
```

## Alternative: Use conda (if available)

```bash
# Install with conda (often avoids build issues)
conda install -c conda-forge spacy
python -m spacy download en_core_web_sm
```

## Verification

After installation, test with:

```bash
python test_imports.py
```

You should see:
```
✓ spacy imported successfully
✓ spaCy English model loaded successfully
✓ gensim imported successfully
```

## What spaCy Provides

1. **Advanced Text Processing:**
   - Lemmatization: "processing" → "process"
   - Stop word removal: "the bank error" → "bank error"
   - Punctuation filtering: "error!" → "error"

2. **Better Clustering:**
   - More meaningful keyphrases
   - Reduced noise in clusters
   - Improved similarity calculations

3. **Professional NLP:**
   - Industry-standard text processing
   - Consistent results across different text formats

## Fallback Behavior

If spaCy installation fails, the script will:
- Use basic text processing (lowercase + punctuation removal)
- Skip coherence scoring
- Still provide clustering functionality
- Show warnings about reduced functionality

## Support

If you encounter issues:
1. Check Python version (3.12 recommended)
2. Ensure pip is up to date
3. Try the automated installation scripts
4. Check the troubleshooting section above

