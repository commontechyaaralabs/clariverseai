@echo off
echo Installing clustering packages with spaCy support...

echo.
echo Step 1: Upgrading pip...
python -m pip install --upgrade pip

echo.
echo Step 2: Installing core packages first...
pip install numpy pandas scikit-learn

echo.
echo Step 3: Installing clustering packages...
pip install hdbscan umap-learn

echo.
echo Step 4: Installing data packages...
pip install pymongo python-dotenv

echo.
echo Step 5: Installing spaCy with compatible versions...
echo Installing spaCy 3.7.2 (compatible with Python 3.12)...
pip install spacy==3.7.2

echo.
echo Step 6: Installing gensim for coherence scoring...
pip install gensim==4.3.2

echo.
echo Step 7: Downloading spaCy English model...
python -m spacy download en_core_web_sm

echo.
echo Step 8: Testing all imports...
python test_imports.py

echo.
echo Installation complete with spaCy support!
echo You can now run: python clustering.py
pause

