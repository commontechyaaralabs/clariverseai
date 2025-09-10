Write-Host "Installing clustering packages with spaCy support..." -ForegroundColor Green

Write-Host "`nStep 1: Upgrading pip..." -ForegroundColor Yellow
python -m pip install --upgrade pip

Write-Host "`nStep 2: Installing core packages first..." -ForegroundColor Yellow
pip install numpy pandas scikit-learn

Write-Host "`nStep 3: Installing clustering packages..." -ForegroundColor Yellow
pip install hdbscan umap-learn

Write-Host "`nStep 4: Installing data packages..." -ForegroundColor Yellow
pip install pymongo python-dotenv

Write-Host "`nStep 5: Installing spaCy with compatible versions..." -ForegroundColor Yellow
Write-Host "Installing spaCy 3.7.2 (compatible with Python 3.12)..." -ForegroundColor Cyan
pip install spacy==3.7.2

Write-Host "`nStep 6: Installing gensim for coherence scoring..." -ForegroundColor Yellow
pip install gensim==4.3.2

Write-Host "`nStep 7: Downloading spaCy English model..." -ForegroundColor Yellow
python -m spacy download en_core_web_sm

Write-Host "`nStep 8: Testing all imports..." -ForegroundColor Green
python test_imports.py

Write-Host "`nInstallation complete with spaCy support!" -ForegroundColor Green
Write-Host "You can now run: python clustering.py" -ForegroundColor Green
Read-Host "Press Enter to continue"

