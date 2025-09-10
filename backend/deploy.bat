@echo off
REM Google Cloud Run Deployment Script for Windows
REM This script deploys the FastAPI backend to Google Cloud Run

setlocal enabledelayedexpansion

REM Get project ID from gcloud config
for /f "tokens=*" %%i in ('gcloud config get-value project') do set PROJECT_ID=%%i
set REGION=us-central1
set SERVICE_NAME=topic-analysis-api
set IMAGE_NAME=gcr.io/%PROJECT_ID%/%SERVICE_NAME%

echo 🚀 Starting Google Cloud Run Deployment
echo ==========================================

REM Check if gcloud is installed
where gcloud >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Google Cloud SDK is not installed. Please install it first.
    echo Visit: https://cloud.google.com/sdk/docs/install
    exit /b 1
)

REM Check if docker is installed
where docker >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not installed. Please install it first.
    exit /b 1
)

REM Check if project is set
if "%PROJECT_ID%"=="" (
    echo ❌ No Google Cloud project configured. Please run: gcloud config set project YOUR_PROJECT_ID
    exit /b 1
)

echo 📋 Using Google Cloud project: %PROJECT_ID%

REM Enable required APIs
echo 🔧 Enabling required Google Cloud APIs...
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com

REM Build and push the Docker image
echo 🐳 Building and pushing Docker image...
docker build -t %IMAGE_NAME% .
docker push %IMAGE_NAME%

REM Deploy to Cloud Run
echo 🚀 Deploying to Cloud Run...
gcloud run deploy %SERVICE_NAME% ^
    --image %IMAGE_NAME% ^
    --platform managed ^
    --region %REGION% ^
    --allow-unauthenticated ^
    --port 8080 ^
    --memory 1Gi ^
    --cpu 1 ^
    --max-instances 10 ^
    --timeout 300 ^
    --concurrency 80 ^
    --set-env-vars MONGO_CONNECTION_STRING="mongodb://ranjith:Ranjith@34.68.23.71:27017/admin",MONGO_DATABASE_NAME="sparzaai"

REM Get the service URL
for /f "tokens=*" %%i in ('gcloud run services describe %SERVICE_NAME% --region=%REGION% --format="value(status.url)"') do set SERVICE_URL=%%i

echo ✅ Deployment completed successfully!
echo ==========================================
echo 🌐 Service URL: %SERVICE_URL%
echo 📊 Health Check: %SERVICE_URL%/health
echo 📚 API Documentation: %SERVICE_URL%/docs
echo 🔍 Interactive API Docs: %SERVICE_URL%/redoc
echo.
echo 📝 Next steps:
echo 1. Update your frontend API base URL to: %SERVICE_URL%
echo 2. Test the API endpoints
echo 3. Configure CORS if needed

pause
