# Google Cloud Run Deployment Script for Windows PowerShell
# This script deploys the FastAPI backend to Google Cloud Run

# Get project ID from gcloud config
$PROJECT_ID = gcloud config get-value project
$REGION = "us-central1"
$SERVICE_NAME = "clariversev1"
$IMAGE_NAME = "gcr.io/$PROJECT_ID/$SERVICE_NAME"

# Colors for output
$RED = "`e[0;31m"
$GREEN = "`e[0;32m"
$YELLOW = "`e[1;33m"
$NC = "`e[0m" # No Color

Write-Host "$GREEN🚀 Starting Google Cloud Run Deployment$NC"
Write-Host "=========================================="

# Check if gcloud is installed
if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    Write-Host "$RED❌ Google Cloud SDK is not installed. Please install it first.$NC"
    Write-Host "Visit: https://cloud.google.com/sdk/docs/install"
    exit 1
}

# Check if docker is installed
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "$RED❌ Docker is not installed. Please install it first.$NC"
    exit 1
}

# Check if project is set
if (-not $PROJECT_ID) {
    Write-Host "$RED❌ No Google Cloud project configured. Please run: gcloud config set project YOUR_PROJECT_ID$NC"
    exit 1
}

Write-Host "$YELLOW📋 Using Google Cloud project: $PROJECT_ID$NC"

# Enable required APIs
Write-Host "$YELLOW🔧 Enabling required Google Cloud APIs...$NC"
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com

# Build and push the Docker image
Write-Host "$YELLOW🐳 Building and pushing Docker image...$NC"
docker build -t $IMAGE_NAME .
docker push $IMAGE_NAME

# Deploy to Cloud Run
Write-Host "$YELLOW🚀 Deploying to Cloud Run...$NC"
gcloud run deploy $SERVICE_NAME `
    --image $IMAGE_NAME `
    --platform managed `
    --region $REGION `
    --allow-unauthenticated `
    --port 8080 `
    --memory 1Gi `
    --cpu 1 `
    --max-instances 10 `
    --timeout 300 `
    --concurrency 80 `
    --set-env-vars "MONGO_CONNECTION_STRING=mongodb://ranjith:Ranjith@34.68.23.71:27017/admin,MONGO_DATABASE_NAME=sparzaai"

# Set IAM permissions to allow public access
Write-Host "$YELLOW🔐 Setting IAM permissions for public access...$NC"
gcloud run services add-iam-policy-binding $SERVICE_NAME `
    --region=$REGION `
    --member="allUsers" `
    --role="roles/run.invoker"

# Get the service URL
$SERVICE_URL = gcloud run services describe $SERVICE_NAME --region=$REGION --format='value(status.url)'

Write-Host "$GREEN✅ Deployment completed successfully!$NC"
Write-Host "=========================================="
Write-Host "$GREEN🌐 Service URL: $SERVICE_URL$NC"
Write-Host "$GREEN📊 Health Check: $SERVICE_URL/health$NC"
Write-Host "$GREEN📚 API Documentation: $SERVICE_URL/docs$NC"
Write-Host "$GREEN🔍 Interactive API Docs: $SERVICE_URL/redoc$NC"
Write-Host ""
Write-Host "$YELLOW📝 Next steps:$NC"
Write-Host "1. Update your frontend API base URL to: $SERVICE_URL"
Write-Host "2. Test the API endpoints"
Write-Host "3. Configure CORS if needed"
Write-Host ""
Write-Host "$YELLOW🔧 To update frontend configuration:$NC"
Write-Host "Update frontend/next.config.ts and frontend/lib/authOptions.ts with:"
Write-Host "NEXT_PUBLIC_API_URL=$SERVICE_URL"
