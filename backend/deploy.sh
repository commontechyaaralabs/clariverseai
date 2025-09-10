#!/bin/bash

# Google Cloud Run Deployment Script
# This script deploys the FastAPI backend to Google Cloud Run

set -e

# Get project ID from gcloud config
PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
SERVICE_NAME="clariversev1"
IMAGE_NAME="gcr.io/$PROJECT_ID/$SERVICE_NAME"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Google Cloud Run Deployment${NC}"
echo "=========================================="

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}❌ Google Cloud SDK is not installed. Please install it first.${NC}"
    echo "Visit: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed. Please install it first.${NC}"
    exit 1
fi

# Check if project is set
if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}❌ No Google Cloud project configured. Please run: gcloud config set project YOUR_PROJECT_ID${NC}"
    exit 1
fi

echo -e "${YELLOW}📋 Using Google Cloud project: $PROJECT_ID${NC}"

# Enable required APIs
echo -e "${YELLOW}🔧 Enabling required Google Cloud APIs...${NC}"
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com

# Build and push the Docker image
echo -e "${YELLOW}🐳 Building and pushing Docker image...${NC}"
docker build -t $IMAGE_NAME .
docker push $IMAGE_NAME

# Deploy to Cloud Run
echo -e "${YELLOW}🚀 Deploying to Cloud Run...${NC}"
gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_NAME \
    --platform managed \
    --region $REGION \
    --allow-unauthenticated \
    --port 8080 \
    --memory 1Gi \
    --cpu 1 \
    --max-instances 10 \
    --timeout 300 \
    --concurrency 80 \
    --set-env-vars "MONGO_CONNECTION_STRING=mongodb://ranjith:Ranjith@34.68.23.71:27017/admin,MONGO_DATABASE_NAME=sparzaai"

# Set IAM permissions to allow public access
echo -e "${YELLOW}🔐 Setting IAM permissions for public access...${NC}"
gcloud run services add-iam-policy-binding $SERVICE_NAME \
    --region=$REGION \
    --member="allUsers" \
    --role="roles/run.invoker"

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --format='value(status.url)')

echo -e "${GREEN}✅ Deployment completed successfully!${NC}"
echo "=========================================="
echo -e "${GREEN}🌐 Service URL: $SERVICE_URL${NC}"
echo -e "${GREEN}📊 Health Check: $SERVICE_URL/health${NC}"
echo -e "${GREEN}📚 API Documentation: $SERVICE_URL/docs${NC}"
echo -e "${GREEN}🔍 Interactive API Docs: $SERVICE_URL/redoc${NC}"
echo ""
echo -e "${YELLOW}📝 Next steps:${NC}"
echo "1. Update your frontend API base URL to: $SERVICE_URL"
echo "2. Test the API endpoints"
echo "3. Configure CORS if needed"
echo ""
echo -e "${YELLOW}🔧 To update frontend configuration:${NC}"
echo "Update frontend/next.config.ts and frontend/lib/authOptions.ts with:"
echo "NEXT_PUBLIC_API_URL=$SERVICE_URL"
