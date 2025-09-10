import logging
import os
from fastapi import FastAPI, Depends, HTTPException, APIRouter  # Add APIRouter here
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import routers using absolute imports
from routers import stats, topic_analysis

# Import auth router
from auth.router import router as auth_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app startup and shutdown events"""
    # Startup
    logger.info("Starting up the API...")
    try:
        # Add startup logic (database connections, model loading, etc.)
        logger.info("API startup completed successfully")
        yield
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")
        raise
    finally:
        # Shutdown
        logger.info("Shutting down the API...")
        logger.info("API shutdown completed")

# Create FastAPI application
app = FastAPI(
    title="Multi-Channel Analytics API",
    description="API for retrieving statistics and topic analysis from MongoDB collections (email, chat, ticket, twitter, voice)",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Modify this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create public router for endpoints that don't need authentication
public_router = APIRouter(tags=["public"])

@public_router.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "status": "online",
        "message": "Welcome to the Multi-Channel Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "home_stats": "/api/home/stats?data_type={email|chat|ticket|twitter|voice}",
            "cluster_options": "/api/topic-analysis/clusters?data_type={email|chat|ticket|twitter|voice}",
            "topic_documents": "/api/topic-analysis/documents?data_type={email|chat|ticket|twitter|voice}",
            "health_check": "/health",
            "docs": "/docs",
            "redoc": "/redoc"
        }
    }

@public_router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "API is running"
    }

# Include routers
app.include_router(public_router)  # Public router for basic endpoints
app.include_router(auth_router, prefix="/api")  # Auth router
app.include_router(stats.router, prefix="/api/v1")  # Stats router with version prefix
app.include_router(topic_analysis.router, prefix="/api")  # Topic analysis router

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled exceptions"""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return HTTPException(
        status_code=500,
        detail="Internal server error occurred"
    )

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", 8000)),
        reload=bool(os.getenv("API_RELOAD", True)),
        workers=int(os.getenv("API_WORKERS", 1)),
        log_level=os.getenv("LOG_LEVEL", "info")
    )