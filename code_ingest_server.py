#!/usr/bin/env python3
"""
Code Ingest Server - FastAPI Implementation

A modern HTTP server for the Code Ingest Web UI.
Handles file operations and running the code ingest tool.

Usage:
    uvicorn code_ingest_server:app --reload --port 8000
"""

import os
import json
import sys
import uuid
import threading
from typing import Dict, List, Optional, Any
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# Add the parent directory to the path so we can import code_ingest
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from code_ingest import code_ingest
except ImportError:
    # If we're running from the web directory
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from code_ingest import code_ingest

# Store for ongoing analysis jobs
analysis_jobs = {}

# Create the FastAPI app
app = FastAPI(
    title="Code Ingest API",
    description="API for analyzing code repositories and generating reports",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update this for production to specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define Pydantic models for request and response validation
class DirectoryValidateRequest(BaseModel):
    path: str = Field(..., description="Path to the directory to validate")

class DirectoryScanRequest(BaseModel):
    path: str = Field(..., description="Path to the directory to scan")

class AnalysisRequest(BaseModel):
    directory: str = Field(..., description="Path to the directory to analyze")
    exclude_patterns: List[str] = Field(default=[], description="Patterns to exclude")
    include_patterns: List[str] = Field(default=[], description="Patterns to include")
    include_subfolders: List[str] = Field(default=[], description="Specific subfolders to include")
    exclude_root_files: bool = Field(default=False, description="Whether to exclude files in the root directory")
    max_file_size_kb: int = Field(default=50, description="Maximum file size in KB to include")

class SaveMarkdownRequest(BaseModel):
    job_id: str = Field(..., description="ID of the analysis job")
    output_path: str = Field(..., description="Path to save the markdown file")

class DirectoryValidateResponse(BaseModel):
    exists: bool
    is_directory: bool
    name: str
    full_path: str
    approximate_file_count: Any  # Can be int or string like "100+"
    branch: str

class DefaultExcludesResponse(BaseModel):
    exclude_patterns: List[str]

class DirectoryScanResponse(BaseModel):
    directory_structure: List[Dict[str, Any]]

class AnalysisJobResponse(BaseModel):
    job_id: str

class AnalysisStatusResponse(BaseModel):
    id: str
    status: str
    progress: float
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None

class MarkdownResponse(BaseModel):
    markdown_content: str

class SaveMarkdownResponse(BaseModel):
    success: bool
    path: str

# Define API routes
@app.post("/api/validate-directory", response_model=DirectoryValidateResponse)
async def validate_directory(request: DirectoryValidateRequest):
    """Validate if a directory exists and is accessible"""
    path = Path(request.path)
    
    if not path.exists():
        raise HTTPException(status_code=404, detail="Directory not found")
    
    if not path.is_dir():
        raise HTTPException(status_code=400, detail="Path is not a directory")
    
    # Get basic information about the directory
    file_count = sum(1 for _ in path.glob("**/*") if _.is_file())
    if file_count > 100:
        file_count = "100+"
        
    branch = "Unknown"
    try:
        branch = code_ingest.get_git_branch(path)
    except:
        pass
    
    return {
        "exists": True,
        "is_directory": True,
        "name": path.name,
        "full_path": str(path.absolute()),
        "approximate_file_count": file_count,
        "branch": branch
    }

@app.get("/api/default-excludes", response_model=DefaultExcludesResponse)
async def default_excludes():
    """Return the default exclude patterns"""
    return {
        "exclude_patterns": code_ingest.DEFAULT_EXCLUDES
    }

@app.post("/api/scan-structure", response_model=DirectoryScanResponse)
async def scan_structure(request: DirectoryScanRequest):
    """Scan directory structure without full ingestion"""
    path = Path(request.path)
    
    if not path.exists() or not path.is_dir():
        raise HTTPException(status_code=404, detail="Directory not found or not a directory")
    
    try:
        # Check if code_ingest has get_full_directory_structure method
        if hasattr(code_ingest, "get_full_directory_structure"):
            structures = code_ingest.get_full_directory_structure([str(path)])
        else:
            # Simulate what the original code likely did by analyzing the directory
            # but only returning the structure, not the content
            result = code_ingest.analyze_multiple_directories(
                [str(path)],
                max_file_size_kb=1,  # Small size to avoid content processing
                show_progress=False
            )
            structures = result.get("directory_structure", [])
        
        return {
            "directory_structure": structures
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/analyze", response_model=AnalysisJobResponse)
async def analyze(request: AnalysisRequest, background_tasks: BackgroundTasks):
    """Start an analysis job"""
    job_id = str(uuid.uuid4())
    
    # Create a new analysis job entry
    analysis_jobs[job_id] = {
        "id": job_id,
        "status": "running",
        "progress": 0.0,
        "result": None,
        "error": None
    }
    
    # Function to run analysis in background
    def run_analysis():
        try:
            # Update progress
            analysis_jobs[job_id]["progress"] = 0.1
            
            # Validate the directory
            path = Path(request.directory)
            if not path.exists() or not path.is_dir():
                analysis_jobs[job_id]["status"] = "failed"
                analysis_jobs[job_id]["error"] = "Directory not found or not a directory"
                return
            
            # Run the analysis
            analysis_jobs[job_id]["progress"] = 0.2
            
            result = code_ingest.analyze_multiple_directories(
                [request.directory],
                exclude_patterns=request.exclude_patterns,
                include_patterns=request.include_patterns,
                include_subfolders=request.include_subfolders,
                max_file_size_kb=request.max_file_size_kb,
                include_root_files=not request.exclude_root_files,
                show_progress=False
            )
            
            analysis_jobs[job_id]["progress"] = 0.9
            
            if result:
                # Generate markdown
                markdown_content = code_ingest.generate_markdown(result)
                
                # Store the result
                analysis_jobs[job_id]["result"] = {
                    "summary": {
                        "repository": result["repository"],
                        "branch": result["branch"],
                        "files_analyzed": result["files_analyzed"],
                        "estimated_tokens": result["estimated_tokens"],
                        "analysis_time": result["analysis_time"]
                    },
                    "directory_structure": result["directory_structure"],
                    "file_contents": result["file_contents"],
                    "ingested_files": result["ingested_files"],
                    "markdown_content": markdown_content
                }
                analysis_jobs[job_id]["status"] = "completed"
                analysis_jobs[job_id]["progress"] = 1.0
            else:
                analysis_jobs[job_id]["status"] = "failed"
                analysis_jobs[job_id]["error"] = "Analysis failed"
        except Exception as e:
            analysis_jobs[job_id]["status"] = "failed"
            analysis_jobs[job_id]["error"] = str(e)
    
    # Add the analysis task to run in the background
    background_tasks.add_task(run_analysis)
    
    # Return the job ID
    return {"job_id": job_id}

@app.get("/api/analysis/{job_id}", response_model=AnalysisStatusResponse)
async def analysis_status(job_id: str):
    """Get the status of an analysis job"""
    if job_id not in analysis_jobs:
        raise HTTPException(status_code=404, detail="Analysis job not found")
    
    job = analysis_jobs[job_id]
    
    response = {
        "id": job["id"],
        "status": job["status"],
        "progress": job["progress"]
    }
    
    if job["error"]:
        response["error"] = job["error"]
    
    if job["result"] and job["status"] == "completed":
        response["result"] = {
            "summary": job["result"]["summary"],
            # Include a truncated version of directory structure
            "directory_structure": job["result"]["directory_structure"]
        }
    
    return response

@app.get("/api/analysis/{job_id}/full")
async def full_result(job_id: str):
    """Get the full result of a completed analysis job"""
    if job_id not in analysis_jobs:
        raise HTTPException(status_code=404, detail="Analysis job not found")
    
    job = analysis_jobs[job_id]
    
    if job["status"] != "completed" or not job["result"]:
        raise HTTPException(status_code=400, detail="Analysis job not completed")
    
    # Return the full result
    return job["result"]

@app.get("/api/analysis/{job_id}/markdown", response_model=MarkdownResponse)
async def markdown(job_id: str):
    """Get the markdown content of a completed analysis job"""
    if job_id not in analysis_jobs:
        raise HTTPException(status_code=404, detail="Analysis job not found")
    
    job = analysis_jobs[job_id]
    
    if job["status"] != "completed" or not job["result"] or "markdown_content" not in job["result"]:
        raise HTTPException(status_code=400, detail="Markdown content not available")
    
    return {"markdown_content": job["result"]["markdown_content"]}

@app.post("/api/save-markdown", response_model=SaveMarkdownResponse)
async def save_markdown(request: SaveMarkdownRequest):
    """Save the markdown content to a file"""
    if request.job_id not in analysis_jobs:
        raise HTTPException(status_code=404, detail="Analysis job not found")
    
    job = analysis_jobs[request.job_id]
    
    if job["status"] != "completed" or not job["result"] or "markdown_content" not in job["result"]:
        raise HTTPException(status_code=400, detail="Markdown content not available")
    
    try:
        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(request.output_path), exist_ok=True)
        
        # Write the markdown content to the file
        with open(request.output_path, "w", encoding="utf-8") as f:
            f.write(job["result"]["markdown_content"])
        
        return {"success": True, "path": request.output_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Mount static files
try:
    app.mount("/", StaticFiles(directory="web", html=True), name="static")
except Exception as e:
    print(f"Warning: Could not mount static files: {e}")
    print("Frontend files should be placed in a 'web' directory relative to this script.")

# Main entry point
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)