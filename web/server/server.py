#!/usr/bin/env python3
"""
Code Ingest Server

A minimal HTTP server for the Code Ingest Web UI.
Handles file operations and running the code ingest tool.

Usage:
    python code_ingest_server.py
"""

import os
import json
import http.server
import socketserver
import sys
import urllib.parse
from pathlib import Path
import threading
import uuid
import mimetypes
import re

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from code_ingest import code_ingest

# Store for ongoing analysis jobs
analysis_jobs = {}

# Default port
PORT = 8000

# Define the HTTP request handler
class CodeIngestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # Set the directory for static files
        self.directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
        super().__init__(*args, **kwargs)
    
    def _send_cors_headers(self):
        """Send headers for CORS support"""
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
    
    def do_OPTIONS(self):
        """Handle preflight requests"""
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()
    
    def _handle_api_validate_directory(self):
        """Validate if a directory exists"""
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")
        data = json.loads(post_data)
        
        path = Path(data.get("path", ""))
        
        if not path.exists():
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Directory not found"}).encode())
            return
        
        if not path.is_dir():
            self.send_response(400)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Path is not a directory"}).encode())
            return
        
        # Get basic information about the directory
        file_count = sum(1 for _ in path.glob("**/*") if _.is_file())
        if file_count > 100:
            file_count = "100+"
            
        branch = "Unknown"
        try:
            branch = code_ingest.get_git_branch(path)
        except:
            pass
        
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self._send_cors_headers()
        self.end_headers()
        
        response = {
            "exists": True,
            "is_directory": True,
            "name": path.name,
            "full_path": str(path.absolute()),
            "approximate_file_count": file_count,
            "branch": branch
        }
        
        self.wfile.write(json.dumps(response).encode())
    
    def _handle_api_default_excludes(self):
        """Return the default exclude patterns"""
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self._send_cors_headers()
        self.end_headers()
        
        response = {
            "exclude_patterns": code_ingest.DEFAULT_EXCLUDES
        }
        
        self.wfile.write(json.dumps(response).encode())
    
    def _handle_api_scan_structure(self):
        """Scan directory structure without full ingestion"""
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")
        data = json.loads(post_data)
        
        path = Path(data.get("path", ""))
        
        if not path.exists() or not path.is_dir():
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Directory not found or not a directory"}).encode())
            return
        
        try:
            structures = code_ingest.get_full_directory_structure([str(path)])
            
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self._send_cors_headers()
            self.end_headers()
            
            response = {
                "directory_structure": structures
            }
            
            self.wfile.write(json.dumps(response).encode())
        except Exception as e:
            self.send_response(500)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())
    
    def _handle_api_analyze(self):
        """Start an analysis job"""
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")
        options = json.loads(post_data)
        
        job_id = str(uuid.uuid4())
        
        # Create a new analysis job entry
        analysis_jobs[job_id] = {
            "id": job_id,
            "status": "running",
            "progress": 0.0,
            "result": None,
            "error": None
        }
        
        # Start analysis in a background thread
        def run_analysis():
            try:
                # Update progress
                analysis_jobs[job_id]["progress"] = 0.1
                
                # Validate the directory
                path = Path(options["directory"])
                if not path.exists() or not path.is_dir():
                    analysis_jobs[job_id]["status"] = "failed"
                    analysis_jobs[job_id]["error"] = "Directory not found or not a directory"
                    return
                
                # Run the analysis
                analysis_jobs[job_id]["progress"] = 0.2
                
                exclude_patterns = options.get("exclude_patterns", [])
                include_patterns = options.get("include_patterns", [])
                include_subfolders = options.get("include_subfolders", [])
                max_file_size_kb = options.get("max_file_size_kb", 50)
                include_root_files = not options.get("exclude_root_files", False)
                
                result = code_ingest.analyze_multiple_directories(
                    [options["directory"]],
                    exclude_patterns=exclude_patterns,
                    include_patterns=include_patterns,
                    include_subfolders=include_subfolders,
                    max_file_size_kb=max_file_size_kb,
                    include_root_files=include_root_files,
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
        
        # Start the background thread
        thread = threading.Thread(target=run_analysis)
        thread.daemon = True
        thread.start()
        
        # Return the job ID
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self._send_cors_headers()
        self.end_headers()
        
        response = {"job_id": job_id}
        self.wfile.write(json.dumps(response).encode())
    
    def _handle_api_analysis_status(self, job_id):
        """Get the status of an analysis job"""
        if job_id not in analysis_jobs:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Analysis job not found"}).encode())
            return
        
        job = analysis_jobs[job_id]
        
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self._send_cors_headers()
        self.end_headers()
        
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
        
        self.wfile.write(json.dumps(response).encode())
    
    def _handle_api_full_result(self, job_id):
        """Get the full result of a completed analysis job"""
        if job_id not in analysis_jobs:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Analysis job not found"}).encode())
            return
        
        job = analysis_jobs[job_id]
        
        if job["status"] != "completed" or not job["result"]:
            self.send_response(400)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Analysis job not completed"}).encode())
            return
        
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self._send_cors_headers()
        self.end_headers()
        
        self.wfile.write(json.dumps(job["result"]).encode())
    
    def _handle_api_markdown(self, job_id):
        """Get the markdown content of a completed analysis job"""
        if job_id not in analysis_jobs:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Analysis job not found"}).encode())
            return
        
        job = analysis_jobs[job_id]
        
        if job["status"] != "completed" or not job["result"] or "markdown_content" not in job["result"]:
            self.send_response(400)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Markdown content not available"}).encode())
            return
        
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self._send_cors_headers()
        self.end_headers()
        
        response = {"markdown_content": job["result"]["markdown_content"]}
        self.wfile.write(json.dumps(response).encode())
    
    def _handle_api_save_markdown(self):
        """Save the markdown content to a file"""
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")
        data = json.loads(post_data)
        
        job_id = data.get("job_id")
        output_path = data.get("output_path")
        
        if not job_id or not output_path:
            self.send_response(400)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Missing job_id or output_path"}).encode())
            return
        
        if job_id not in analysis_jobs:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Analysis job not found"}).encode())
            return
        
        job = analysis_jobs[job_id]
        
        if job["status"] != "completed" or not job["result"] or "markdown_content" not in job["result"]:
            self.send_response(400)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Markdown content not available"}).encode())
            return
        
        try:
            # Create the output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Write the markdown content to the file
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(job["result"]["markdown_content"])
            
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self._send_cors_headers()
            self.end_headers()
            
            response = {"success": True, "path": output_path}
            self.wfile.write(json.dumps(response).encode())
        except Exception as e:
            self.send_response(500)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())
    
    def do_POST(self):
        """Handle POST requests"""
        parsed_path = urllib.parse.urlparse(self.path)
        path = parsed_path.path
        
        # API endpoints
        if path == "/api/validate-directory":
            self._handle_api_validate_directory()
        elif path == "/api/scan-structure":
            self._handle_api_scan_structure()
        elif path == "/api/analyze":
            self._handle_api_analyze()
        elif path == "/api/save-markdown":
            self._handle_api_save_markdown()
        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Endpoint not found"}).encode())
    
    def do_GET(self):
        """Handle GET requests"""
        parsed_path = urllib.parse.urlparse(self.path)
        path = parsed_path.path
        
        # API endpoints
        if path == "/api/default-excludes":
            self._handle_api_default_excludes()
        elif path.startswith("/api/analysis/"):
            parts = path.split("/")
            if len(parts) >= 4:
                job_id = parts[3]
                if len(parts) == 5 and parts[4] == "full":
                    self._handle_api_full_result(job_id)
                elif len(parts) == 5 and parts[4] == "markdown":
                    self._handle_api_markdown(job_id)
                else:
                    self._handle_api_analysis_status(job_id)
            else:
                self.send_response(404)
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid analysis endpoint"}).encode())
        else:
            # Serve static files
            if path == "/":
                path = "/index.html"
            
            # Map the URL path to the file path
            file_path = os.path.join(self.directory, path.lstrip("/"))
            
            # Check if the file exists
            if not os.path.exists(file_path) or os.path.isdir(file_path):
                # If not, serve the index.html for SPA routing
                file_path = os.path.join(self.directory, "index.html")
            
            try:
                # Determine the content type
                content_type, _ = mimetypes.guess_type(file_path)
                if content_type is None:
                    content_type = "application/octet-stream"
                
                # Open and send the file
                with open(file_path, "rb") as f:
                    self.send_response(200)
                    self.send_header("Content-type", content_type)
                    self._send_cors_headers()
                    self.end_headers()
                    self.wfile.write(f.read())
            except Exception as e:
                self.send_response(500)
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())

def run_server():
    """Run the HTTP server"""
    # Create a web directory if it doesn't exist
    web_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
    os.makedirs(web_dir, exist_ok=True)
    
    # Setup and run the server
    handler = CodeIngestHandler
    httpd = socketserver.TCPServer(("", PORT), handler)
    
    print(f"Serving at http://localhost:{PORT}")
    httpd.serve_forever()

if __name__ == "__main__":
    run_server()