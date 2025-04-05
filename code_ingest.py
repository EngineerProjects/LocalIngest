#!/usr/bin/env python3
"""
Code Ingest CLI Tool

A simple standalone script to analyze a directory structure and create a Markdown report
without any web server or API requirements.

Usage:
    python code_ingest.py /path/to/project --exclude .git,.venv,__pycache__ --max-size 80 --output project_report.md
"""

import os
import re
import sys
import argparse
from pathlib import Path
import time

# Default patterns to exclude
DEFAULT_EXCLUDES = [
    r'\.git',
    r'\.mypy_cache',
    r'\.ruff_cache',
    r'\.venv',
    r'__pycache__',
    r'uv\.lock',
    r'node_modules',
    r'\.idea',
    r'\.vscode',
    r'\.DS_Store',
    r'\.env',
    r'\.pytest_cache',
]

# Estimated tokens per character (rough approximation)
TOKENS_PER_CHAR = 0.25

def count_tokens(content):
    """Estimate token count based on character count"""
    return len(content) * TOKENS_PER_CHAR

def is_excluded(path, exclude_patterns):
    """Check if a path should be excluded based on patterns"""
    for pattern in exclude_patterns:
        if re.search(pattern, str(path)):
            return True
    return False

def is_binary(file_path):
    """Check if file is likely binary"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            f.read(1024)  # Try to read as text
        return False
    except UnicodeDecodeError:
        return True
    except Exception:
        # Any other error, like permission issues
        return True

def print_progress(current, total, prefix='Progress:', suffix='Complete', length=50):
    """Display a progress bar"""
    percent = int(100 * (current / float(total)))
    filled_length = int(length * current // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    sys.stdout.write(f'\r{prefix} |{bar}| {percent}% {suffix}')
    sys.stdout.flush()
    if current == total:
        sys.stdout.write('\n')

def analyze_directory(root_path, exclude_patterns=None, max_file_size_kb=50, show_progress=True):
    """Analyze a directory and return its structure and file contents"""
    print(f"Analyzing directory: {root_path}")
    start_time = time.time()
    
    if exclude_patterns is None:
        exclude_patterns = DEFAULT_EXCLUDES

    # Convert max file size to bytes
    max_file_size = max_file_size_kb * 1024
    
    root_path = Path(root_path)
    file_contents = []
    directory_structure = []
    analyzed_files = 0
    total_tokens = 0
    
    # Validate the directory exists
    if not root_path.exists() or not root_path.is_dir():
        print(f"Error: Directory not found: {root_path}")
        return None

    # First count total files for progress tracking
    if show_progress:
        print("Counting files...")
        total_files = 0
        for root, _, files in os.walk(root_path):
            for file in files:
                rel_path = os.path.join(os.path.relpath(root, start=root_path), file)
                if not is_excluded(rel_path, exclude_patterns):
                    total_files += 1
        print(f"Found {total_files} files to analyze")
    
    # Build the directory structure and collect file contents
    def build_tree(directory, parent_path=""):
        nonlocal analyzed_files
        
        nodes = []
        try:
            items = sorted(directory.iterdir(), key=lambda x: (not x.is_dir(), x.name))
        except PermissionError:
            # Skip directories we can't access
            return []
        
        for item in items:
            relative_path = os.path.join(parent_path, item.name)
            
            if is_excluded(relative_path, exclude_patterns):
                continue
                
            if item.is_dir():
                children = build_tree(item, relative_path)
                if children:  # Only add directories with contents
                    nodes.append({
                        "name": item.name,
                        "path": relative_path,
                        "type": "directory",
                        "children": children
                    })
            else:
                # Skip files larger than max size
                try:
                    if item.stat().st_size > max_file_size:
                        continue
                    
                    # Skip binary files
                    if is_binary(item):
                        continue
                    
                    analyzed_files += 1
                    if show_progress and total_files > 0:
                        print_progress(analyzed_files, total_files, 
                                      prefix=f'Analyzing files ({analyzed_files}/{total_files}):', 
                                      suffix='Complete')
                    
                    try:
                        with open(item, 'r', encoding='utf-8', errors='replace') as f:
                            content = f.read()
                            
                        nonlocal total_tokens
                        file_token_count = count_tokens(content)
                        total_tokens += file_token_count
                        
                        # Add file content
                        file_contents.append({
                            "path": relative_path,
                            "content": content,
                            "size": item.stat().st_size,
                            "tokens": int(file_token_count)
                        })
                        
                        # Add file to tree
                        nodes.append({
                            "name": item.name,
                            "path": relative_path,
                            "type": "file",
                            "size": item.stat().st_size
                        })
                    except Exception as e:
                        print(f"\nError reading {item}: {e}")
                except Exception as e:
                    print(f"\nError processing {item}: {e}")
        
        return nodes
    
    try:
        directory_structure = build_tree(root_path)
    except Exception as e:
        print(f"Error analyzing directory: {e}")
        return None
    
    # Extract repository and branch info from path
    repo_name = root_path.name
    branch = "main"  # Default value
    
    # Try to get actual branch name if .git exists
    git_head_path = root_path / ".git" / "HEAD"
    if git_head_path.exists():
        try:
            with open(git_head_path, 'r') as f:
                head_content = f.read().strip()
                if head_content.startswith("ref: refs/heads/"):
                    branch = head_content.split("/")[-1]
        except:
            pass
    
    end_time = time.time()
    
    return {
        "repository": str(root_path),
        "branch": branch,
        "files_analyzed": analyzed_files,
        "estimated_tokens": int(total_tokens),
        "directory_structure": directory_structure,
        "file_contents": file_contents,
        "analysis_time": end_time - start_time
    }

def generate_markdown(data):
    """Generate a markdown representation of the analyzed directory"""
    if not data:
        return "ERROR: No data to generate report from."
    
    result = []
    
    # Add summary section
    result.append("# Project Summary\n")
    result.append(f"Repository: {data['repository']}\n")
    result.append(f"Branch: {data['branch']}\n")
    result.append(f"Files analyzed: {data['files_analyzed']}\n")
    result.append(f"Estimated tokens: {(data['estimated_tokens'] / 1000):.1f}k\n")
    result.append(f"Analysis time: {data['analysis_time']:.2f} seconds\n\n")
    
    # Add directory structure section
    result.append("# Directory Structure\n\n```\n")
    
    def print_tree(node, indent=""):
        tree_text = ""
        for i, item in enumerate(node):
            is_last = i == len(node) - 1
            prefix = "└── " if is_last else "├── "
            tree_text += f"{indent}{prefix}{item['name']}\n"
            
            if 'children' in item:
                next_indent = indent + ("    " if is_last else "│   ")
                tree_text += print_tree(item['children'], next_indent)
        return tree_text
    
    result.append(print_tree(data['directory_structure']))
    result.append("```\n\n")
    
    # Add file contents section
    result.append("# Files Content\n\n")
    for file_data in data['file_contents']:
        result.append(f"## {file_data['path']}\n\n")
        
        # Determine if we should use code blocks (based on file extension)
        ext = os.path.splitext(file_data['path'])[1]
        if ext:
            result.append(f"```{ext[1:]}\n{file_data['content']}\n```\n\n")
        else:
            result.append(f"```\n{file_data['content']}\n```\n\n")
    
    return "".join(result)

def main():
    """Main entry point for the CLI tool"""
    parser = argparse.ArgumentParser(description='Code Ingest Tool - Analyze directory and create Markdown report')
    parser.add_argument('directory', help='Directory to analyze')
    parser.add_argument('--exclude', default=','.join(DEFAULT_EXCLUDES), 
                      help='Comma-separated list of patterns to exclude (default: git,venv,etc.)')
    parser.add_argument('--max-size', type=int, default=50,
                      help='Maximum file size in KB to include (default: 50)')
    parser.add_argument('--output', default=None,
                      help='Output file name (default: <directory_name>_ingest.md)')
    parser.add_argument('--no-progress', action='store_true',
                      help='Disable progress display')
    
    args = parser.parse_args()
    
    # Process exclude patterns
    exclude_patterns = []
    if args.exclude:
        exclude_patterns = [p.strip() for p in args.exclude.split(',') if p.strip()]
    
    # Set default output filename if not provided
    if not args.output:
        dir_name = os.path.basename(os.path.normpath(args.directory))
        args.output = f"{dir_name}_ingest.md"
    
    # Run the analysis
    result = analyze_directory(
        args.directory, 
        exclude_patterns=exclude_patterns, 
        max_file_size_kb=args.max_size,
        show_progress=not args.no_progress
    )
    
    if not result:
        print("Analysis failed. Exiting.")
        return 1
    
    # Generate the report
    markdown_content = generate_markdown(result)
    
    # Save to file
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    
    print(f"\nReport generated successfully: {args.output}")
    print(f"- Files analyzed: {result['files_analyzed']}")
    print(f"- Estimated tokens: {(result['estimated_tokens'] / 1000):.1f}k")
    print(f"- Analysis time: {result['analysis_time']:.2f} seconds")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())