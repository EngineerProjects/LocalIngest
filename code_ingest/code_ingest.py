#!/usr/bin/env python3
"""
Enhanced Code Ingest Module

A standalone script to analyze directory structures and create Markdown reports
with support for multiple directories, subfolder inclusion, and more detailed visualization.

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
    r'\.git',                 # Git version control
    r'\.hg',                  # Mercurial
    r'\.svn',                 # Subversion
    r'\.mypy_cache',          # mypy type checker
    r'\.ruff_cache',          # ruff linter
    r'\.venv',                # virtualenv
    r'\.env',                 # alternative virtualenv
    r'__pycache__',           # Python bytecode
    r'\.pytest_cache',        # pytest cache
    r'\.coverage',            # coverage.py data file
    r'\.tox',                 # tox environments
    r'\.nox',                 # nox environments
    r'\.eggs',                # setuptools
    r'\.egg-info',            # setuptools
    r'\.vscode',              # VSCode config
    r'\.idea',                # JetBrains IDEs
    r'\.DS_Store',            # macOS finder metadata
    r'node_modules',          # Node.js packages
    r'dist',                  # Python/JS built distributions
    r'build',                 # Python/JS/C build artifacts
    r'\.parcel-cache',        # Parcel bundler
    r'\.next',                # Next.js build output
    r'\.turbo',               # Turbo repo cache
    r'\.svelte-kit',          # SvelteKit
    r'\.cache',               # Various tools (npm, etc.)
    r'\.coverage.*',          # coverage reports
    r'\.yarn-cache',          # Yarn
    r'\.pnp',                 # Yarn Plug'n'Play
    r'\.gradle',              # Gradle
    r'\.ccls-cache',          # CCLS (C++ LSP)
    r'target',                # Rust & other builds
    r'Cargo.lock',            # Rust lock file
    r'package-lock\.json',    # npm lock file
    r'yarn\.lock',            # yarn lock file
    r'pnpm-lock\.yaml',       # pnpm lock file
    r'Pipfile\.lock',         # pipenv lock file
    r'uv\.lock',              # uv lock file
    r'\.sqlite3?',            # local dbs
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

def is_included(path, include_patterns):
    """Check if a path should be included based on patterns"""
    # If no include patterns are specified, include everything
    if not include_patterns:
        return True
    
    # Check if path matches any include pattern
    for pattern in include_patterns:
        if re.search(pattern, str(path)):
            return True
    
    # If include patterns exist but none match, exclude the path
    return False

def should_process_path(path, include_patterns, exclude_patterns):
    """Determine if a path should be processed based on include and exclude patterns"""
    # First check include patterns
    if not is_included(path, include_patterns):
        return False
    
    # Then check exclude patterns
    if is_excluded(path, exclude_patterns):
        return False
    
    return True

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

def get_git_branch(directory):
    """
    Get the current git branch for a repository
    
    Args:
        directory (str): Path to the repository
        
    Returns:
        str: Branch name or None if not a git repository
    """
    # Try to get actual branch name if .git exists
    git_head_path = Path(directory) / ".git" / "HEAD"
    if git_head_path.exists():
        try:
            with open(git_head_path, 'r') as f:
                head_content = f.read().strip()
                if head_content.startswith("ref: refs/heads/"):
                    return head_content.split("/")[-1]
        except:
            pass
    
    # Try using git command if available
    try:
        import subprocess
        cmd = ['git', '-C', directory, 'branch', '--show-current']
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        
        # If the above command fails or returns empty (detached HEAD),
        # try to get the branch from the HEAD reference
        cmd = ['git', '-C', directory, 'rev-parse', '--abbrev-ref', 'HEAD']
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        
        if result.returncode == 0 and result.stdout.strip():
            branch = result.stdout.strip()
            return branch if branch != "HEAD" else "detached HEAD"
    except Exception:
        pass
    
    return "main"  # Default value

def get_full_directory_structure(directories):
    """Get directory structure without analyzing file contents"""
    structures = []
    
    for directory in directories:
        path = Path(directory)
        
        def build_tree(dir_path, parent_path=""):
            nodes = []
            try:
                items = sorted(dir_path.iterdir(), key=lambda x: (not x.is_dir(), x.name))
            except PermissionError:
                return []
            
            for item in items:
                relative_path = os.path.join(parent_path, item.name)
                
                if item.is_dir():
                    children = build_tree(item, relative_path)
                    nodes.append({
                        "name": item.name,
                        "path": relative_path,
                        "type": "directory",
                        "children": children,
                        "is_included": True
                    })
                else:
                    nodes.append({
                        "name": item.name,
                        "path": relative_path,
                        "type": "file",
                        "size": item.stat().st_size if os.access(item, os.R_OK) else 0,
                        "is_included": True
                    })
            
            return nodes
        
        structures.extend(build_tree(path))
    
    return structures

def analyze_directory(root_path, exclude_patterns=None, include_patterns=None, max_file_size_kb=50, show_progress=True):
    """
    Analyze a directory and return its structure and file contents
    
    Args:
        root_path (str): Path to directory to analyze
        exclude_patterns (list): Patterns to exclude
        include_patterns (list): Patterns to include (applied before exclude)
        max_file_size_kb (int): Maximum file size in KB
        show_progress (bool): Whether to show progress bar
        
    Returns:
        dict: Analysis results
    """
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
    ingested_files = []  # New: Track which files were actually ingested
    
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
                # Use the combined function to check patterns
                if should_process_path(rel_path, include_patterns, exclude_patterns):
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
            
            # Add file/directory to structure regardless of include/exclude patterns
            # but with a flag to indicate if it's included
            is_included_file = should_process_path(relative_path, include_patterns, exclude_patterns)
            
            if item.is_dir():
                children = build_tree(item, relative_path)
                # Only add directories that contain at least one visible item
                if children:
                    nodes.append({
                        "name": item.name,
                        "path": relative_path,
                        "type": "directory",
                        "children": children,
                        "is_included": is_included_file
                    })
            else:
                # Add file to tree, always, to show full structure
                file_node = {
                    "name": item.name,
                    "path": relative_path,
                    "type": "file",
                    "size": item.stat().st_size if os.access(item, os.R_OK) else 0,
                    "is_included": is_included_file
                }
                nodes.append(file_node)
                
                # Skip file contents based on inclusion/exclusion
                if not is_included_file:
                    continue
                
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
                        
                        # Add to list of ingested files
                        ingested_files.append(relative_path)
                        
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
    
    # Get branch info
    branch = get_git_branch(root_path)
    
    end_time = time.time()
    
    return {
        "repository": str(root_path),
        "branch": branch,
        "files_analyzed": analyzed_files,
        "estimated_tokens": int(total_tokens),
        "directory_structure": directory_structure,
        "file_contents": file_contents,
        "analysis_time": end_time - start_time,
        "ingested_files": ingested_files  # New: Include list of ingested files
    }

def analyze_multiple_directories(directories, exclude_patterns=None, include_patterns=None, 
                              include_subfolders=None, max_file_size_kb=50, 
                              include_root_files=True, show_progress=True):
    """
    Analyze multiple directories and combine results
    
    Args:
        directories (list): List of directories to analyze
        exclude_patterns (list): Patterns to exclude
        include_patterns (list): Patterns to include
        include_subfolders (list): Specific subfolders to include
        max_file_size_kb (int): Maximum file size in KB
        include_root_files (bool): Whether to include files in the root directory
        show_progress (bool): Whether to show progress
        
    Returns:
        dict: Combined analysis results
    """
    if exclude_patterns is None:
        exclude_patterns = DEFAULT_EXCLUDES.copy()
    else:
        exclude_patterns = exclude_patterns.copy()
    
    # Initialize include patterns if empty
    if include_patterns is None:
        include_patterns = []
    else:
        include_patterns = include_patterns.copy()
    
    # Add patterns for specific subfolders if provided
    if include_subfolders:
        for subfolder in include_subfolders:
            # Create pattern to match this subfolder and its contents
            subfolder_pattern = f"^{re.escape(subfolder)}(/.*)?$"
            include_patterns.append(subfolder_pattern)
            print(f"Added include pattern for subfolder: {subfolder}")
    
    # If not including root files, add exclude pattern for files in the root directory
    if not include_root_files:
        # Pattern to exclude files directly in the root directory (no path separator)
        root_files_pattern = r"^[^/\\]+$"
        exclude_patterns.append(root_files_pattern)
        print(f"Added exclude pattern for root files")
    
    # Process each directory
    all_results = []
    
    for i, directory in enumerate(directories):
        print(f"\nAnalyzing directory ({i+1}/{len(directories)}): {directory}")
        
        result = analyze_directory(
            directory,
            exclude_patterns=exclude_patterns,
            include_patterns=include_patterns,
            max_file_size_kb=max_file_size_kb,
            show_progress=show_progress
        )
        
        if result:
            all_results.append(result)
    
    # Return empty result if no directories were analyzed successfully
    if not all_results:
        return None
    
    # Create combined result
    combined_result = {
        "repository": [result["repository"] for result in all_results],
        "branch": all_results[0]["branch"],  # Use first branch for simplicity
        "files_analyzed": sum(result["files_analyzed"] for result in all_results),
        "estimated_tokens": sum(result["estimated_tokens"] for result in all_results),
        "analysis_time": sum(result["analysis_time"] for result in all_results),
        "directory_structure": [
            {
                "name": os.path.basename(result["repository"]),
                "type": "directory",
                "children": result["directory_structure"],
                "is_included": True
            }
            for result in all_results
        ],
        "file_contents": [
            file_content
            for result in all_results
            for file_content in result["file_contents"]
        ],
        "ingested_files": [
            file_path
            for result in all_results
            for file_path in result.get("ingested_files", [])
        ]
    }
    
    return combined_result

def generate_markdown(data):
    """Generate a markdown representation of the analyzed directory"""
    if not data:
        return "ERROR: No data to generate report from."
    
    result = []
    
    # Add summary section
    result.append("# Project Summary\n")
    
    # Handle multiple repositories
    if isinstance(data['repository'], list):
        result.append("## Repositories\n")
        for repo in data['repository']:
            result.append(f"- {repo}\n")
    else:
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
            
            # Add visual indicator for included/excluded files
            if item.get('is_included', True):
                name_display = item['name']
            else:
                name_display = f"{item['name']} (excluded)"
            
            tree_text += f"{indent}{prefix}{name_display}\n"
            
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
    parser.add_argument('directory', nargs='+', help='Directory/directories to analyze')
    parser.add_argument('--exclude', default=','.join(DEFAULT_EXCLUDES), 
                      help='Comma-separated list of patterns to exclude (default: git,venv,etc.)')
    parser.add_argument('--include', default="",
                      help='Comma-separated list of patterns to include')
    parser.add_argument('--include-subfolders', default="",
                      help='Comma-separated list of specific subfolders to include')
    parser.add_argument('--exclude-root', action='store_true',
                      help='Exclude files in the root directory')
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
    
    # Process include patterns
    include_patterns = []
    if args.include:
        include_patterns = [p.strip() for p in args.include.split(',') if p.strip()]
    
    # Process include subfolders
    include_subfolders = []
    if args.include_subfolders:
        include_subfolders = [f.strip() for f in args.include_subfolders.split(',') if f.strip()]
    
    # Set default output filename if not provided
    if not args.output:
        if len(args.directory) == 1:
            dir_name = os.path.basename(os.path.normpath(args.directory[0]))
        else:
            dir_name = "multi_repository"
        args.output = f"{dir_name}_ingest.md"
    
    # Run the analysis
    result = analyze_multiple_directories(
        args.directory,
        exclude_patterns=exclude_patterns,
        include_patterns=include_patterns,
        include_subfolders=include_subfolders,
        max_file_size_kb=args.max_size,
        include_root_files=not args.exclude_root,
        show_progress=not args.no_progress
    )
    
    if not result:
        print("Analysis failed. Exiting.")
        return 1
    
    # Generate the report
    markdown_content = generate_markdown(result)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
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