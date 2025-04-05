#!/usr/bin/env python3
"""
Beautiful Terminal UI for Code Ingest (Fixed Version)

A stylish, modern terminal interface for the code ingest tool with rich formatting.
This version fixes progress bar issues and provides a more robust implementation.

Installation:
    pip install rich typer

Run:
    python terminal_code_ingest_ui.py interactive
"""

import os
import sys
import time
import threading
import importlib.util
from pathlib import Path
from typing import List, Optional
import typer
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.syntax import Syntax
from rich.tree import Tree
from rich.markdown import Markdown
from rich.prompt import Prompt, Confirm
from rich import box

# Initialize rich console
console = Console()

# Create the app
app = typer.Typer(
    help="Analyze code repositories and generate beautiful reports",
    add_completion=False
)

# Import code_ingest module
def import_code_ingest(module_path=None):
    """Import the code_ingest module from the current directory"""
    try:
        if module_path is None:
            # Try to find code_ingest.py in the same directory as this script
            module_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "code_ingest.py")
        
        # Check if file exists
        if not os.path.exists(module_path):
            console.print(f"[bold red]Error:[/] Could not find code_ingest.py at {module_path}")
            return None
            
        # Import module from file
        spec = importlib.util.spec_from_file_location("code_ingest", module_path)
        code_ingest = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(code_ingest)
        return code_ingest
    except Exception as e:
        console.print(f"[bold red]Error importing code_ingest:[/] {str(e)}")
        return None

def display_logo():
    """Display a stylish ASCII logo"""
    logo = """
    [bold cyan]╔═╗╔═╗╔╦╗╔═╗  ╦╔╗╔╔═╗╔═╗╔═╗╔╦╗[/]
    [bold cyan]║  ║ ║ ║║║╣   ║║║║║ ╦║╣ ╚═╗ ║ [/]
    [bold cyan]╚═╝╚═╝═╩╝╚═╝  ╩╝╚╝╚═╝╚═╝╚═╝ ╩ [/]
    """
    console.print(logo)
    console.print("[dim]A beautiful terminal UI for code repository analysis[/]\n")

# A simple progress tracking class that doesn't rely on Rich Progress internals
class SimpleProgressTracker:
    """Simple progress tracker that's compatible with code_ingest"""
    
    def __init__(self):
        self.current = 0
        self.total = 0
        self.last_update = 0
        self.update_interval = 0.1  # seconds
        
    def __call__(self, current, total, prefix='', suffix='', length=50):
        """Progress callback function compatible with code_ingest"""
        self.current = current
        self.total = total
        
        # Only update UI every update_interval seconds to avoid console flicker
        current_time = time.time()
        if current_time - self.last_update >= self.update_interval or current >= total:
            # Calculate percentage
            percent = int(100 * (current / float(total))) if total > 0 else 0
            
            # Create progress bar
            bar_length = length
            filled_length = int(bar_length * current // total) if total > 0 else 0
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            
            # Print progress
            console.print(
                f"\r{prefix} |{bar}| {percent}% {suffix} ({current}/{total})",
                end=""
            )
            
            # Update last update time
            self.last_update = current_time
            
            # Add newline if complete
            if current >= total:
                console.print()

def display_directory_tree(structure, title="Directory Structure"):
    """Display directory structure as a rich tree"""
    if not structure:
        console.print("[yellow]No files found in directory structure.[/]")
        return
        
    tree = Tree(f"[bold cyan]{title}[/]")
    
    def add_to_tree(node, tree_node):
        for item in node:
            if item.get('type') == 'directory':
                folder = tree_node.add(f"[bold blue]{item['name']}[/] [dim](dir)[/]")
                if 'children' in item:
                    add_to_tree(item['children'], folder)
            else:
                size_kb = item.get('size', 0) / 1024
                tree_node.add(f"[green]{item['name']}[/] [dim]({size_kb:.1f} KB)[/]")
    
    add_to_tree(structure, tree)
    
    console.print(Panel(tree, title=title, border_style="cyan", expand=False))

def display_summary(data):
    """Display a summary of the analysis results"""
    table = Table(title="Analysis Summary", box=box.ROUNDED, border_style="cyan", title_style="bold cyan")
    
    table.add_column("Metric", style="bold white")
    table.add_column("Value", style="cyan")
    
    # Add rows
    table.add_row("Repository", os.path.basename(data['repository']))
    table.add_row("Branch", data['branch'])
    table.add_row("Files Analyzed", str(data['files_analyzed']))
    table.add_row("Estimated Tokens", f"{(data['estimated_tokens'] / 1000):.1f}k")
    table.add_row("Analysis Time", f"{data['analysis_time']:.2f} seconds")
    
    console.print(table)

def display_file_preview(file_contents, max_files=3, max_lines=10):
    """Display a preview of file contents"""
    if not file_contents:
        console.print("[yellow]No file contents to display.[/]")
        return
        
    # Keep only the specified number of files for preview
    preview_files = file_contents[:max_files]
    
    console.print("\n[bold cyan]File Content Preview[/] [dim](first few files)[/]")
    
    for file_data in preview_files:
        # Get file extension for syntax highlighting
        ext = os.path.splitext(file_data['path'])[1][1:] if os.path.splitext(file_data['path'])[1] else "text"
        
        # Limit content to the specified number of lines
        content_lines = file_data['content'].split('\n')
        if len(content_lines) > max_lines:
            preview_content = '\n'.join(content_lines[:max_lines]) + "\n[...] (content truncated)"
        else:
            preview_content = file_data['content']
        
        # Create syntax highlighted panel
        syntax = Syntax(
            preview_content, 
            ext, 
            theme="monokai", 
            line_numbers=True,
            word_wrap=True
        )
        
        console.print(Panel(
            syntax,
            title=f"[bold green]{file_data['path']}[/] [dim]({file_data['size'] / 1024:.1f} KB)[/]",
            border_style="green",
            expand=False
        ))

def analyze_with_progress(code_ingest, directory, exclude_patterns, max_file_size):
    """Run analysis with console progress tracking"""
    # Create simple progress tracker
    progress_tracker = SimpleProgressTracker()
    
    # Store original progress function
    original_print_progress = code_ingest.print_progress
    
    # Replace with our simple progress tracker
    code_ingest.print_progress = progress_tracker
    
    try:
        # Run the analysis
        with console.status("[cyan]Scanning repository...", spinner="dots"):
            console.print(f"Analyzing directory: {directory}")
            
            result = code_ingest.analyze_directory(
                directory,
                exclude_patterns=exclude_patterns,
                max_file_size_kb=max_file_size,
                show_progress=True
            )
        
        return result
    finally:
        # Restore original function
        code_ingest.print_progress = original_print_progress

@app.command()
def analyze(
    directory: str = typer.Argument(..., help="Directory to analyze"),
    exclude: str = typer.Option(None, "--exclude", "-e", help="Comma-separated patterns to exclude"),
    max_size: int = typer.Option(50, "--max-size", "-m", help="Maximum file size in KB to include"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file name"),
    preview: bool = typer.Option(True, "--preview/--no-preview", help="Show preview of file contents"),
):
    """Analyze a code repository and generate a beautiful report"""
    # Import code_ingest module
    code_ingest = import_code_ingest()
    if not code_ingest:
        sys.exit(1)
    
    # Set default exclude patterns if not provided
    if exclude is None:
        exclude = ",".join(code_ingest.DEFAULT_EXCLUDES)
    
    # Process exclude patterns
    exclude_patterns = [p.strip() for p in exclude.split(',') if p.strip()]
    
    # Set default output filename if not provided
    if not output:
        dir_name = os.path.basename(os.path.normpath(directory))
        output = f"{dir_name}_ingest.md"
    
    # Display logo
    display_logo()
    
    # Show analysis parameters
    params_table = Table(box=box.ROUNDED, border_style="blue", show_header=False)
    params_table.add_column("Parameter", style="bold white")
    params_table.add_column("Value", style="yellow")
    
    params_table.add_row("Directory", directory)
    params_table.add_row("Exclude Patterns", exclude)
    params_table.add_row("Max File Size", f"{max_size} KB")
    params_table.add_row("Output File", output)
    
    console.print(Panel(params_table, title="Analysis Parameters", border_style="blue", expand=False))
    
    # Confirm analysis
    if not Confirm.ask("\n[bold cyan]Start analysis?[/]"):
        console.print("[yellow]Analysis cancelled.[/]")
        return
    
    console.print()  # Add a newline
    
    # Run the analysis
    result = analyze_with_progress(code_ingest, directory, exclude_patterns, max_size)
    
    if not result:
        console.print("[bold red]Analysis failed. Exiting.[/]")
        sys.exit(1)
    
    # Generate the report
    with console.status("[cyan]Generating Markdown report...", spinner="dots"):
        markdown_content = code_ingest.generate_markdown(result)
        
        # Save to file
        with open(output, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
    
    # Display results
    console.print("\n[bold green]✓[/] [bold]Analysis completed successfully![/]\n")
    
    # Display summary
    display_summary(result)
    
    # Display directory tree
    display_directory_tree(result['directory_structure'])
    
    # Display file preview if requested
    if preview and result['file_contents']:
        display_file_preview(result['file_contents'])
    
    # Show where the report was saved
    console.print(f"\n[bold green]Report saved:[/] [bold white]{output}[/]")
    
    # Ask if user wants to view the report
    if Confirm.ask("\n[bold cyan]View report in terminal?[/]"):
        # Read the report file
        with open(output, 'r', encoding='utf-8') as f:
            report_content = f.read()
        
        # Display the report using Markdown renderer
        md = Markdown(report_content)
        console.print(Panel(md, title=f"[bold green]{output}[/]", border_style="green", expand=True))

@app.command()
def interactive():
    """Start an interactive analysis session"""
    # Import code_ingest module
    code_ingest = import_code_ingest()
    if not code_ingest:
        sys.exit(1)
    
    # Display logo
    display_logo()
    
    # Get directory path
    directory = Prompt.ask("[bold cyan]Enter directory to analyze[/]")
    
    # Get exclude patterns
    default_excludes = ",".join(code_ingest.DEFAULT_EXCLUDES)
    exclude = Prompt.ask(
        "[bold cyan]Enter exclude patterns[/] [dim](comma-separated)[/]",
        default=default_excludes
    )
    exclude_patterns = [p.strip() for p in exclude.split(',') if p.strip()]
    
    # Get max file size
    max_size = int(Prompt.ask(
        "[bold cyan]Maximum file size in KB[/]",
        default="50"
    ))
    
    # Get output file name
    dir_name = os.path.basename(os.path.normpath(directory))
    default_output = f"{dir_name}_ingest.md"
    output = Prompt.ask(
        "[bold cyan]Output file name[/]",
        default=default_output
    )
    
    # Confirm analysis
    console.print("\n[bold]Analysis parameters:[/]")
    console.print(f"  [dim]Directory:[/] [yellow]{directory}[/]")
    console.print(f"  [dim]Exclude patterns:[/] [yellow]{exclude}[/]")
    console.print(f"  [dim]Max file size:[/] [yellow]{max_size} KB[/]")
    console.print(f"  [dim]Output file:[/] [yellow]{output}[/]")
    
    if not Confirm.ask("\n[bold cyan]Start analysis?[/]"):
        console.print("[yellow]Analysis cancelled.[/]")
        return
    
    console.print()  # Add a newline
    
    # Run the analysis
    result = analyze_with_progress(code_ingest, directory, exclude_patterns, max_size)
    
    if not result:
        console.print("[bold red]Analysis failed. Exiting.[/]")
        sys.exit(1)
    
    # Generate the report
    with console.status("[cyan]Generating Markdown report...", spinner="dots"):
        markdown_content = code_ingest.generate_markdown(result)
        
        # Save to file
        with open(output, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
    
    # Display results
    console.print("\n[bold green]✓[/] [bold]Analysis completed successfully![/]\n")
    
    # Display summary
    display_summary(result)
    
    # Display directory tree
    display_directory_tree(result['directory_structure'])
    
    # Ask if user wants to preview file contents
    if Confirm.ask("\n[bold cyan]Show file content preview?[/]"):
        display_file_preview(result['file_contents'])
    
    # Show where the report was saved
    console.print(f"\n[bold green]Report saved:[/] [bold white]{output}[/]")
    
    # Ask if user wants to view the report
    if Confirm.ask("\n[bold cyan]View report in terminal?[/]"):
        # Read the report file
        with open(output, 'r', encoding='utf-8') as f:
            report_content = f.read()
        
        # Display the report using Markdown renderer
        md = Markdown(report_content)
        console.print(Panel(md, title=f"[bold green]{output}[/]", border_style="green", expand=True))

if __name__ == "__main__":
    try:
        app()
    except KeyboardInterrupt:
        console.print("\n[yellow]Analysis cancelled.[/]")
        sys.exit(0)
    except Exception as e:
        console.print(f"\n[bold red]Error:[/] {str(e)}")
        sys.exit(1)