"""
Helper module for creating directory structures for the Code Ingest Tool.
This can be useful for setting up the project structure correctly.
"""

import os
import shutil

def create_project_structure():
    """Create the basic directory structure for the Code Ingest Tool project."""
    
    # Create main directories
    os.makedirs('templates', exist_ok=True)
    os.makedirs('outputs', exist_ok=True)
    
    # Create template directory if it doesn't exist
    template_dir = os.path.join(os.path.dirname(__file__), 'template')
    os.makedirs(template_dir, exist_ok=True)
    
    # Check if files exist
    if not os.path.exists('app.py'):
        print("Error: app.py not found. Please make sure you've created this file.")
    
    if not os.path.exists(os.path.join('templates', 'index.html')):
        print("Error: templates/index.html not found. Please make sure you've created this file.")
    
    print("Project structure verified.")

def verify_installation():
    """Verify that all required files are present."""
    required_files = [
        'app.py',
        os.path.join('templates', 'index.html'),
        'requirements.txt',
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("Warning: The following required files are missing:")
        for file_path in missing_files:
            print(f"  - {file_path}")
        return False
    
    print("All required files are present.")
    return True

if __name__ == "__main__":
    create_project_structure()
    verify_installation()