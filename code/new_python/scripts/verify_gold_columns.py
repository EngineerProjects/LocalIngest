#!/usr/bin/env python3
"""
Verify that all GOLD_COLUMNS_PTF_MVT are created in the codebase.
"""

import subprocess
import sys
from pathlib import Path

# Import the column list
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.constants import GOLD_COLUMNS_PTF_MVT

def check_column_exists(column_name: str) -> tuple:
    """
    Check if a column is created anywhere in the codebase.
    Returns (exists, files_found, sample_line)
    """
    search_paths = [
        "src/processors",
        "utils/transformations",
        "config"
    ]
    
    for search_path in search_paths:
        try:
            result = subprocess.run(
                ["grep", "-r", "-l", column_name, search_path],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent
            )
            
            if result.returncode == 0:
                files = result.stdout.strip().split('\n')
                
                # Get a sample line
                sample_result = subprocess.run(
                    ["grep", "-h", column_name, files[0]],
                    capture_output=True,
                    text=True,
                    cwd=Path(__file__).parent.parent
                )
                sample_line = sample_result.stdout.strip().split('\n')[0][:80]
                
                return (True, files, sample_line)
        except:
            continue
    
    return (False, [], "")

def main():
    """Verify all columns exist."""
    print(f"Verifying {len(GOLD_COLUMNS_PTF_MVT)} columns from GOLD_COLUMNS_PTF_MVT...\n")
    
    missing_columns = []
    found_columns = []
    
    for col in GOLD_COLUMNS_PTF_MVT:
        exists, files, sample = check_column_exists(col)
        
        if exists:
            found_columns.append(col)
            print(f"✅ {col:25} - Found in {len(files)} file(s)")
        else:
            missing_columns.append(col)
            print(f"❌ {col:25} - NOT FOUND")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY:")
    print(f"  Found: {len(found_columns)}/{len(GOLD_COLUMNS_PTF_MVT)} ({100*len(found_columns)/len(GOLD_COLUMNS_PTF_MVT):.1f}%)")
    print(f"  Missing: {len(missing_columns)}")
    print(f"{'='*80}")
    
    if missing_columns:
        print(f"\n⚠️  MISSING COLUMNS ({len(missing_columns)}):")
        for col in missing_columns:
            print(f"    - {col}")
        
        print(f"\n📝 These columns will be NULL in output unless created!")
        return 1
    else:
        print(f"\n✅ All columns verified - good to go!")
        return 0

if __name__ == "__main__":
    sys.exit(main())
