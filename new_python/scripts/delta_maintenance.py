#!/usr/bin/env python
"""
Delta Lake Maintenance Script

This script provides maintenance operations for Delta Lake tables:
- OPTIMIZE: Compact small files and apply Z-Ordering
- VACUUM: Remove old versions to reclaim storage

Usage:
    # Optimize all tables for a specific vision
    python scripts/delta_maintenance.py --vision 202509 --action optimize
    
    # Vacuum all tables (keep 30 days)
    python scripts/delta_maintenance.py --vision 202509 --action vacuum --retention-days 30
    
    # Optimize specific table with Z-Ordering
    python scripts/delta_maintenance.py --vision 202509 --action optimize \\
        --table silver/mvt_const_ptf --zorder police,dtfin

Best Practices:
    - Run OPTIMIZE after bulk data loads or frequent small writes
    - Run VACUUM weekly to reclaim storage (balance retention vs. cost)
    - Schedule during off-peak hours (expensive operations)
    - For production: Keep 30+ days retention for auditing
    - For development: 7 days retention is usually sufficient
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from utils.loaders.config_loader import ConfigLoader
from utils.helpers import optimize_delta_table, vacuum_delta_table, extract_year_month_int
from utils.logging.pipeline_logger import PipelineLogger


def get_all_delta_tables(config: ConfigLoader, vision: str, layer: str) -> list:
    """
    Get list of all Delta tables for a vision and layer.
    
    Returns list of (table_name, table_path) tuples.
    """
    base_path = config.get('datalake.base_path')
    year, month = extract_year_month_int(vision)
    layer_path = f"{base_path}/{layer}/{year}/{month:02d}"
    
    # Common table names for PTF_MVT pipeline
    if layer == "silver":
        tables = [
            "mvt_const_ptf",
            "azec_ptf",
            "consolidation_ptf"
        ]
    elif layer == "gold":
        tables = [
            "construction_portfolio",
            "portfolio_movements"
        ]
    else:
        tables = []
    
    return [(name, f"{layer_path}/{name}_{vision}") for name in tables]


def optimize_all_tables(
    spark: SparkSession,
    config: ConfigLoader,
    vision: str,
    logger: PipelineLogger,
    zorder_cols: dict = None
):
    """
    Optimize all Delta tables for a vision.
    
    Args:
        spark: SparkSession
        config: ConfigLoader
        vision: Vision in YYYYMM format
        logger: Logger instance
        zorder_cols: Dict mapping table_name -> list of columns for Z-Ordering
    """
    zorder_cols = zorder_cols or {}
    
    logger.info(f"Starting OPTIMIZE for all tables (vision: {vision})")
    
    for layer in ["silver", "gold"]:
        tables = get_all_delta_tables(config, vision, layer)
        
        for table_name, table_path in tables:
            try:
                logger.info(f"Optimizing {layer}/{table_name}...")
                
                # Get Z-Ordering columns for this table
                zorder = zorder_cols.get(table_name)
                
                optimize_delta_table(
                    spark,
                    table_path,
                    zorder_columns=zorder,
                    logger=logger
                )
                
            except Exception as e:
                logger.warning(f"Failed to optimize {table_name}: {e}")
                continue
    
    logger.success("OPTIMIZE completed for all tables")


def vacuum_all_tables(
    spark: SparkSession,
    config: ConfigLoader,
    vision: str,
    retention_hours: int,
    logger: PipelineLogger
):
    """
    VACUUM all Delta tables for a vision.
    
    Args:
        spark: SparkSession
        config: ConfigLoader
        vision: Vision in YYYYMM format
        retention_hours: Retention period in hours
        logger: Logger instance
    """
    logger.info(f"Starting VACUUM for all tables (vision: {vision}, retention: {retention_hours}h)")
    logger.warning(f"⚠️  Time travel will be limited to {retention_hours}h after VACUUM")
    
    for layer in ["silver", "gold"]:
        tables = get_all_delta_tables(config, vision, layer)
        
        for table_name, table_path in tables:
            try:
                logger.info(f"Vacuuming {layer}/{table_name}...")
                
                vacuum_delta_table(
                    spark,
                    table_path,
                    retention_hours=retention_hours,
                    logger=logger
                )
                
            except Exception as e:
                logger.warning(f"Failed to vacuum {table_name}: {e}")
                continue
    
    logger.success("VACUUM completed for all tables")


def main():
    parser = argparse.ArgumentParser(
        description="Delta Lake Maintenance Operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--vision",
        required=True,
        help="Vision in YYYYMM format (e.g., 202509)"
    )
    
    parser.add_argument(
        "--action",
        required=True,
        choices=["optimize", "vacuum"],
        help="Maintenance action to perform"
    )
    
    parser.add_argument(
        "--table",
        help="Specific table path (e.g., silver/mvt_const_ptf). If not specified, processes all tables."
    )
    
    parser.add_argument(
        "--zorder",
        help="Comma-separated list of columns for Z-Ordering (optimize only)"
    )
    
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Retention period in days for VACUUM (default: 7)"
    )
    
    parser.add_argument(
        "--config",
        default="config/config.yml",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    
    # Initialize Spark
    spark = SparkSession.builder.appName("DeltaMaintenance").getOrCreate()
    
    # Load configuration
    config = ConfigLoader(args.config)
    
    # Initialize logger
    logger = PipelineLogger("delta_maintenance", args.vision)
    
    try:
        if args.action == "optimize":
            if args.table:
                # Optimize specific table
                base_path = config.get('datalake.base_path')
                table_path = f"{base_path}/{args.table}_{args.vision}"
                
                zorder = args.zorder.split(",") if args.zorder else None
                
                optimize_delta_table(
                    spark,
                    table_path,
                    zorder_columns=zorder,
                    logger=logger
                )
            else:
                # Optimize all tables with recommended Z-Ordering
                zorder_config = {
                    "mvt_const_ptf": ["police", "dtfin"],
                    "azec_ptf": ["police", "dtfin"],
                    "consolidation_ptf": ["police", "dtfin"],
                    "construction_portfolio": ["police"],
                    "portfolio_movements": ["police", "dtfin"]
                }
                
                optimize_all_tables(
                    spark,
                    config,
                    args.vision,
                    logger,
                    zorder_cols=zorder_config
                )
        
        elif args.action == "vacuum":
            retention_hours = args.retention_days * 24
            
            if args.table:
                # Vacuum specific table
                base_path = config.get('datalake.base_path')
                table_path = f"{base_path}/{args.table}_{args.vision}"
                
                vacuum_delta_table(
                    spark,
                    table_path,
                    retention_hours=retention_hours,
                    logger=logger
                )
            else:
                # Vacuum all tables
                vacuum_all_tables(
                    spark,
                    config,
                    args.vision,
                    retention_hours,
                    logger
                )
        
        logger.success("Delta Lake maintenance completed successfully")
        
    except Exception as e:
        logger.error(f"Delta Lake maintenance failed: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
