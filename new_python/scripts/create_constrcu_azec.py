#!/usr/bin/env python3
"""
CONSTRCU_AZEC Reference Table Generator

This script creates the CONSTRCU_AZEC reference table by preprocessing raw CONSTRCU data.
Replicates SAS logic from REF_segmentation_azec.sas (L226-343).

Input:
  - CONSTRCU.CONSTRCU (raw construction site data)
  - LOB (product segmentation reference)
  - POLIC_CU (for filtering construction market)

Output:
  - CONSTRCU_AZEC (4 columns: POLICE, CDPROD, SEGMENT, TYPE_PRODUIT)

Usage:
  python scripts/create_constrcu_azec.py --vision 202509
  python scripts/create_constrcu_azec.py --vision ref  # For reference data
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, coalesce
from src.reader import BronzeReader
from config.config import Config
from utils.logger import get_logger


class ConstrucuAzecGenerator:
    """
    Generates CONSTRCU_AZEC reference table from raw CONSTRCU data.
    
    Process:
    1. Read CONSTRCU raw data (has PRODUIT)
    2. Join with LOB to get CDPROD and SEGMENT
    3. Calculate TYPE_PRODUIT based on business rules
    4. Output 4 columns: POLICE, CDPROD, SEGMENT, TYPE_PRODUIT
    """
    
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config
        self.logger = get_logger(__name__)
    
    def generate(self, vision: str = 'ref') -> DataFrame:
        """
        Generate CONSTRCU_AZEC table.
        
        Args:
            vision: Vision period (YYYYMM) or 'ref' for reference data
            
        Returns:
            DataFrame with POLICE, CDPROD, SEGMENT, TYPE_PRODUIT columns
        """
        self.logger.info("=" * 80)
        self.logger.info("  CONSTRCU_AZEC Reference Table Generator")
        self.logger.info("=" * 80)
        self.logger.info(f"Vision: {vision}")
        
        # Step 1: Read CONSTRCU raw data
        self.logger.info("\nStep 1: Reading CONSTRCU raw data...")
        df_constrcu = self._read_constrcu(vision)
        self.logger.info(f"  Rows read: {df_constrcu.count():,}")
        
        # Step 2: Read LOB reference (for CDPROD and SEGMENT)
        self.logger.info("\nStep 2: Reading LOB reference data...")
        df_lob = self._read_lob()
        self.logger.info(f"  LOB products: {df_lob.count():,}")
        
        # Step 3: Join CONSTRCU with LOB to get CDPROD and SEGMENT
        self.logger.info("\nStep 3: Joining CONSTRCU with LOB...")
        df_enriched = self._enrich_with_lob(df_constrcu, df_lob)
        self.logger.info(f"  Enriched rows: {df_enriched.count():,}")
        
        # Step 4: Calculate TYPE_PRODUIT based on business rules
        self.logger.info("\nStep 4: Calculating TYPE_PRODUIT...")
        df_enriched = self._calculate_type_produit(df_enriched)
        
        # Step 5: Select final columns (SAS L343: keep POLICE CDPROD SEGMENT TYPE_PRODUIT)
        self.logger.info("\nStep 5: Selecting final columns...")
        df_final = df_enriched.select(
            'police',
            'cdprod',
            'segment',
            'type_produit'
        ).dropDuplicates(['police', 'cdprod'])
        
        final_count = df_final.count()
        self.logger.info(f"  Final rows: {final_count:,}")
        
        # Show sample
        self.logger.info("\nSample of CONSTRCU_AZEC:")
        df_final.show(10, truncate=False)
        
        return df_final
    
    def _read_constrcu(self, vision: str) -> DataFrame:
        """Read CONSTRCU raw data (SAS L226-231)."""
        reader = BronzeReader(self.spark, self.config)
        
        try:
            # Read CONSTRCU
            df = reader.read_file_group('constrcu_azec', vision)
            
            # Select needed columns (SAS L226: KEEP = POLICE PRODUIT typmarc1 ltypmar1 formule nat_cnt)
            # Note: We may not have all these columns in the raw file
            available_cols = df.columns
            needed_cols = ['police', 'produit']
            
            # Add optional columns if they exist
            optional_cols = ['typmarc1', 'ltypmar1', 'formule', 'nat_cnt']
            for col_name in optional_cols:
                if col_name in available_cols:
                    needed_cols.append(col_name)
            
            df = df.select(*needed_cols)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read CONSTRCU: {e}")
            raise
    
    def _read_lob(self) -> DataFrame:
        """Read LOB reference data for product segmentation."""
        reader = BronzeReader(self.spark, self.config)
        
        try:
            df = reader.read_file_group('lob', 'ref')
            
            # Filter for construction market (SAS L230: IF CMARCH IN ('6'))
            df = df.filter(col('cmarch') == '6')
            
            # Select needed columns for enrichment
            df = df.select(
                'produit',      # Join key
                'cdprod',       # Product code
                'cprod',        # Alternative product code
                'segment',      # Segment classification
                'cmarch',       # Market code
                'cseg',         # Segment code
                'cssseg',       # Sub-segment code
                'lssseg'        # Sub-segment label (for TYPE_PRODUIT logic)
            ).dropDuplicates(['produit'])
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read LOB: {e}")
            raise
    
    def _enrich_with_lob(self, df_constrcu: DataFrame, df_lob: DataFrame) -> DataFrame:
        """
        Join CONSTRCU with LOB to get CDPROD and SEGMENT.
        
        SAS uses HASH table lookup (L227: %SEGMENTA macro, L82: defineData)
        Python equivalent: LEFT JOIN
        """
        df = df_constrcu.alias('c').join(
            df_lob.alias('l'),
            col('c.produit') == col('l.produit'),
            how='left'
        ).select(
            col('c.*'),                    # All CONSTRCU columns
            col('l.cdprod'),               # From LOB
            col('l.segment'),              # From LOB
            col('l.cseg'),                 # For filtering
            col('l.cssseg'),               # For TYPE_PRODUIT logic
            col('l.lssseg')                # For TYPE_PRODUIT logic
        )
        
        return df
    
    def _calculate_type_produit(self, df: DataFrame) -> DataFrame:
        """
        Calculate TYPE_PRODUIT based on business rules.
        
        Based on SAS L324-335:
        - TRC if LSSSEG = "TOUS RISQUES CHANTIERS"
        - DO if LSSSEG = "DOMMAGES OUVRAGES"
        - Entreprises if PRODUIT = 'RCC'
        - Otherwise from Typrd_2 table or 'Autres'
        
        Simplified version: We don't join Typrd_2 yet, just apply basic rules.
        """
        df = df.withColumn('type_produit',
            # Rule 1: TOUS RISQUES CHANTIERS → TRC (SAS L329)
            when(col('lssseg') == 'TOUS RISQUES CHANTIERS', lit('TRC'))
            # Rule 2: DOMMAGES OUVRAGES → DO (SAS L330)
            .when(col('lssseg') == 'DOMMAGES OUVRAGES', lit('DO'))
            # Rule 3: RCC → Entreprises (SAS L333)
            .when(col('produit') == 'RCC', lit('Entreprises'))
            # Rule 4: Based on typmarc1 if available (simplified version)
            .when(
                (col('produit').isin(['RBA', 'RCD'])) & 
                (col('typmarc1') == '01') if 'typmarc1' in df.columns else lit(False),
                lit('Artisans')
            )
            .when(
                (col('produit').isin(['RBA', 'RCD'])) & 
                (col('typmarc1').isin(['02', '03', '04'])) if 'typmarc1' in df.columns else lit(False),
                lit('Entreprises')
            )
            # Default: Autres (SAS L331)
            .otherwise(lit('Autres'))
        )
        
        return df
    
    def write(self, df: DataFrame, vision: str = 'ref') -> None:
        """
        Write CONSTRCU_AZEC to bronze/ref directory.
        
        Args:
            df: CONSTRCU_AZEC DataFrame
            vision: Vision period or 'ref'
        """
        output_path = self.config.get_bronze_path() / 'ref' / f'constrcu_azec_{vision}'
        
        self.logger.info(f"\nWriting CONSTRCU_AZEC to: {output_path}")
        
        try:
            df.write.mode('overwrite').parquet(str(output_path))
            self.logger.info("✓ SUCCESS: CONSTRCU_AZEC written successfully")
        except Exception as e:
            self.logger.error(f"✗ FAILED to write CONSTRCU_AZEC: {e}")
            raise


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description='Generate CONSTRCU_AZEC reference table'
    )
    parser.add_argument(
        '--vision',
        type=str,
        default='ref',
        help='Vision period (YYYYMM) or "ref" for reference data (default: ref)'
    )
    
    args = parser.parse_args()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("CONSTRCU_AZEC Generator") \
        .getOrCreate()
    
    # Load config
    config = Config()
    
    # Create generator
    generator = ConstrucuAzecGenerator(spark, config)
    
    try:
        # Generate CONSTRCU_AZEC
        df_constrcu_azec = generator.generate(vision=args.vision)
        
        # Write to file
        generator.write(df_constrcu_azec, vision=args.vision)
        
        print("\n" + "=" * 80)
        print("✓ CONSTRCU_AZEC generation completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n✗ FAILED: {e}")
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
