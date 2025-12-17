#!/usr/bin/env python3
"""
CLOUD GAP FILLER - Generate Only Missing Files
================================================

Generates ONLY files that exist in data_generator.py but are NOT in the cloud.
Code COPIED EXACTLY from data_generator.py for consistency.

Files to Generate (12 files total):
-----------------------------------
MONTHLY (bronze/2025/09/) - 3 files:
  ✅ 1IPFM0024_202509.csv
  ✅ 1IPFM63_202509.csv
  ✅ 1IPFM99_202509.csv

REFERENCE (bronze/ref/) - 9 files:
  ✅ segmentprdt_202509.csv
  ✅ ptgst_202509.csv
  ✅ basecli_inv.csv
  ✅ histo_note_risque.csv
  ✅ table_segmentation_azec_mml.csv
  ✅ constrcu_azec_segment.csv
  ✅ do_dest.csv
  ✅ ref_mig_azec_vs_ims.csv
  ✅ indices.csv

Files ALEADY in Cloud (do NOT generate):
----------------------------------------
❌ ipf16/36.csv
❌ ird_risk_q45/q46/qan_202509.csv
❌ All mapping tables (isic, naf, etc.)
❌ All other reference files (lob, cproduit, clacent, garantcu, etc.)

Usage:
------
python3 workenv/generate_cloud_missing.py

Output: ./datalake_cloud_missing/
"""

import os
import csv
import random
import string
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Configuration
VISION = "202509"
OUTPUT_PATH = "./datalake_cloud_missing"
RANDOM_SEED = 42

random.seed(RANDOM_SEED)


class CloudGapGenerator:
    """Generates ONLY missing files - code copied from DataGenerator."""
    
    def __init__(self, vision: str, output_path: str):
        self.vision = vision
        self.output_path = Path(output_path)
        self.year, self.month = self._parse_vision(vision)
        
        # Date boundaries
        self.vision_start = datetime(self.year, self.month, 1)
        self.vision_end = self._get_month_end(self.year, self.month)
        self.year_start = datetime(self.year, 1, 1)
        self.year_end = datetime(self.year, 12, 31)
        self.one_year_ago = datetime(self.year - 1, self.month, 1)
        
        # Pre-generated data pools (same as data_generator.py)
        self.product_codes = [f"01{str(i).zfill(3)}" for i in range(1, 151) if i != 73]
        self.policy_numbers = [f"POL{str(i).zfill(8)}" for i in range(1, 50001)]
        self.client_ids = [f"CLT{str(i).zfill(7)}" for i in range(1, 8001)]
        self.management_points = [f"PT{str(i).zfill(5)}" for i in range(1, 501)]
        
        # NAF codes (same pool as data_generator.py)
        self.naf_2008_codes = ['4120A', '4120B', '4211Z', '4212Z', '4213A', '4213B',
                               '4221Z', '4222Z', '4299Z', '4311Z', '4312A', '4312B',
                               '4313Z', '4321A', '4321B', '4322A', '4322B', '4329A',
                               '4329B', '4331Z', '4332A', '4332B', '4332C', '4333Z',
                               '4334Z', '4339Z', '4391A', '4391B', '4399A', '4399B',
                               '4399C', '4399D', '4399E']
        
        self.actprin_codes = ['4120A', '4211Z', '4213A', '4221Z', '4312A',
                             '4321A', '4322A', '4329A', '4332A', '4391A']
        
        # AZEC policies
        self.azec_policies = [f"AZEC{str(i).zfill(7)}" for i in range(1, 5001)]
        
        # Create silver and gold folders
        self._create_datalake_structure()
        
        print(f"📦 Output: {self.output_path}")
        print(f"🎯 Vision: {self.vision}")
    
    def _create_datalake_structure(self):
        """Create silver and gold folder structure."""
        (self.output_path / "silver" / str(self.year) / f"{self.month:02d}").mkdir(parents=True, exist_ok=True)
        (self.output_path / "gold" / str(self.year) / f"{self.month:02d}").mkdir(parents=True, exist_ok=True)
        print("✅ Created silver/ and gold/ folders")
    
    def _parse_vision(self, vision: str):
        """Parse vision string."""
        return int(vision[:4]), int(vision[4:6])
    
    def _get_month_end(self, year: int, month: int):
        """Get last day of month."""
        if month == 12:
            return datetime(year, month, 31)
        next_month = datetime(year, month + 1, 1)
        return next_month - timedelta(days=1)
    
    def random_choice(self, choices):
        """Safe random choice."""
        return random.choice(choices) if choices else None
    
    def random_numeric_string(self, length: int):
        """Generate random numeric string."""
        return ''.join(random.choices(string.digits, k=length))
    
    def random_date(self, start_date: datetime, end_date: datetime):
        """Generate random date."""
        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")
    
    def random_double(self, min_val: float, max_val: float):
        """Generate random double."""
        return round(random.uniform(min_val, max_val), 2)
    
    def add_extra_columns(self, data: Dict, num_rows: int, num_extra: int = 5):
        """Add extra placeholder columns."""
        for i in range(num_extra):
            data[f'EXTRA_COL_{i+1}'] = [''] * num_rows
        return data
    
    def write_csv(self, data, filename, location_type, compression=None, separator=";"):
        """Write CSV (copied from data_generator.py)."""
        if location_type == "monthly":
            folder = self.output_path / "bronze" / str(self.year) / f"{self.month:02d}"
        else:
            folder = self.output_path / "bronze" / "ref"
        
        folder.mkdir(parents=True, exist_ok=True)
        filepath = folder / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            if data:
                writer = csv.DictWriter(f, fieldnames=data.keys(), delimiter=separator)
                writer.writeheader()
                num_rows = len(next(iter(data.values())))
                for i in range(num_rows):
                    row = {key: values[i] for key, values in data.items()}
                    writer.writerow(row)
        
        size_kb = filepath.stat().st_size / 1024
        print(f"  ✅ {filename}: {size_kb:.2f} KB")
    
    # =========================================================================
    # GENERATION METHODS (Copied EXACTLY from data_generator.py)
    # =========================================================================
    
    def generate_segmentprdt(self):
        """Generate segmentprdt (line 202-211 from data_generator.py)."""
        print("\n📝 segmentprdt")
        data = {
            'CPROD': [f'{i+1:04d}' for i in range(300)],
            'CDPOLE': [self.random_choice(['1', '3']) for _ in range(300)],
            'CMARCH': ['6'] * 300,
            'CSEG': ['2'] * 300,
            'CSSSEG': [f'{(i % 10) + 1:02d}' for i in range(300)]
        }
        data = self.add_extra_columns(data, 300)
        self.write_csv(data, f"segmentprdt_{self.vision}.csv", "reference")
    
    def generate_ptgst(self):
        """Generate ptgst (line 232-240 from data_generator.py)."""
        print("\n📝 ptgst")
        num_points = 500
        data = {
            'PTGST': [f'PT{i+1:05d}' for i in range(num_points)],
            'UPPER_MID': [self.random_choice(['MID', 'UPPER', 'LARGE', 'SME']) for _ in range(num_points)]
        }
        data = self.add_extra_columns(data, num_points)
        self.write_csv(data, f"ptgst_{self.vision}.csv", "reference")
    
    def generate_basecli_inv(self):
        """Generate basecli_inv (line 280-285 from data_generator.py)."""
        print("\n📝 basecli_inv")
        data = {
            'NOCLT': random.choices(self.client_ids, k=15000),
            'CDAPET': [self.random_choice(self.naf_2008_codes) for _ in range(15000)]
        }
        data = self.add_extra_columns(data, 15000)
        self.write_csv(data, "basecli_inv.csv", "reference")
    
    def generate_histo_note_risque(self):
        """Generate histo_note_risque (line 287-295 from data_generator.py)."""
        print("\n📝 histo_note_risque")
        data = {
            'CDSIREN': [self.random_numeric_string(9) for _ in range(10000)],
            'CDNOTE': [self.random_choice(['A+', 'A', 'B', 'C']) for _ in range(10000)],
            'DTDEB_VALID': [self.random_date(self.one_year_ago, self.vision_start) for _ in range(10000)],
            'DTFIN_VALID': [self.random_date(self.vision_end, self.year_end) for _ in range(10000)]
        }
        data = self.add_extra_columns(data, 10000)
        self.write_csv(data, "histo_note_risque.csv", "reference")
    
    def generate_ipfm_files(self):
        """Generate IPFM files (line 495-534 from data_generator.py)."""
        print("\n📝 IPFM Special Products (3 files)")
        num_rows = 800
        selected_policies = random.choices(self.policy_numbers, k=num_rows)
        
        # IPFM0024
        data = {
            'NOPOL': selected_policies,
            'NOINT': [self.random_choice([f'INT{i:05d}' for i in range(1, 301)]) for _ in range(num_rows)],
            'CDPROD': [self.random_choice(self.product_codes) for _ in range(num_rows)],
            'CDACTPRF01': [self.random_choice(['ACT01', 'ACT02', 'ACT03', 'ACT04', 'ACT05']) for _ in range(num_rows)],
            'CDACTPRF02': [self.random_choice(['ACT01', 'ACT02', 'ACT03', '']) for _ in range(num_rows)]
        }
        data = self.add_extra_columns(data, num_rows, 3)
        self.write_csv(data, f"1IPFM0024_{self.vision}.csv", "monthly")
        
        # IPFM63
        data = {
            'NOPOL': selected_policies,
            'NOINT': [self.random_choice([f'INT{i:05d}' for i in range(1, 301)]) for _ in range(num_rows)],
            'CDPROD': [self.random_choice(self.product_codes) for _ in range(num_rows)],
            'ACTPRIN': [self.random_choice(self.actprin_codes) for _ in range(num_rows)],
            'ACTSEC1': [self.random_choice(self.actprin_codes + ['']) for _ in range(num_rows)],
            'CDNAF': [self.random_choice(self.naf_2008_codes) for _ in range(num_rows)],
            'MTCA1': [self.random_double(0, 100000) for _ in range(num_rows)]
        }
        data = self.add_extra_columns(data, num_rows, 3)
        self.write_csv(data, f"1IPFM63_{self.vision}.csv", "monthly")
        
        # IPFM99
        data = {
            'NOPOL': selected_policies,
            'NOINT': [self.random_choice([f'INT{i:05d}' for i in range(1, 301)]) for _ in range(num_rows)],
            'CDPROD': [self.random_choice(self.product_codes) for _ in range(num_rows)],
            'CDACPR1': [self.random_choice(['ACT01', 'ACT02', 'ACT03', 'ACT04']) for _ in range(num_rows)],
            'CDACPR2': [self.random_choice(['ACT01', 'ACT02', '']) for _ in range(num_rows)],
            'MTCA': [self.random_double(0, 80000) for _ in range(num_rows)]
        }
        data = self.add_extra_columns(data, num_rows, 3)
        self.write_csv(data, f"1IPFM99_{self.vision}.csv", "monthly")
    
    def generate_azec_files(self):
        """Generate AZEC reference files (line 742-766 from data_generator.py)."""
        print("\n📝 AZEC Reference Files (4 files)")
        
        # table_segmentation_azec_mml
        data = {
            'PRODUIT': ['A00', 'A01', 'AA1', 'MA1', 'MP1'] * 20,
            'SEGMENT': ['PME'] * 100,
            'CMARCH': ['6'] * 100,
            'CSEG': ['2'] * 100,
            'CSSSEG': ['01'] * 100,
            'LMARCH': ['CONSTRUCTION'] * 100,
            'LSEG': ['CONSTRUCTION'] * 100,
            'LSSSEG': ['Sub-seg 1'] * 100
        }
        data = self.add_extra_columns(data, 100)
        self.write_csv(data, "table_segmentation_azec_mml.csv", "reference")
        
        # constrcu_azec_segment
        num_seg = 400
        azec_policy_sample = random.choices(self.azec_policies, k=num_seg)
        data = {
            'CDPROD': random.choices(['A00', 'A01', 'AA1', 'MA1', 'MP1'], k=num_seg),
            'POLICE': azec_policy_sample,
            'SEGMENT': random.choices(['PME', 'GE', 'PROF', 'TPE'], k=num_seg),
            'TYPE_PRODUIT': random.choices(['MRH', 'MRP', 'TRC', 'DO', 'RC'], k=num_seg)
        }
        data = self.add_extra_columns(data, num_seg, 2)
        self.write_csv(data, "constrcu_azec_segment.csv", "reference", separator=",")
    
    def generate_other_files(self):
        """Generate do_dest, ref_mig, indices (line 800-825 from data_generator.py)."""
        print("\n📝 Other Reference Files (3 files)")
        
        # do_dest
        destinations = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '']
        data = {
            'DESTINAT': [self.random_choice(destinations) for _ in range(500)]
        }
        data = self.add_extra_columns(data, 500, 2)
        self.write_csv(data, "do_dest.csv", "reference", separator=",")
        
        # ref_mig_azec_vs_ims
        data = {
            'POLICE_AZEC': random.choices(self.azec_policies, k=200),
            'NOPOL_IMS': random.choices(self.policy_numbers, k=200),
            'STATUS': ['MIGRATED'] * 200
        }
        self.write_csv(data, "ref_mig_azec_vs_ims.csv", "reference", separator=",")
        
        # indices
        data = {
            'INDEX_CODE': ['BT01', 'BT50', 'TP01', 'TP10'] * 50,
            'INDEX_VALUE': [self.random_double(100, 150) for _ in range(200)],
            'INDEX_DATE': [self.random_date(self.vision_start, self.vision_end) for _ in range(200)]
        }
        self.write_csv(data, "indices.csv", "reference", separator=",")
    
    def generate_all(self):
        """Generate all missing files."""
        print("=" * 80)
        print("🚀 CLOUD GAP FILLER - Generating Missing Files")
        print("=" * 80)
        print(f"Vision: {self.vision}")
        print(f"Output: {self.output_path}")
        print("=" * 80)
        
        # Reference files (9 files)
        self.generate_segmentprdt()
        self.generate_ptgst()
        self.generate_basecli_inv()
        self.generate_histo_note_risque()
        self.generate_azec_files()      # 2 files
        self.generate_other_files()      # 3 files
        
        # Monthly files (3 files)
        self.generate_ipfm_files()       # 3 files
        
        print("\n" + "=" * 80)
        print("✅ GENERATION COMPLETE - 12 Files Created")
        print("=" * 80)
        print(f"\n📤 Upload from: {self.output_path}/bronze/")
        print("\nFiles generated:")
        print("  MONTHLY (3): 1IPFM0024/63/99_202509.csv")
        print("  REFERENCE (9): segmentprdt, ptgst, basecli_inv, histo_note_risque")
        print("                 table_segmentation_azec, constrcu_azec_segment")
        print("                 do_dest, ref_mig_azec_vs_ims, indices")
        print("\n✨ Ready for cloud upload!")
        print("=" * 80)


if __name__ == "__main__":
    generator = CloudGapGenerator(vision=VISION, output_path=OUTPUT_PATH)
    generator.generate_all()
