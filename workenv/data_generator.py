#!/usr/bin/env python3
"""
UNIFIED COMPREHENSIVE DATA GENERATOR FOR PTF_MVT PIPELINE
==========================================================

Generates ALL required test data files for SAS-to-PySpark migration testing.
All data is business-rule compliant with correct values.

CRITICAL FIXES:
- CSEGT = '2' (Construction segment) ✓
- CMARCH = '6' (Construction market) ✓
- All other business filters compliant ✓

Usage:
    cd /workspace/workenv
    python3 unified_data_generator.py

Author: Generated for PTF_MVT pipeline testing
Date: 2025-12-16
"""

import os
import csv
import gzip
import random
import string
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Configuration
VISION = "202509"
OUTPUT_PATH = "/workspace/datalake"
RANDOM_SEED = 42

random.seed(RANDOM_SEED)


class DataGenerator:
    """Unified data generator for all PTF_MVT datasets."""
    
    def __init__(self, vision: str, output_path: str):
        self.vision = vision
        self.output_path = Path(output_path)
        self.year, self.month = self._parse_vision(vision)
        
        # Date boundaries
        self.vision_start = datetime(self.year, self.month, 1)
        self.vision_end = self._get_month_end(self.year, self.month)
        self.year_start = datetime(self.year, 1, 1)
        self.year_end = datetime(self.year, 12, 31)
        self.one_year_ago = self.vision_end - timedelta(days=365)
        
        # Shared reference data
        self.product_codes = []
        self.client_ids = []
        self.policy_numbers = []
        self.intermediaries = []
        self.management_points = []
        
        # NAF codes - Centralized pool
        self.naf_2008_codes = ['4120A', '4120B', '4211Z', '4212Z', '4213A', '4213B', 
                               '4221Z', '4222Z', '4299Z', '4311Z', '4312A', '4312B',
                               '4313Z', '4321A', '4321B', '4322A', '4322B', '4329A',
                               '4329B', '4331Z', '4332A', '4332B', '4332C', '4333Z',
                               '4334Z', '4339Z', '4391A', '4391B', '4399A', '4399B',
                               '4399C', '4399D', '4399E']
        self.naf_2003_codes = ['451A', '451B', '452A', '452B', '453A', '453B', '454A']
        
        # ACTPRIN codes (used for IPFM)
        self.actprin_codes = ['4120A', '4211Z', '4213A', '4221Z', '4312A',
                             '4321A', '4322A', '4329A', '4332A', '4391A']
        
        # Create silver and gold folders
        self._create_datalake_structure()
        
        print(f"📦 Initializing data generator for vision {vision}")
    
    def _create_datalake_structure(self):
        """Create silver and gold folder structure."""
        (self.output_path / "silver" / str(self.year) / f"{self.month:02d}").mkdir(parents=True, exist_ok=True)
        (self.output_path / "gold" / str(self.year) / f"{self.month:02d}").mkdir(parents=True, exist_ok=True)
        print("✅ Created silver/ and gold/ folders")
    
    @staticmethod
    def _parse_vision(vision: str):
        """Parse YYYYMM vision."""
        return int(vision[:4]), int(vision[4:6])
    
    @staticmethod
    def _get_month_end(year: int, month: int):
        """Get last day of month."""
        if month == 12:
            return datetime(year, 12, 31)
        return datetime(year, month + 1, 1) - timedelta(days=1)
    
    def random_date(self, start: datetime, end: datetime) -> str:
        """Generate random date in YYYY-MM-DD format."""
        delta = end - start
        random_days = random.randint(0, max(delta.days, 0))
        return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")
    
    def random_choice(self, choices: List[Any], weights: Optional[List[float]] = None) -> Any:
        """Random choice with optional weights."""
        if weights:
            return random.choices(choices, weights=weights, k=1)[0]
        return random.choice(choices)
    
    def random_string(self, length: int) -> str:
        """Generate random string."""
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))
    
    def random_numeric_string(self, length: int) -> str:
        """Generate random numeric string."""
        return ''.join(random.choice(string.digits) for _ in range(length))
    
    def random_double(self, min_val: float, max_val: float, decimals: int = 2) -> float:
        """Generate random double."""
        return round(random.uniform(min_val, max_val), decimals)
    
    def add_extra_columns(self, data: Dict[str, List], num_rows: int, num_extra: int = 5) -> Dict[str, List]:
        """Add random supplementary columns."""
        for i in range(num_extra):
            col_name = f"EXTRA_COL_{i+1}"
            data[col_name] = [self.random_string(10) for _ in range(num_rows)]
        return data
    
    def write_csv(self, data: Dict[str, List], filename: str, location_type: str, 
                  separator: str = "|", compression: Optional[str] = None):
        """Write data to CSV file."""
        
        if location_type == "monthly":
            output_dir = self.output_path / "bronze" / str(self.year) / f"{self.month:02d}"
        else:
            output_dir = self.output_path / "bronze" / "ref"
        
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / filename
        
        columns = list(data.keys())
        num_rows = len(data[columns[0]])
        
        write_func = gzip.open if compression == "gzip" else open
        mode = "wt" if compression == "gzip" else "w"
        
        with write_func(output_file, mode, newline='', encoding='latin9') as f:
            writer = csv.writer(f, delimiter=separator)
            writer.writerow(columns)
            
            for i in range(num_rows):
                row = [data[col][i] for col in columns]
                writer.writerow(row)
        
        print(f"✓ {filename:40s} {num_rows:>6,} rows")
    
    # ========================================================================
    # REFERENCE DATA GENERATORS
    # ========================================================================
    
    def generate_lob(self, num_products: int = 150):
        """Generate LOB (Line of Business) reference."""
        construction_products = []
        for i in range(num_products):
            prod_num = f"{i+1:05d}"
            cdprod = f'01{prod_num[2:]}'
            
            # EXCLUDE product 01073 (filtered by business rules)
            if cdprod == '01073':
                continue
            
            construction_products.append({
                'PRODUIT': f'PROD_{prod_num}',
                'CDPROD': cdprod,
                'CPROD': f'{(i % 50) + 1:04d}',
                'CMARCH': '6',
                'LMARCH': 'CONSTRUCTION',
                'LMARCH2': 'Construction Insurance',
                'CSEG': '2',
                'LSEG': 'CONSTRUCTION',
                'LSEG2': 'Construction Segment',
                'CSSSEG': f'{(i % 10) + 1:02d}',
                'LSSSEG': f'Sub-segment {(i % 10) + 1}',
                'LSSSEG2': f'Construction Sub-segment {(i % 10) + 1}',
                'LPROD': f'Construction Product {prod_num}',
                'SEGMENT': self.random_choice(['PME', 'GE', 'PROF', 'TPE'])
            })
        
        data = {col: [p[col] for p in construction_products] for col in construction_products[0].keys()}
        self.product_codes = [p['CDPROD'] for p in construction_products]
        data = self.add_extra_columns(data, num_products)
        self.write_csv(data, "lob.csv", "reference")
    
    def generate_cproduit(self, num_products: int = 150):
        """Generate CPRODUIT reference."""
        data = {
            'cprod': [f'{i+1:04d}' for i in range(num_products)],
            'Type_Produit_2': [self.random_choice(['MRH', 'MRP', 'TRC', 'DO', 'RC']) for _ in range(num_products)],
            'segment': [self.random_choice(['PME', 'GE', 'PROF', 'TPE']) for _ in range(num_products)],
            'Segment_3': [self.random_choice(['Construction', 'Professional', 'Enterprise']) for _ in range(num_products)]
        }
        data = self.add_extra_columns(data, num_products)
        self.write_csv(data, "cproduit.csv", "reference")
    
    def generate_segmentation(self):
        """Generate segmentation tables."""
        # prdpfa1
        data = {
            'CMARCH': ['6'] * 200,
            'CPROD': [f'{i+1:04d}' for i in range(200)],
            'CSEG': ['2'] * 200,
            'CSSSEG': [f'{(i % 10) + 1:02d}' for i in range(200)],
            'LMARCH': ['CONSTRUCTION'] * 200,
            'LPROD': [f'Product {i+1}' for i in range(200)],
            'LSEG': ['CONSTRUCTION'] * 200,
            'LSSSEG': [f'Sub-segment {(i % 10) + 1}' for i in range(200)]
        }
        data = self.add_extra_columns(data, 200)
        self.write_csv(data, "prdpfa1.csv", "reference")
        self.write_csv(data, "prdpfa3.csv", "reference")
        
        # segmentprdt - FIXED: Use same product format as LOB (01001, 01002, etc.)
        # LOB creates products as: 01001, 01002, etc. (5 chars starting with 01)
        # Segmentation MUST match for join to work in emissions
        data = {
            'CPROD': [f'01{i+1:03d}' for i in range(300)],  # Matches LOB format: 01001-01300
            'CDPOLE': [self.random_choice(['1', '3']) for _ in range(300)],
            'CMARCH': ['6'] * 300,
            'CSEG': ['2'] * 300,
            'CSSSEG': [f'{(i % 10) + 1:02d}' for i in range(300)]
        }
        data = self.add_extra_columns(data, 300)
        self.write_csv(data, f"segmentprdt_{self.vision}.csv", "reference")
    
    def generate_clients(self, num_clients: int = 8000):
        """Generate client data."""
        clients = []
        for i in range(num_clients):
            siren = self.random_numeric_string(9)
            clients.append({
                'NOCLT': f'CLT{i+1:07d}',
                'CDSIRET': siren + self.random_numeric_string(5),
                'CDSIREN': siren,
                'CDNAF': self.random_choice(self.naf_2008_codes)  # Use centralized pool
            })
        
        self.client_ids = [c['NOCLT'] for c in clients]
        data = {col: [c[col] for c in clients] for col in clients[0].keys()}
        
        data = self.add_extra_columns(data, num_clients)
        self.write_csv(data, "clacent1.csv", "reference")
        self.write_csv(data, "clacent3.csv", "reference")
    
    def generate_management_points(self, num_points: int = 500):
        """Generate management point data."""
        data = {
            'PTGST': [f'PT{i+1:05d}' for i in range(num_points)],
            'UPPER_MID': [self.random_choice(['MID', 'UPPER', 'LARGE', 'SME']) for _ in range(num_points)]
        }
        self.management_points = data['PTGST']
        data = self.add_extra_columns(data, num_points)
        self.write_csv(data, f"ptgst_{self.vision}.csv", "reference")
        
        data_static = {
            'PTGST': self.management_points,
            'REGION': [self.random_choice(['IDF', 'PACA', 'ARA', 'OCCITANIE']) for _ in range(num_points)],
            'P_NUM': [f'P{i+1:04d}' for i in range(num_points)]
        }
        data_static = self.add_extra_columns(data_static, num_points)
        self.write_csv(data_static, "ptgst.csv", "reference")
    
    def generate_other_references(self):
        """Generate other reference files."""
        # PRDCAP
        data = {
            'CDPROD': [f'01{i+1:03d}' for i in range(100)],
            'LBTPROD': [f'Product Type {i+1}' for i in range(100)]
        }
        data = self.add_extra_columns(data, 100)
        self.write_csv(data, "prdcap.csv", "reference")
        
        # GARANTCU
        policy_sample = [f'POL{i+1:08d}' for i in range(100)]
        data = {
            'POLICE': [random.choice(policy_sample) for _ in range(300)],
            'GARANTIE': [self.random_choice(['IRD', 'PE', 'RD', 'RC', 'DO', 'TRC']) for _ in range(300)],
            'BRANCHE': [self.random_choice(['IP0', 'ID0', 'RC0', 'DO0']) for _ in range(300)]
        }
        data = self.add_extra_columns(data, 300)
        self.write_csv(data, "garantcu.csv", "reference")
        
        # CATMIN
        data = {
            'PRODUIT': [f'01{i+1:03d}' for i in range(200)],
            'GARANTIE': [self.random_choice(['IRD', 'PE', 'RD', 'RC', 'DO']) for _ in range(200)],
            'CATMIN5': [self.random_choice(['CAT1', 'CAT2', 'CAT3']) for _ in range(200)]
        }
        data = self.add_extra_columns(data, 200)
        self.write_csv(data, "import_catmin.csv", "reference")
        
        # W6_BASECLI_INV
        data = {
            'NOCLT': random.choices(self.client_ids, k=15000),
            'CDAPET': [self.random_choice(self.naf_2008_codes) for _ in range(15000)]  # Use centralized pool
        }
        data = self.add_extra_columns(data, 15000)
        self.write_csv(data, "basecli_inv.csv", "reference")
        
        # BINSEE
        data = {
            'CDSIREN': [self.random_numeric_string(9) for _ in range(10000)],
            'CDNOTE': [self.random_choice(['A+', 'A', 'B', 'C']) for _ in range(10000)],
            'DTDEB_VALID': [self.random_date(self.one_year_ago, self.vision_start) for _ in range(10000)],
            'DTFIN_VALID': [self.random_date(self.vision_end, self.year_end) for _ in range(10000)]
        }
        data = self.add_extra_columns(data, 10000)
        self.write_csv(data, "histo_note_risque.csv", "reference")
    
    # ========================================================================
    # IMS AZ MONTHLY DATA GENERATORS
    # ========================================================================
    
    def generate_ipf_az(self, num_rows: int = 15000, pole: str = "1"):
        """Generate IPF AZ data (ipf16 or ipf36) with CORRECT business values."""
        
        filename = "ipf16.csv" if pole == "1" else "ipf36.csv"
        policy_numbers = [f'POL{i+1:08d}' for i in range(num_rows)]
        
        # Generate excluded intermediaries
        excluded_noint = [
            "H90061", "482001", "489090", "102030", "H90036", "H90059", "H99045",
            "H99059", "5B2000", "446000", "5R0001", "446118", "4F1004", "4A1400",
            "4A1500", "4A1600", "4A1700", "4A1800", "4A1900", "4L1010"
        ]
        all_intermediaries = [f'INT{i:05d}' for i in range(1, 301)]
        valid_intermediaries = [x for x in all_intermediaries if x not in excluded_noint]
        
        data = {
            # Core identifiers
            'CDPROD': [self.random_choice(self.product_codes) for _ in range(num_rows)],
            'NOPOL': policy_numbers,
            'NOCLT': [self.random_choice(self.client_ids) for _ in range(num_rows)],
            'NMCLT': [f'Client Name {i+1}' for i in range(num_rows)],
            'NOINT': [self.random_choice(valid_intermediaries) for _ in range(num_rows)],
            'NMACTA': [f'Actor Name {i+1}' for i in range(num_rows)],
            
            # Address
            'POSACTA': [self.random_numeric_string(5) for _ in range(num_rows)],
            'RUEACTA': [f'{random.randint(1,200)} Rue Example' for _ in range(num_rows)],
            'CEDIACTA': [f'{random.randint(1,999):03d}' for _ in range(num_rows)],
            
            # Dates
            'DTCREPOL': [self.random_date(self.one_year_ago, self.vision_start) for _ in range(num_rows)],
            'DTEFFAN': [self.random_date(self.year_start, self.vision_end) for _ in range(num_rows)],
            'DTTRAAN': ['' if random.random() > 0.1 else self.random_date(self.year_start, self.vision_end) for _ in range(num_rows)],
            'DTRESILP': ['' if random.random() > 0.15 else self.random_date(self.vision_end, self.year_end) for _ in range(num_rows)],
            'DTTRAAR': [''] * num_rows,
            'DTTYPLI1': [''] * num_rows,
            'DTTYPLI2': [''] * num_rows,
            'DTTYPLI3': [''] * num_rows,
            'DTECHANN': [''] * num_rows,
            'DTOUCHAN': [''] * num_rows,
            'DTRCPPR': [''] * num_rows,
            'DTRECTRX': [''] * num_rows,
            'DTRCPRE': [''] * num_rows,
            'DTEFSITT': [self.random_date(self.year_start, self.vision_end) for _ in range(num_rows)],
            
            # CRITICAL BUSINESS RULE COMPLIANT VALUES
            'CDNATP': [self.random_choice(['R', 'O', 'T', 'C']) for _ in range(num_rows)],  # Valid natures
            'TXCEDE': [self.random_double(0, 30) for _ in range(num_rows)],
            'PTGST': [self.random_choice(self.management_points) for _ in range(num_rows)],
            'CMARCH': ['6'] * num_rows,  # ✓ CONSTRUCTION MARKET
            'CDSITP': [self.random_choice(['1', '2', '3']) for _ in range(num_rows)],  # ✓ Exclude 4, 5
            'CSEGT': ['2'] * num_rows,  # ✓✓✓ CONSTRUCTION SEGMENT (NOT '1'!)
            'CSSEGT': [f'{random.randint(1,10):02d}' for _ in range(num_rows)],
            'CDRI': [self.random_choice(['A', 'B', 'C', 'D', 'E']) for _ in range(num_rows)],  # ✓ Exclude 'X'
            
            # Type liaison
            'CDTYPLI1': [''] * num_rows,
            'CDTYPLI2': [''] * num_rows,
            'CDTYPLI3': [''] * num_rows,
            
            # Financial
            'MTPRPRTO': [self.random_double(1000, 50000) for _ in range(num_rows)],
            'PRCDCIE': [self.random_double(80, 100) for _ in range(num_rows)],
            'MTCAF': [self.random_double(0, 10000) for _ in range(num_rows)],
            'FNCMACA': [self.random_double(0, 5000) for _ in range(num_rows)],
            'MTSMPR': [self.random_double(0, 2000) for _ in range(num_rows)],
            
            # Coassurance
            'CDPOLQPL': [self.random_choice(['1', '0'], weights=[0.2, 0.8]) for _ in range(num_rows)],
            'CDTPCOA': [self.random_choice(['3', '4', '5', '6', '8', '']) for _ in range(num_rows)],
            'CDCIEORI': [''] * num_rows,
            
            # Revision
            'CDPOLRVI': [self.random_choice(['1', '0']) for _ in range(num_rows)],
            'CDGREV': [self.random_choice(['20', '21', '40', '41', '42', '']) for _ in range(num_rows)],
            
            # Management
            'CDSITMGR': [''] * num_rows,
            'CDGECENT': [''] * num_rows,
            'CDMOTRES': [''] * num_rows,
            'NOPOLLI1': [''] * num_rows,
            'CDCASRES': [''] * num_rows,
            
            # Risk
            'CDFRACT': [''] * num_rows,
            'QUARISQ': [''] * num_rows,
            'NMRISQ': [f'Risk {i+1}' for i in range(num_rows)],
            'NMSRISQ': [''] * num_rows,
            'RESRISQ': [''] * num_rows,
            'RUERISQ': [f'{random.randint(1,200)} Rue Risk' for i in range(num_rows)],
            'LIDIRISQ': [''] * num_rows,
            'POSRISQ': [self.random_numeric_string(5) for _ in range(num_rows)],
            'VILRISQ': [self.random_choice(['PARIS', 'LYON', 'MARSEILLE']) for _ in range(num_rows)],
            'CDREG': [self.random_choice(['11', '13', '75', '84']) for _ in range(num_rows)],
            
            # NAF / Activity
            'CDNAF': [self.random_choice(self.naf_2008_codes) for _ in range(num_rows)],
            'CDTRE': [self.random_choice(['T01', 'T02', 'T03']) for _ in range(num_rows)],
            'CDACTPRO': [''] * num_rows,
            'ACTPRIN': [self.random_choice(self.actprin_codes) for _ in range(num_rows)],
            
            # Risk type fields (NOT IRD - these are in base IPF data)
            'tydris1': [self.random_choice(['QAW', 'QBJ', 'QBK', 'QBB', 'QBM', 'QXX']) for _ in range(num_rows)],  # LTA type indicator
            'OPAPOFFR': [self.random_choice(['O', 'N']) for _ in range(num_rows)],  # Offer participation
            
            # IRD - These fields are DELIBERATELY EMPTY because:
            # 1. CTDEFTRA/CTPRVTRV/LBNATTRV are IRD transaction codes, enriched from separate IRD files
            # 2. DSTCSC is construction site classification, joined from DO_DEST reference
            # 3. LBQLTSOU is quality source label, typically null in base IPF data
            # The IPF files contain policy master data; IRD enrichment happens in separate IRD_RISK files
            'CTDEFTRA': [''] * num_rows,
            'CTPRVTRV': [''] * num_rows,
            'LBNATTRV': [''] * num_rows,
            'DSTCSC': [''] * num_rows,
            'LBQLTSOU': [''] * num_rows,
        }
        
        
        # Add 14 pairs of capital fields with REALISTIC distribution matching extraction config
        # Match keywords from az_transformations.json capital_extraction
        capital_keywords = {
            'smp': ['SMP GLOBAL DU CONTRAT', 'SMP RETENU', 'SINISTRE MAXIMUM POSSIBLE'],
            'lci': ['LCI GLOBAL DU CONTRAT', 'CAPITAL REFERENCE OU LCI', 'LCI GLOBALE', 'LCI TOTAL'],
            'perte_exp': ['PERTE EXPLOITATION (MARGE BRUTE)', 'PERTE D EXPLOITATION', 'CAPITAUX TOTAUX P.E.', 'PE TOTALE'],
            'risque_direct': ['RISQUE DIRECT', 'CAPITAUX DOMMAGES DIR', 'DOMMAGES DIRECTS', 'RD TOTAL'],
            'valeur_neuf': ['VALEUR A NEUF', 'VAN', 'VALEUR NEUF'],
            'rc_labels': ['RC PRO', 'RC EXPLOITATION', 'RC DECENNALE', 'RCPRO'],
            'do_trc': ['DOMMAGES OUVRAGE', 'TOUS RISQUES CHANTIER', 'TRC', 'DO']
        }
        
        all_keywords = (
            capital_keywords['smp'] + capital_keywords['lci'] + 
            capital_keywords['perte_exp'] + capital_keywords['risque_direct'] +
            capital_keywords['valeur_neuf'] + capital_keywords['rc_labels'] + capital_keywords['do_trc']
        )
        
        for i in range(1, 15):
            # For each policy, decide capital field distribution
            lbcapi_values = []
            mtcapi_values = []
            
            for _ in range(num_rows):
                # 70% chance to have a capital value
                if random.random() < 0.7:
                    # Weighted distribution favoring SMP/LCI (most important for extraction)
                    keyword_type = self.random_choice(
                        ['smp', 'lci', 'perte_exp', 'risque_direct', 'valeur_neuf', 'rc_labels', 'do_trc'],
                        weights=[0.25, 0.25, 0.15, 0.12, 0.08, 0.08, 0.07]
                    )
                    label = self.random_choice(capital_keywords[keyword_type])
                    amount = self.random_double(10000, 500000)
                else:
                    label = ''
                    amount = 0
                
                lbcapi_values.append(label)
                mtcapi_values.append(amount)
            
            data[f'LBCAPI{i}'] = lbcapi_values
            data[f'MTCAPI{i}'] = mtcapi_values
        
        
        # Add provider and indexation fields (14 pairs)
        # CDPRVB: Provider/coverage codes - used for indexation source tracking
        # PRPRVC: Indexation coefficients - evolution rates from first year (0.95-1.15 range)
        
        provider_codes = ['FFB', 'BTP', 'FNTP', 'CAPEB', 'SCOP', 'USI', 'FMB', '']  # Construction industry providers
        
        for i in range(1, 15):
            # CDPRVB: Provider codes (empty for ~70% of fields, actual codes for 30%)
            # Schema: StringType - matches IPF_AZ_SCHEMA L158-171
            cdprvb_values = []
            for _ in range(num_rows):
                if random.random() < 0.3:  # 30% have provider codes
                    cdprvb_values.append(self.random_choice(provider_codes[:-1]))  # Exclude empty
                else:
                    cdprvb_values.append('')  # 70% empty
            
            data[f'CDPRVB{i}'] = cdprvb_values
            
            # PRPRVC: Indexation coefficients (evolution rates)
            # Schema: DoubleType - matches IPF_AZ_SCHEMA L172-185
            # Range: 0.95-1.15 (typical construction cost evolution ±5-15% over contract life)
            # Used by indexation_v2.sas to adjust capital values over time
            prprvc_values = []
            for _ in range(num_rows):
                # 40% no indexation (coef = 1.0)
                # 60% with indexation (0.95-1.15 range)
                if random.random() < 0.4:
                    prprvc_values.append(1.0)  # No evolution
                else:
                    # Realistic construction cost evolution
                    prprvc_values.append(self.random_double(0.95, 1.15, 4))  # 4 decimals precision
            
            data[f'PRPRVC{i}'] = prprvc_values

        
        data = self.add_extra_columns(data, num_rows, 8)
        self.write_csv(data, filename, "monthly")
        
        self.policy_numbers = policy_numbers
    
    def generate_ipfm99_az(self, num_rows: int = 500):
        """Generate IPFM99 files for product 01099 special CA handling."""
        # Generate for both poles
        for pole, prefix in [("1", "3SPEIPFM99"), ("3", "E1SPEIPFM99")]:
            # Select policies that use product 01099
            selected_policies = random.choices(self.policy_numbers, k=num_rows)
            
            data = {
                'CDPROD': ['01099'] * num_rows,  # Special CA product
                'NOPOL': selected_policies,
                'NOINT': [self.random_choice([f'INT{i:05d}' for i in range(1, 301)]) for _ in range(num_rows)],
                'CDACPR1': [self.random_choice(['ACT01', 'ACT02', 'ACT03', 'ACT04']) for _ in range(num_rows)],
                'CDACPR2': [self.random_choice(['ACT01', 'ACT02', '']) for _ in range(num_rows)],
                'MTCA': [self.random_double(0, 50000) for _ in range(num_rows)],
                'MTCAENP': [self.random_double(0, 20000) for _ in range(num_rows)],
                'MTCASST': [self.random_double(0, 15000) for _ in range(num_rows)],
                'MTCAVNT': [self.random_double(0, 10000) for _ in range(num_rows)]
            }
            
            data = self.add_extra_columns(data, num_rows, 3)
            filename = f"{prefix}_IPF_{self.vision}.csv.gz"
            self.write_csv(data, filename, "monthly", compression="gzip")
    
    def generate_ipfspe_files(self):
        """Generate IPFSPE special product activity files."""
        num_rows = 800
        selected_policies = random.choices(self.policy_numbers, k=num_rows)
        
        # IPFM0024 - Activity codes for special products
        data = {
            'NOPOL': selected_policies,
            'NOINT': [self.random_choice([f'INT{i:05d}' for i in range(1, 301)]) for _ in range(num_rows)],
            'CDPROD': [self.random_choice(self.product_codes) for _ in range(num_rows)],
            'CDACTPRF01': [self.random_choice(['ACT01', 'ACT02', 'ACT03', 'ACT04', 'ACT05']) for _ in range(num_rows)],
            'CDACTPRF02': [self.random_choice(['ACT01', 'ACT02', 'ACT03', '']) for _ in range(num_rows)]
        }
        data = self.add_extra_columns(data, num_rows, 3)
        self.write_csv(data, f"1IPFM0024_{self.vision}.csv", "monthly")
        
        # IPFM63 - Professional activities
        data = {
            'NOPOL': selected_policies,
            'NOINT': [self.random_choice([f'INT{i:05d}' for i in range(1, 301)]) for _ in range(num_rows)],
            'CDPROD': [self.random_choice(self.product_codes) for _ in range(num_rows)],
            'ACTPRIN': [self.random_choice(self.actprin_codes) for _ in range(num_rows)],  # Use centralized pool
            'ACTSEC1': [self.random_choice(self.actprin_codes + ['']) for _ in range(num_rows)],
            'CDNAF': [self.random_choice(self.naf_2008_codes) for _ in range(num_rows)],  # Use centralized pool
            'MTCA1': [self.random_double(0, 100000) for _ in range(num_rows)]
        }
        data = self.add_extra_columns(data, num_rows, 3)
        self.write_csv(data, f"1IPFM63_{self.vision}.csv", "monthly")
        
        # IPFM99 - Generic activities
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
    
    def generate_isic_tables(self):
        """Generate all ISIC mapping and codification tables."""
        
        # Use centralized codes for consistency
        isic_local_codes = [f'ISIC{i:03d}' for i in range(1, 101)]
        isic_global_codes = [f'GISIC{i:02d}' for i in range(1, 51)]
        cdtre_codes = ['T01', 'T02', 'T03', 'T04', 'T05']
        destinat_codes = ['Habitation', 'Autres', 'Bureaux', 'Commercial', 'Industriel']
        
        # 1. mapping_isic_const_act_202305.csv - Activity-based ISIC
        num_rows = 100
        data = {
            'ACTPRIN': random.choices(self.actprin_codes, k=num_rows),  # Centralized
            'CDNAF08': random.choices(self.naf_2008_codes, k=num_rows),  # Production uses CDNAF08
            'CDTRE': random.choices(cdtre_codes, k=num_rows),
            'CDNAF03': random.choices(self.naf_2003_codes, k=num_rows),  # Production uses CDNAF03
            'CDISIC': random.choices(isic_local_codes, k=num_rows)
        }
        data = self.add_extra_columns(data, num_rows, 2)
        self.write_csv(data, "mapping_isic_const_act_202305.csv", "reference")
        
        # 2. mapping_isic_const_cht_202305.csv - Construction site destination
        num_rows = 50
        data = {
            'DESTI_ISIC': random.choices(destinat_codes, k=num_rows),
            'CDNAF08': random.choices(self.naf_2008_codes, k=num_rows),  # Production uses CDNAF08
            'CDTRE': random.choices(cdtre_codes, k=num_rows),
            'CDNAF03': random.choices(self.naf_2003_codes, k=num_rows),  # Production uses CDNAF03
            'CDISIC': random.choices(isic_local_codes, k=num_rows)
        }
        data = self.add_extra_columns(data, num_rows, 2)
        self.write_csv(data, "mapping_isic_const_cht_202305.csv", "reference")
        
        # 3. mapping_cdnaf2003_isic_202305.csv - NAF 2003 to ISIC
        num_rows = 200
        data = {
            'CDNAF_2003': random.choices(self.naf_2003_codes, k=num_rows),  # With underscore per SAS (line 148)
            'ISIC_CODE': random.choices(isic_local_codes, k=num_rows)
        }
        data = self.add_extra_columns(data, num_rows, 2)
        self.write_csv(data, "mapping_cdnaf2003_isic_202305.csv", "reference")
        
        # 4. mapping_cdnaf2008_isic_202305.csv - NAF 2008 to ISIC
        num_rows = 200
        data = {
            'CDNAF_2008': random.choices(self.naf_2008_codes * 20, k=num_rows),  # With underscore per SAS join
            'ISIC_CODE': random.choices(isic_local_codes, k=num_rows)
        }
        data = self.add_extra_columns(data, num_rows, 2)
        self.write_csv(data, "mapping_cdnaf2008_isic_202305.csv", "reference")
        
        # 5. table_isic_tre_naf_202305.csv - ISIC hazard grades
        num_rows = 100
        hazard_grades = ['A', 'B', 'C', 'D', 'E']
        data = {
            'ISIC_CODE': isic_local_codes,
            'HAZARD_GRADES_FIRE': random.choices(hazard_grades, k=num_rows),
            'HAZARD_GRADES_BI': random.choices(hazard_grades, k=num_rows),
            'HAZARD_GRADES_RCA': random.choices(hazard_grades, k=num_rows),
            'HAZARD_GRADES_RCE': random.choices(hazard_grades, k=num_rows),
            'HAZARD_GRADES_TRC': random.choices(hazard_grades, k=num_rows),
            'HAZARD_GRADES_RCD': random.choices(hazard_grades, k=num_rows),
            'HAZARD_GRADES_DO': random.choices(hazard_grades, k=num_rows)
        }
        data = self.add_extra_columns(data, num_rows, 2)
        self.write_csv(data, "table_isic_tre_naf_202305.csv", "reference")
        
        # 6. ird_suivi_engagements_202511.csv - IRD NAF tracking
        num_rows = 1000
        selected_policies = random.choices(self.policy_numbers, k=num_rows)
        data = {
            'NOPOL': selected_policies,
            'CDPROD': random.choices(self.product_codes, k=num_rows),
            'CDNAF08': random.choices(self.naf_2008_codes * 100, k=num_rows),  # Production uses CDNAF08
            'CDISIC': random.choices(isic_local_codes, k=num_rows)
        }
        data = self.add_extra_columns(data, num_rows, 2)
        self.write_csv(data, "ird_suivi_engagements_202511.csv", "reference")
        
        # 7. isic_lg_202306.csv - Local-to-Global ISIC mapping
        num_rows = 50
        data = {
            'ISIC_Local': isic_local_codes[:num_rows],
            'ISIC_Global': random.choices(isic_global_codes, k=num_rows)
        }
        data = self.add_extra_columns(data, num_rows, 2)
        self.write_csv(data, "isic_lg_202306.csv", "reference")
    
    
    
    
    # ========================================================================
    # AZEC DATA GENERATORS
    # ========================================================================
    
    def generate_azec_files(self):
        """Generate all AZEC reference files."""
        
        num_azec = 800
        azec_policies = [f'AZ{i+1:07d}' for i in range(num_azec)]
        
        # POLIC_CU - Use products that exist in segmentation (CPROD 0001-0300)
        data = {
            'POLICE': azec_policies,
            'PRODUIT': [f'{random.randint(1, 300):04d}' for _ in range(num_azec)],  # Match segmentation range
            'INTERMED': [self.random_numeric_string(5) for _ in range(num_azec)],
            'POINGEST': [f'P{random.randint(1,500):04d}' for _ in range(num_azec)],
            'NOMCLI': [f'AZEC Client {i+1}' for i in range(num_azec)],
            'EFFETPOL': [self.random_date(self.one_year_ago, self.vision_start) for _ in range(num_azec)],
            'DATFIN': ['' if random.random() > 0.2 else self.random_date(self.vision_end, self.year_end) for _ in range(num_azec)],
            'DATRESIL': ['' if random.random() > 0.15 else self.random_date(self.vision_end, self.year_end) for _ in range(num_azec)],
            'DATAFN': [self.random_date(self.vision_start, self.year_end) for _ in range(num_azec)],
            'DATEXPIR': [self.random_date(self.vision_end, self.year_end) for _ in range(num_azec)],
            'FINPOL': [''] * num_azec,
            'DATTERME': [self.random_date(self.year_end, self.year_end) for _ in range(num_azec)],
            'ETATPOL': [self.random_choice(['E', 'R'], weights=[0.7, 0.3]) for _ in range(num_azec)],
            'DUREE': [self.random_choice(['00', '01', '02']) for _ in range(num_azec)],
            'CODECOAS': [self.random_choice(['1', '0']) for _ in range(num_azec)],
            'PRIME': [self.random_double(1000, 50000) for _ in range(num_azec)],
            'PARTBRUT': [self.random_double(80, 100) for _ in range(num_azec)],
            'CPCUA': [self.random_double(80, 100) for _ in range(num_azec)],
            'MOTIFRES': [''] * num_azec,
            'ORIGRES': [''] * num_azec,
            'TYPCONTR': [self.random_choice(['CPORTEFEUILLE', 'NORMAL']) for _ in range(num_azec)],
            'RMPLCANT': [''] * num_azec,
            'GESTSIT': [self.random_choice(['', 'MIGRAZ'], weights=[0.9, 0.1]) for _ in range(num_azec)],
            'ECHEANMM': [f'{random.randint(1,12):02d}' for _ in range(num_azec)],
            'ECHEANJJ': [f'{random.randint(1,28):02d}' for _ in range(num_azec)],
            'INDREGUL': [self.random_choice(['O', 'N']) for _ in range(num_azec)],
        }
        data = self.add_extra_columns(data, num_azec)
        self.write_csv(data, "polic_cu.csv", "reference")
        
        # CAPITXCU - Use products that exist in segmentation (CPROD 0001-0300 with CMARCH=6)
        num_cap = num_azec * 2
        data = {
            'POLICE': random.choices(azec_policies, k=num_cap),
            'PRODUIT': [f'{random.randint(1, 300):04d}' for _ in range(num_cap)],  # Match segmentation range
            'SMP_SRE': [self.random_choice(['SMP', 'LCI']) for _ in range(num_cap)],
            'BRCH_REA': [self.random_choice(['IP0', 'ID0']) for _ in range(num_cap)],
            'CAPX_100': [self.random_double(10000, 500000) for _ in range(num_cap)],
            'CAPX_CUA': [self.random_double(8000, 400000) for _ in range(num_cap)]
        }
        data = self.add_extra_columns(data, num_cap)
        self.write_csv(data, "capitxcu.csv", "reference")
        
        # INCENDCU - Same fix for product codes
        data = {
            'POLICE': random.choices(azec_policies, k=600),
            'PRODUIT': [f'{random.randint(1, 300):04d}' for _ in range(600)],  # Match segmentation range
            'COD_NAF': ['4120A'] * 600,
            'COD_TRE': ['T01'] * 600,
            'MT_BASPE': [10000.0] * 600,
            'MT_BASDI': [20000.0] * 600
        }
        data = self.add_extra_columns(data, 600)
        self.write_csv(data, "incendcu.csv", "reference")
        
        # CONSTRCU - Same fix for product codes
        data = {
            'POLICE': random.choices(azec_policies, k=400),
            'PRODUIT': [f'{random.randint(1, 300):04d}' for _ in range(400)],  # Match segmentation range
            'DATFINCH': ['2025-12-31'] * 400,
            'DATOUVCH': ['2025-01-01'] * 400,
            'DATRECEP': [''] * 400,
            'DEST_LOC': ['H'] * 400,
            'FORMULE': ['F1'] * 400,
            'LDESTLOC': ['Habitation'] * 400,
            'LQUALITE': [''] * 400,
            'LTYPMAR1': [''] * 400,
            'MNT_GLOB': [100000.0] * 400,
            'NAT_CNT': [''] * 400,
            'TYPMARC1': [''] * 400
        }
        data = self.add_extra_columns(data, 400)
        self.write_csv(data, "constrcu.csv", "reference")
        
        # RCENTCU, RISTECCU
        for fname in ['rcentcu.csv', 'risteccu.csv']:
            data = {
                'POLICE': random.choices(azec_policies, k=300),
                'COD_NAF': ['4120A'] * 300,
                'FORMULE': ['F1'] * 300,
                'FORMULE2': [''] * 300,
                'FORMULE3': [''] * 300,
                'FORMULE4': [''] * 300
            }
            data = self.add_extra_columns(data, 300)
            self.write_csv(data, fname, "reference")
        
        # MULPROCU
        data = {
            'POLICE': random.choices(azec_policies, k=200),
            'CHIFFAFF': [50000.0] * 200
        }
        data = self.add_extra_columns(data, 200)
        self.write_csv(data, "mulprocu.csv", "reference")
        
        # MPACU
        data = {
            'POLICE': random.choices(azec_policies, k=150),
            'COD_NAF': ['4120A'] * 150
        }
        data = self.add_extra_columns(data, 150)
        self.write_csv(data, "mpacu.csv", "reference")
        
        # TABLE_SEGMENTATION_AZEC_MML
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
        
        # CONSTRCU_AZEC_SEGMENT - AZEC construction segment reference
        num_seg = 400
        azec_policy_sample = random.choices(azec_policies, k=num_seg)
        data = {
            'CDPROD': random.choices(['A00', 'A01', 'AA1', 'MA1', 'MP1'], k=num_seg),
            'POLICE': azec_policy_sample,
            'SEGMENT': random.choices(['PME', 'GE', 'PROF', 'TPE'], k=num_seg),
            'TYPE_PRODUIT': random.choices(['MRH', 'MRP', 'TRC', 'DO', 'RC'], k=num_seg)
        }
        data = self.add_extra_columns(data, num_seg, 2)
        self.write_csv(data, "constrcu_azec_segment.csv", "reference", separator=",")
    
    # ========================================================================
    # EMISSIONS DATA GENERATOR (ONE BI)
    # ========================================================================
    
    def generate_emissions_one_bi(self, num_rows: int = 20000):
        """
        Generate One BI premium data (rf_fr1_prm_dtl_midcorp_m).
        
        CRITICAL: All business filters must be satisfied to avoid empty results.
        Based on: schemas.py RF_FR1_PRM_DTL_MIDCORP_M_SCHEMA (15 columns)
        """
        
        # === VALID VALUES ONLY ===
        
        # Distribution channels (4 values only)
        distribution_channels = ['DCAG', 'DCPS', 'DIGITAL', 'BROKDIV']
        
        # Excluded intermediaries (from emissions_config.json)
        excluded_noint = [
            "102030", "446000", "446118", "446218", "482001", "489090", "500150",
            "4A1400", "4A1500", "4A1600", "4A1700", "4A1800", "4A1900",
            "4F1004", "5B2000", "5R0001",
            "H90036", "H90037", "H90059", "H90061", "H99045", "H99059"
        ]
        
        # Generate valid intermediaries (exclude the bad ones)
        all_intermediaries = [f'INT{i:05d}' for i in range(1, 301)]
        valid_intermediaries = [x for x in all_intermediaries if x not in excluded_noint]
        
        # Valid guarantee codes (exclude 180, 183, 184, 185)
        valid_guarantees = ['220', '240', '250', '260', '300', '310', '320']
        
        # Build 5-char prospective codes (format: XX{code}YY)
        guarantee_prospective_codes = [f'XX{g}YY' for g in valid_guarantees]
        
        # Valid categories (exclude 792, 793)
        valid_categories = [f'{i:03d}' for i in range(1, 800) if i not in [792, 793]]
        
        # Statuses
        statuses = ['ACT', 'CAN', 'SUS', 'VAL']
        
        # Vision year
        vision_year = self.year  # 2025 for vision 202509
        
        # === GENERATE DATA ===
        
        data = {
            # Distribution (REQUIRED for CDPOLE mapping)
            'CD_NIV_2_STC': [self.random_choice(distribution_channels, 
                                               weights=[0.30, 0.20, 0.10, 0.40]) 
                            for _ in range(num_rows)],
            
            # Identifiers
            'CD_INT_STC': [self.random_choice(valid_intermediaries) 
                          for _ in range(num_rows)],
            'NU_CNT_PRM': [self.random_choice(self.policy_numbers) 
                          for _ in range(num_rows)],
            'CD_PRD_PRM': [self.random_choice(self.product_codes) 
                          for _ in range(num_rows)],
            'CD_STATU_CTS': [self.random_choice(statuses) 
                            for _ in range(num_rows)],
            
            # Dates (all in YYYY-MM-DD format)
            'DT_CPTA_CTS': [self.random_date(self.year_start, self.vision_end) 
                           for _ in range(num_rows)],
            'DT_EMIS_CTS': [self.random_date(self.year_start, self.vision_start) 
                           for _ in range(num_rows)],
            'DT_ANNU_CTS': ['' if random.random() > 0.1 else 
                           self.random_date(self.vision_start, self.year_end) 
                           for _ in range(num_rows)],
            
            # Financial (ensure non-zero to pass filters)
            'MT_HT_CTS': [self.random_double(100, 50000) for _ in range(num_rows)],
            'MT_CMS_CTS': [self.random_double(10, 5000) for _ in range(num_rows)],
            
            # Categories (REQUIRED: exclude 792, 793)
            'CD_CAT_MIN': [self.random_choice(valid_categories) for _ in range(num_rows)],
            
            # Guarantees (CRITICAL: Use valid codes)
            'CD_GAR_PRINC': [self.random_choice(valid_guarantees) for _ in range(num_rows)],
            'CD_GAR_PROSPCTIV': [self.random_choice(guarantee_prospective_codes) 
                                for _ in range(num_rows)],
            
            # EXERCICE split (STRING type: "2023", "2024", "2025")
            'NU_EX_RATT_CTS': [str(self.random_choice(
                                  [vision_year, vision_year-1, vision_year-2],
                                  weights=[0.70, 0.25, 0.05]
                              )) for _ in range(num_rows)],
            
            # Market (REQUIRED for filter)
            'CD_MARCHE': ['6'] * num_rows,  # Construction market
        }
        
        # Write with COMMA separator (not pipe!)
        self.write_csv(
            data, 
            f"rf_fr1_prm_dtl_midcorp_m_{self.vision}.csv", 
            location_type="monthly",
            separator=","  # ← IMPORTANT: Use comma, not pipe!
        )
    
    # ========================================================================
    # IRD AND OTHER FILES
    # ========================================================================
    
    def generate_ird_and_others(self):
        """Generate IRD risk files and other required files."""
        
        # IRD Q45, Q46 - Use vision-specific naming
        for fname in [f'ird_risk_q45_{self.vision}.csv', f'ird_risk_q46_{self.vision}.csv']:
            data = {
                'NOPOL': random.choices(self.policy_numbers, k=2000),
                'DTOUCHAN': ['2025-09-15'] * 2000,
                'DTRECTRX': ['2025-09-15'] * 2000,
                'DTREFFIN': ['2025-12-31'] * 2000,
                'CTPRVTRV': [''] * 2000,
                'CTDEFTRA': [''] * 2000,
                'LBNATTRV': [''] * 2000,
                'LBDSTCSC': ['Habitation'] * 2000
            }
            data = self.add_extra_columns(data, 2000)
            self.write_csv(data, fname, "monthly")
        
        # IRD QAN
        data = {
            'NOPOL': random.choices(self.policy_numbers, k=1000),
            'DTOUCHAN': ['2025-09-15'] * 1000,
            'CTPRVTRV': [''] * 1000,
            'CTDEFTRA': [''] * 1000,
            'LBNATTRV': [''] * 1000
        }
        data = self.add_extra_columns(data, 1000)
        self.write_csv(data, f"ird_risk_qan_{self.vision}.csv", "monthly")
        
        # DO_DEST
        data = {
            'NOPOL': random.choices(self.policy_numbers, k=1000),
            'DESTINAT': ['Habitation'] * 1000
        }
        data = self.add_extra_columns(data, 1000)
        self.write_csv(data, "do_dest.csv", "reference", separator=",")
        
        # REF_MIG_AZEC_VS_IMS
        data = {
            'NOPOL_AZEC': [f'AZ{i+1:07d}' for i in range(100)],
            'NOPOL_IMS': [f'POL{i+1000:08d}' for i in range(100)],
            'DATE_MIG': ['2024-01-01'] * 100
        }
        data = self.add_extra_columns(data, 100)
        self.write_csv(data, "ref_mig_azec_vs_ims.csv", "reference", separator=",")
        
        # INDICES
        data = {
            'annee': ['2024']*12 + ['2025']*12,
            'mois': [f'{i:02d}' for i in range(1,13)]*2,
            'indice': [105.0] * 24
        }
        data = self.add_extra_columns(data, 24)
        self.write_csv(data, "indices.csv", "reference", separator=",")
    
    # ========================================================================
    # MAIN GENERATION ORCHESTRATOR
    # ========================================================================
    
    def generate_all(self):
        """Generate ALL required datasets."""
        
        print("\n" + "=" * 70)
        print(" UNIFIED PTF_MVT DATA GENERATOR")
        print("=" * 70)
        print(f" Vision: {self.vision}")
        print(f" Output: {self.output_path}")
        print("=" * 70)
        
        print("\n📦 PHASE 1: Reference Data (20+ files)")
        print("-" * 70)
        self.generate_lob(150)
        self.generate_cproduit(150)
        self.generate_segmentation()
        self.generate_clients(8000)
        self.generate_management_points(500)
        self.generate_other_references()
        
        print("\n📦 PHASE 2: IMS AZ Monthly Data (8 files)")
        print("-" * 70)
        self.generate_ipf_az(15000, pole="1")  # ipf16
        self.generate_ipf_az(15000, pole="3")  # ipf36
        self.generate_ipfm99_az(500)  # 2 compressed files for product 01099
        self.generate_ipfspe_files()  # 3 IPFSPE files
        
        print("\n📦 PHASE 3: ISIC Mapping Tables (7 files)")
        print("-" * 70)
        self.generate_isic_tables()
        
        print("\n📦 PHASE 4: AZEC Data (9 files)")
        print("-" * 70)
        self.generate_azec_files()
        
        print("\n📦 PHASE 5: EMISSIONS Data (1 file)")
        print("-" * 70)
        self.generate_emissions_one_bi(20000)
        
        print("\n📦 PHASE 6: IRD & Others (6 files)")
        print("-" * 70)
        self.generate_ird_and_others()
        
        print("\n" + "=" * 70)
        print(" ✅ DATA GENERATION COMPLETE!")
        print("=" * 70)
        print(f"\n📊 Check files at: {self.output_path}/bronze/")
        print("� Expected file count: 50+ files")
        print("   - Reference files: 35+ (bronze/ref/)")
        print("   - Monthly files: 15+ (bronze/2025/09/)")
        print("\n🔍 Key improvements:")
        print("   ✓ Capital fields with 50% SMP/LCI keywords")
        print("   ✓ Excluded product 01073")
        print("   ✓ Added IPFM99 for product 01099")
        print("   ✓ Added 3 IPFSPE activity files")
        print("   ✓ Added 7 ISIC mapping tables")
        print("   ✓ Vision-specific IRD naming")
        print("\n🚀 Next: Test pipeline in Docker environment")



# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    VISION = "202509"
    OUTPUT_PATH = "/workspace/datalake"  # Use this path when running in Docker
    # OUTPUT_PATH = "./datalake"  # Uncomment this for local testing outside Docker
    
    generator = DataGenerator(vision=VISION, output_path=OUTPUT_PATH)
    generator.generate_all()
