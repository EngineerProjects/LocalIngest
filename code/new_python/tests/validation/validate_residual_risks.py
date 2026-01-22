#!/usr/bin/env python3
"""
Validation des Risques Résiduels AZEC - SAS vs PySpark
======================================================

Ce script valide les 3 risques résiduels identifiés dans l'audit AZEC:
1. Config drift: JSON vs SAS hardcoded
2. CAPITXCU fail-fast robustness
3. Numeric precision (Double tolerance)

Author: Antigravity AI
Date: 2026-01-22
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Tuple


class AZECRiskValidator:
    """Validator for AZEC residual risks."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.config_path = project_root / "code/new_python/config/transformations/azec_transformations.json"
        self.sas_path = project_root / "code/sas/PTF_MVTS_AZEC_MACRO.sas"
        self.processor_path = project_root / "code/new_python/src/processors/ptf_mvt_processors/azec_processor.py"
        
        self.results = {
            "risk1_config_drift": {},
            "risk2_failfast": {},
            "risk3_precision": {}
        }
    
    # ========== RISK 1: CONFIG DRIFT ==========
    
    def validate_risk1_config_drift(self) -> Dict:
        """
        Validate JSON config matches SAS hardcoded values.
        
        Checks:
        - Capital mappings (8 rules)
        - Product list (46 products)
        """
        print("\n" + "="*70)
        print("RISK 1: CONFIG DRIFT (JSON vs SAS)")
        print("="*70)
        
        results = {}
        
        # Load JSON config
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Check 1.1: Capital Mappings
        print("\n[1.1] Validating Capital Mappings...")
        capital_result = self._validate_capital_mappings(config)
        results['capital_mappings'] = capital_result
        
        # Check 1.2: Product List
        print("\n[1.2] Validating Product List...")
        product_result = self._validate_product_list(config)
        results['product_list'] = product_result
        
        # Overall
        all_passed = capital_result['passed'] and product_result['passed']
        results['overall_status'] = '✅ PASS' if all_passed else '❌ FAIL'
        
        self.results['risk1_config_drift'] = results
        return results
    
    def _validate_capital_mappings(self, config: Dict) -> Dict:
        """Validate capital mapping config vs SAS L387-408."""
        
        # Expected from SAS (L389-407)
        expected_mappings = [
            {"smp_sre": "LCI", "brch_rea": "IP0", "source": "capx_100", "target": "lci_pe_100"},
            {"smp_sre": "LCI", "brch_rea": "IP0", "source": "capx_cua", "target": "lci_pe_cie"},
            {"smp_sre": "LCI", "brch_rea": "ID0", "source": "capx_100", "target": "lci_dd_100"},
            {"smp_sre": "LCI", "brch_rea": "ID0", "source": "capx_cua", "target": "lci_dd_cie"},
            {"smp_sre": "SMP", "brch_rea": "IP0", "source": "capx_100", "target": "smp_pe_100"},
            {"smp_sre": "SMP", "brch_rea": "IP0", "source": "capx_cua", "target": "smp_pe_cie"},
            {"smp_sre": "SMP", "brch_rea": "ID0", "source": "capx_100", "target": "smp_dd_100"},
            {"smp_sre": "SMP", "brch_rea": "ID0", "source": "capx_cua", "target": "smp_dd_cie"},
        ]
        
        actual_mappings = config['capital_mapping']['mappings']
        
        # Normalize for comparison
        def normalize_mapping(m):
            return {
                "smp_sre": m["smp_sre"],
                "brch_rea": m["brch_rea"],
                "source": m["source"],
                "target": m["target"]
            }
        
        expected_set = [normalize_mapping(m) for m in expected_mappings]
        actual_set = [normalize_mapping(m) for m in actual_mappings]
        
        # Compare
        missing = [m for m in expected_set if m not in actual_set]
        extra = [m for m in actual_set if m not in expected_set]
        
        passed = len(missing) == 0 and len(extra) == 0 and len(actual_mappings) == 8
        
        print(f"   Expected count: 8")
        print(f"   Actual count: {len(actual_mappings)}")
        print(f"   Missing mappings: {len(missing)}")
        print(f"   Extra mappings: {len(extra)}")
        
        if missing:
            print(f"   ❌ Missing: {missing}")
        if extra:
            print(f"   ⚠️  Extra: {extra}")
        
        status = '✅ PASS' if passed else '❌ FAIL'
        print(f"   → Status: {status}")
        
        return {
            "passed": passed,
            "expected_count": 8,
            "actual_count": len(actual_mappings),
            "missing": missing,
            "extra": extra,
            "status": status
        }
    
    def _validate_product_list(self, config: Dict) -> Dict:
        """Validate product list config vs SAS L43."""
        
        # Load SAS file to extract product list
        with open(self.sas_path, 'r', encoding='latin-1') as f:
            sas_content = f.read()
        
        # Extract %let produit = (...)
        pattern = r"%let\s+produit\s*=\s*\((.*?)\);"
        match = re.search(pattern, sas_content, re.DOTALL | re.IGNORECASE)
        
        if not match:
            print("   ❌ Could not find %let produit in SAS file")
            return {"passed": False, "error": "Pattern not found"}
        
        # Parse products from SAS
        sas_products_str = match.group(1)
        sas_products = [p.strip().strip("'\"") for p in sas_products_str.split()]
        sas_products = [p for p in sas_products if p]  # Remove empty
        
        # Get products from JSON
        json_products = config['product_list']['products']
        
        # Compare
        sas_set = set(sas_products)
        json_set = set(json_products)
        
        missing = sas_set - json_set
        extra = json_set - sas_set
        
        passed = len(missing) == 0 and len(extra) == 0 and len(json_products) == len(sas_products)
        
        print(f"   SAS count: {len(sas_products)}")
        print(f"   JSON count: {len(json_products)}")
        print(f"   Missing from JSON: {len(missing)}")
        print(f"   Extra in JSON: {len(extra)}")
        
        if missing:
            print(f"   ❌ Missing: {sorted(missing)}")
        if extra:
            print(f"   ⚠️  Extra: {sorted(extra)}")
        
        status = '✅ PASS' if passed else '❌ FAIL'
        print(f"   → Status: {status}")
        
        return {
            "passed": passed,
            "sas_count": len(sas_products),
            "json_count": len(json_products),
            "missing": sorted(list(missing)),
            "extra": sorted(list(extra)),
            "status": status
        }
    
    # ========== RISK 2: CAPITXCU FAIL-FAST ==========
    
    def validate_risk2_failfast(self) -> Dict:
        """
        Validate CAPITXCU fail-fast robustness.
        
        Checks:
        - Empty mapping handling
        - Missing table handling
        - Missing columns handling
        - Empty sources handling
        - Proper error messages
        """
        print("\n" + "="*70)
        print("RISK 2: CAPITXCU FAIL-FAST ROBUSTNESS")
        print("="*70)
        
        results = {}
        
        # Load processor code
        with open(self.processor_path, 'r', encoding='utf-8') as f:
            processor_code = f.read()
        
        # Extract _join_capitals method (L488-608)
        method_pattern = r'def _join_capitals\(self.*?(?=\n    def |\nclass |\Z)'
        match = re.search(method_pattern, processor_code, re.DOTALL)
        
        if not match:
            print("   ❌ Could not find _join_capitals method")
            return {"passed": False, "error": "Method not found"}
        
        method_code = match.group(0)
        
        # Check 2.1: Empty mapping guard
        print("\n[2.1] Empty Mapping Guard...")
        has_empty_mapping_check = 'if not AZEC_CAPITAL_MAPPING:' in method_code
        has_empty_mapping_raise = 'AZEC_CAPITAL_MAPPING est vide' in method_code
        empty_mapping_passed = has_empty_mapping_check and has_empty_mapping_raise
        print(f"   Has empty check: {has_empty_mapping_check}")
        print(f"   Has error message: {has_empty_mapping_raise}")
        print(f"   → Status: {'✅ PASS' if empty_mapping_passed else '❌ FAIL'}")
        results['empty_mapping_guard'] = empty_mapping_passed
        
        # Check 2.2: Missing table handling
        print("\n[2.2] Missing Table Handling...")
        has_try_except = 'except FileNotFoundError' in method_code
        has_missing_raise = 'Missing required capital data: CAPITXCU' in method_code
        missing_table_passed = has_try_except and has_missing_raise
        print(f"   Has FileNotFoundError catch: {has_try_except}")
        print(f"   Has error message: {has_missing_raise}")
        print(f"   → Status: {'✅ PASS' if missing_table_passed else '❌ FAIL'}")
        results['missing_table_handling'] = missing_table_passed
        
        # Check 2.3: Missing key columns
        print("\n[2.3] Missing Key Columns Handling...")
        has_required_cols = 'required_key_cols' in method_code
        has_column_check = 'missing_keys' in method_code
        missing_cols_passed = has_required_cols and has_column_check
        print(f"   Defines required_key_cols: {has_required_cols}")
        print(f"   Checks missing_keys: {has_column_check}")
        print(f"   → Status: {'✅ PASS' if missing_cols_passed else '❌ FAIL'}")
        results['missing_columns_handling'] = missing_cols_passed
        
        # Check 2.4: Empty sources validation
        print("\n[2.4] Empty Sources Validation...")
        has_source_check = 'existing_sources' in method_code
        has_no_source_raise = 'Aucune colonne source mappée trouvée' in method_code or \
                              'Aucune colonne source de la mapping' in method_code
        empty_sources_passed = has_source_check and has_no_source_raise
        print(f"   Has existing_sources check: {has_source_check}")
        print(f"   Has error for no sources: {has_no_source_raise}")
        print(f"   → Status: {'✅ PASS' if empty_sources_passed else '❌ FAIL'}")
        results['empty_sources_validation'] = empty_sources_passed
        
        # Check 2.5: No targets created
        print("\n[2.5] No Targets Created Handling...")
        has_created_targets_check = 'if not created_targets:' in method_code
        has_no_targets_raise = 'Impossible de créer des colonnes cibles' in method_code
        no_targets_passed = has_created_targets_check and has_no_targets_raise
        print(f"   Has created_targets check: {has_created_targets_check}")
        print(f"   Has error message: {has_no_targets_raise}")
        print(f"   → Status: {'✅ PASS' if no_targets_passed else '❌ FAIL'}")
        results['no_targets_handling'] = no_targets_passed
        
        # Overall
        all_checks = [
            empty_mapping_passed,
            missing_table_passed,
            missing_cols_passed,
            empty_sources_passed,
            no_targets_passed
        ]
        overall_passed = all(all_checks)
        results['overall_status'] = '✅ PASS' if overall_passed else '❌ FAIL'
        results['passed'] = overall_passed
        
        print(f"\n   Overall fail-fast robustness: {results['overall_status']}")
        
        self.results['risk2_failfast'] = results
        return results
    
    # ========== RISK 3: NUMERIC PRECISION ==========
    
    def validate_risk3_precision(self) -> Dict:
        """
        Validate numeric precision handling.
        
        Checks:
        - NULL → 0 conversion in capitals
        - Coalesce usage
        - Double arithmetic (sum, addition)
        - Tolerance docs/comments
        """
        print("\n" + "="*70)
        print("RISK 3: NUMERIC PRECISION")
        print("="*70)
        
        results = {}
        
        # Load processor code
        with open(self.processor_path, 'r', encoding='utf-8') as f:
            processor_code = f.read()
        
        # Extract _join_capitals method
        method_pattern = r'def _join_capitals\(self.*?(?=\n    def |\nclass |\Z)'
        match = re.search(method_pattern, processor_code, re.DOTALL)
        
        if not match:
            print("   ❌ Could not find _join_capitals method")
            return {"passed": False, "error": "Method not found"}
        
        method_code = match.group(0)
        
        # Check 3.1: NULL → 0 conversion via coalesce
        print("\n[3.1] NULL → 0 Conversion (coalesce)...")
        has_coalesce = 'coalesce(col(src), lit(0))' in method_code
        coalesce_count = method_code.count('coalesce')
        print(f"   Uses coalesce(..., lit(0)): {has_coalesce}")
        print(f"   Coalesce usage count: {coalesce_count}")
        null_conversion_passed = has_coalesce and coalesce_count >= 1
        print(f"   → Status: {'✅ PASS' if null_conversion_passed else '❌ FAIL'}")
        results['null_conversion'] = null_conversion_passed
        
        # Check 3.2: SAS NULL handling match
        print("\n[3.2] SAS NULL Handling Match...")
        # SAS: if LCI_PE_100=. then LCI_PE_100=0; (L391-394, L401-404)
        # PySpark: coalesce(col(src), lit(0)).otherwise(lit(0))
        sas_null_comment = "SAS replaces NULL with 0 (L391-407)" in processor_code or \
                           "NULL → 0" in method_code
        has_otherwise_zero = '.otherwise(lit(0))' in method_code
        sas_match_passed = has_coalesce or has_otherwise_zero
        print(f"   Has explicit NULL → 0 logic: {sas_match_passed}")
        print(f"   → Status: {'✅ PASS' if sas_match_passed else '⚠️  WARNING'}")
        results['sas_null_match'] = sas_match_passed
        
        # Check 3.3: Aggregation functions
        print("\n[3.3] Aggregation Functions (spark_sum)...")
        has_spark_sum = 'spark_sum' in method_code
        has_sum_alias = 'spark_sum(c).alias(c)' in method_code
        aggregation_passed = has_spark_sum and has_sum_alias
        print(f"   Uses spark_sum: {has_spark_sum}")
        print(f"   Proper aliasing: {has_sum_alias}")
        print(f"   → Status: {'✅ PASS' if aggregation_passed else '❌ FAIL'}")
        results['aggregation'] = aggregation_passed
        
        # Check 3.4: Safe addition (avoid NULL propagation)
        print("\n[3.4] Safe Addition (safe_add helper)...")
        has_safe_add = 'def safe_add(' in method_code
        uses_safe_add = 'safe_add(df_cap_agg' in method_code
        safe_add_count = method_code.count('safe_add(df_cap_agg')
        safe_addition_passed = has_safe_add and uses_safe_add and safe_add_count == 4
        print(f"   Defines safe_add helper: {has_safe_add}")
        print(f"   Uses safe_add: {uses_safe_add}")
        print(f"   Safe_add calls (expected 4): {safe_add_count}")
        print(f"   → Status: {'✅ PASS' if safe_addition_passed else '❌ FAIL'}")
        results['safe_addition'] = safe_addition_passed
        
        # Check 3.5: Documentation about precision
        print("\n[3.5] Precision Documentation...")
        # Check if there are comments about tolerance or precision
        has_tolerance_doc = 'tolerance' in processor_code.lower() or \
                           '0.01' in processor_code
        print(f"   Has precision/tolerance docs: {has_tolerance_doc}")
        print(f"   → Status: {'✅ PASS' if has_tolerance_doc else '⚠️  INFO'}")
        results['precision_docs'] = has_tolerance_doc
        
        # Overall
        critical_checks = [
            null_conversion_passed,
            aggregation_passed,
            safe_addition_passed
        ]
        overall_passed = all(critical_checks)
        results['overall_status'] = '✅ PASS' if overall_passed else '❌ FAIL'
        results['passed'] = overall_passed
        
        print(f"\n   Overall precision handling: {results['overall_status']}")
        
        self.results['risk3_precision'] = results
        return results
    
    # ========== REPORTING ==========
    
    def generate_report(self) -> str:
        """Generate comprehensive validation report."""
        
        report = []
        report.append("\n" + "="*70)
        report.append("VALIDATION DES RISQUES RÉSIDUELS AZEC - RAPPORT FINAL")
        report.append("="*70)
        
        # Summary table
        report.append("\n## RÉSUMÉ")
        report.append("-" * 70)
        
        risk1_status = self.results['risk1_config_drift'].get('overall_status', '❓ PENDING')
        risk2_status = self.results['risk2_failfast'].get('overall_status', '❓ PENDING')
        risk3_status = self.results['risk3_precision'].get('overall_status', '❓ PENDING')
        
        report.append(f"Risk 1 (Config Drift):        {risk1_status}")
        report.append(f"Risk 2 (CAPITXCU Fail-Fast):  {risk2_status}")
        report.append(f"Risk 3 (Numeric Precision):   {risk3_status}")
        
        # Determine overall
        all_passed = all([
            '✅' in risk1_status,
            '✅' in risk2_status,
            '✅' in risk3_status
        ])
        
        overall = "✅ TOUS LES RISQUES RÉSIDUELS VALIDÉS" if all_passed else "❌ ÉCHECS DÉTECTÉS"
        report.append("-" * 70)
        report.append(f"STATUS GLOBAL: {overall}")
        report.append("="*70)
        
        # Details
        report.append("\n## DÉTAILS PAR RISQUE")
        
        # Risk 1
        report.append("\n### Risk 1: Config Drift (JSON vs SAS)")
        r1 = self.results['risk1_config_drift']
        if 'capital_mappings' in r1:
            cm = r1['capital_mappings']
            report.append(f"  - Capital Mappings: {cm['status']}")
            report.append(f"    Count: {cm['actual_count']}/8")
            if cm['missing']:
                report.append(f"    Missing: {cm['missing']}")
            if cm['extra']:
                report.append(f"    Extra: {cm['extra']}")
        
        if 'product_list' in r1:
            pl = r1['product_list']
            report.append(f"  - Product List: {pl['status']}")
            report.append(f"    Count: {pl['json_count']} (SAS: {pl['sas_count']})")
            if pl['missing']:
                report.append(f"    Missing: {pl['missing']}")
            if pl['extra']:
                report.append(f"    Extra: {pl['extra']}")
        
        # Risk 2
        report.append("\n### Risk 2: CAPITXCU Fail-Fast Robustness")
        r2 = self.results['risk2_failfast']
        for check, value in r2.items():
            if check not in ['overall_status', 'passed']:
                status = '✅' if value else '❌'
                report.append(f"  - {check}: {status}")
        
        # Risk 3
        report.append("\n### Risk 3: Numeric Precision")
        r3 = self.results['risk3_precision']
        for check, value in r3.items():
            if check not in ['overall_status', 'passed']:
                status = '✅' if value else '❌'
                report.append(f"  - {check}: {status}")
        
        # Recommendations
        report.append("\n## RECOMMANDATIONS")
        report.append("-" * 70)
        
        if not all_passed:
            report.append("⚠️  Corrections requises:")
            if '❌' in risk1_status:
                report.append("  1. Mettre à jour azec_transformations.json pour matcher SAS")
            if '❌' in risk2_status:
                report.append("  2. Renforcer les fail-fast guards dans _join_capitals()")
            if '❌' in risk3_status:
                report.append("  3. Améliorer la gestion de précision numérique")
        else:
            report.append("✅ Aucune correction requise.")
            report.append("   Tous les risques résiduels sont correctement mitigés.")
        
        report.append("\n" + "="*70)
        
        return "\n".join(report)
    
    def run_all(self) -> Dict:
        """Run all validations and generate report."""
        
        print("\n🚀 Démarrage de la validation des risques résiduels AZEC...\n")
        
        # Run all validations
        self.validate_risk1_config_drift()
        self.validate_risk2_failfast()
        self.validate_risk3_precision()
        
        # Generate report
        report = self.generate_report()
        print(report)
        
        # Save report
        report_path = self.project_root / "code/new_python/tests/validation/residual_risks_report.txt"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"\n📄 Rapport sauvegardé: {report_path}")
        
        return self.results


def main():
    """Main entry point."""
    project_root = Path(__file__).resolve().parents[4]
    
    validator = AZECRiskValidator(project_root)
    results = validator.run_all()
    
    # Exit code
    all_passed = all([
        results['risk1_config_drift'].get('passed', False),
        results['risk2_failfast'].get('passed', False),
        results['risk3_precision'].get('passed', False)
    ])
    
    exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
