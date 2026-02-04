#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test Script: Validation SAS DATETIME Parsing

Valide que _parse_sas_datetime_to_date() parse correctement
le format SAS DATETIME '14MAY1991:00:00:00' selon conformité SAS.

Usage:
    spark-submit scripts/test_sas_datetime_parsing.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from utils.transformations.enrichment.risk_enrichment import _parse_sas_datetime_to_date


def test_sas_datetime_parsing():
    """Test SAS DATETIME parsing with real-world examples."""
    
    print("\n" + "="*80)
    print("🧪 TEST VALIDATION: SAS DATETIME PARSING")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName("TestSASDATETIMEParsing") \
        .getOrCreate()
    
    # Test cases with expected results
    test_data = [
        # (input_datetime_string, expected_date_output, description)
        ("14MAY1991:00:00:00", "1991-05-14", "Normal date"),
        ("31DEC2012:00:00:00", "2012-12-31", "End of year"),
        ("01JAN2008:00:00:00", "2008-01-01", "Start of year"),
        ("28FEB2020:00:00:00", "2020-02-28", "February non-leap"),
        ("29FEB2020:00:00:00", "2020-02-29", "Leap year edge case"),
        ("30SEP1992:00:00:00", "1992-09-30", "End of month (Sep)"),
        ("15JUL1991:00:00:00", "1991-07-15", "Mid-month"),
        ("04MAR2012:00:00:00", "2012-03-04", "Single digit day"),
        ("12DEC2012:00:00:00", "2012-12-12", "Double digit month/day"),
        ("12JAN2008:00:00:00", "2008-01-12", "January mid-month"),
        (None, None, "NULL value"),
        ("", None, "Empty string"),
    ]
    
    # Create test DataFrame
    df = spark.createDataFrame(
        [(row[0], row[1], row[2]) for row in test_data],
        ["datetime_str", "expected_date", "description"]
    )
    
    # Apply parsing function
    df = df.withColumn(
        "parsed_date",
        _parse_sas_datetime_to_date(col("datetime_str"))
    )
    
    # Convert to comparable format
    from pyspark.sql.functions import date_format
    df = df.withColumn(
        "parsed_date_str",
        date_format(col("parsed_date"), "yyyy-MM-dd")
    )
    
    # Collect results
    results = df.select(
        "description",
        "datetime_str",
        "expected_date",
        "parsed_date_str"
    ).collect()
    
    # Display results
    print("\n📊 RÉSULTATS DES TESTS:\n")
    
    passed = 0
    failed = 0
    
    for row in results:
        desc = row.description
        input_val = row.datetime_str if row.datetime_str else "NULL"
        expected = row.expected_date if row.expected_date else "NULL"
        actual = row.parsed_date_str if row.parsed_date_str else "NULL"
        
        # Compare
        match = (expected == actual)
        status = "✅ PASS" if match else "❌ FAIL"
        
        if match:
            passed += 1
        else:
            failed += 1
        
        print(f"{status} | {desc:25} | Input: {input_val:20} | Expected: {expected:12} | Got: {actual:12}")
    
    # Summary
    total = passed + failed
    print("\n" + "="*80)
    print(f"📈 RÉSUMÉ: {passed}/{total} tests réussis ({(passed/total)*100:.1f}%)")
    if failed > 0:
        print(f"⚠️  {failed} test(s) échoué(s)")
    else:
        print("🎉 TOUS LES TESTS RÉUSSIS!")
    print("="*80 + "\n")
    
    spark.stop()
    
    return failed == 0


if __name__ == "__main__":
    success = test_sas_datetime_parsing()
    sys.exit(0 if success else 1)
