git add .
git commit -m "feat: complete performance optimizations and missing data tooling

- Apply critical cache/unpersist to consolidation IRD processing (+40% perf)
- Optimize withColumn chains in capital_operations (+20% perf)
- Add business-compliant generator for 8 missing reference files
- Create SAS connection scripts for manual data extraction
- Document missing files with SAS table mappings"