# Data Catalog

## Pipeline Input/Output Inventory

**Pipelines:** PTF_MVT, Capitaux, Emissions

---

## Summary

| Layer                     | Count          | Path Pattern                           |
| ------------------------- | -------------- | -------------------------------------- |
| **Bronze (Inputs)**       | 45 file groups | `bronze/{YYYY}/{MM}/` or `bronze/ref/` |
| **Silver (Intermediate)** | 5 tables       | `silver/{YYYY}/{MM}/`                  |
| **Gold (Final)**          | 7 tables       | `gold/{YYYY}/{MM}/`                    |

---

## Gold Layer Outputs (7 Tables)

### PTF_MVT

| #   | Table                           | Description                        |
| --- | ------------------------------- | ---------------------------------- |
| 1   | `mvt_ptf_{vision}.parquet`      | Consolidated movements (AZ + AZEC) |
| 2   | `ird_risk_q45_{vision}.parquet` | IRD risk Q45                       |
| 3   | `ird_risk_q46_{vision}.parquet` | IRD risk Q46                       |

### Capitaux

| #   | Table                               | Description           |
| --- | ----------------------------------- | --------------------- |
| 4   | `az_azec_capitaux_{vision}.parquet` | Consolidated capitals |

### Emissions

| #   | Table                                     | Description                      |
| --- | ----------------------------------------- | -------------------------------- |
| 5   | `primes_emises_{vision}_pol_garp.parquet` | Premiums by policy and guarantee |
| 6   | `primes_emises_{vision}_pol.parquet`      | Premiums by policy (aggregated)  |

---

## Silver Layer (4 Tables)

| #   | Table                       | Source              | Destination             |
| --- | --------------------------- | ------------------- | ----------------------- |
| 1   | `mvt_const_ptf_{vision}`    | AZ Processor        | → Gold mvt_ptf          |
| 2   | `azec_ptf_{vision}`         | AZEC Processor      | → Gold mvt_ptf          |
| 3   | `az_capitaux_{vision}`      | AZ Capitaux         | → Gold az_azec_capitaux |
| 4   | `azec_capitaux_{vision}`    | AZEC Capitaux       | → Gold az_azec_capitaux |
| 5   | `emissions_silver_{vision}` | Emissions Processor | → Gold primes_emises    |

---

## Bronze Layer Inputs

### IMS Files (Monthly)

| File Group  | Pattern                                    | Used By           |
| ----------- | ------------------------------------------ | ----------------- |
| `ipf_az`    | *IPFE16_IPF_*.csv.gz, *IPFE36_IPF_*.csv.gz | PTF_MVT, Capitaux |
| `ipfm99_az` | *SPEIPFM99_IPF_*.csv.gz                    | PTF_MVT           |

### OneBI Files (Monthly)

| File Group                 | Pattern                        | Used By   |
| -------------------------- | ------------------------------ | --------- |
| `rf_fr1_prm_dtl_midcorp_m` | rf_fr1_prm_dtl_midcorp_m_*.csv | Emissions |

### AZEC Files (Reference)

| File Group      | Pattern      | Used By           |
| --------------- | ------------ | ----------------- |
| `polic_cu_azec` | polic_cu.csv | PTF_MVT           |
| `capitxcu_azec` | capitxcu.csv | PTF_MVT, Capitaux |
| `incendcu_azec` | incendcu.csv | PTF_MVT, Capitaux |
| `constrcu_azec` | constru.csv  | PTF_MVT           |
| `mulprocu_azec` | mulprocu.csv | PTF_MVT           |

### IRD Risk (Monthly)

| File Group     | Pattern            | Vision   |
| -------------- | ------------------ | -------- |
| `ird_risk_q45` | ird_risk_q45_*.csv | ≥ 202210 |
| `ird_risk_q46` | ird_risk_q46_*.csv | ≥ 202210 |
| `ird_risk_qan` | ird_risk_qan_*.csv | < 202210 |

### Reference Tables

| File Group         | Pattern           | Used By       |
| ------------------ | ----------------- | ------------- |
| `segmentprdt`      | segmentprdt_*.csv | All pipelines |
| `segmprdt_prdpfa1` | prdpfa1.csv       | PTF_MVT       |
| `segmprdt_prdpfa3` | prdpfa3.csv       | PTF_MVT       |
| `table_pt_gest`    | ptgst_*.csv       | PTF_MVT       |
| `ptgst_static`     | ptgst.csv         | PTF_MVT       |
| `cproduit`         | cproduit.csv      | PTF_MVT       |
| `prdcap`           | prdcap.csv        | PTF_MVT       |

### New Reference Tables

| File Group               | Pattern                    | Purpose                  |
| ------------------------ | -------------------------- | ------------------------ |
| `ref_mig_azec_vs_ims`    | ref_mig_azec_vs_ims.csv    | AZEC migration mapping   |
| `indices`                | indices.csv                | Capital indexation       |
| `mapping_isic_const_act` | mapping_isic_const_act.csv | ISIC activity mapping    |
| `do_dest_isic`           | do_dest_isic.csv           | ISIC destination mapping |
| `ref_isic`               | ref_isic.csv               | ISIC hazard grades       |

---

## Path Conventions

| Layer            | Path                  |
| ---------------- | --------------------- |
| Bronze Monthly   | `bronze/{YYYY}/{MM}/` |
| Bronze Reference | `bronze/ref/`         |
| Silver           | `silver/{YYYY}/{MM}/` |
| Gold             | `gold/{YYYY}/{MM}/`   |

**Vision Format:** YYYYMM (e.g., 202512)

---

**Last Updated**: 2026-02-06  
**Version**: 1.1  
**Note**: File groups count and schemas may evolve with new requirements
