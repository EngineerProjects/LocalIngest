# Catalogue de Données

## Inventaire Entrée/Sortie du Pipeline

**Pipelines :** PTF_MVT, Capitaux, Émissions

---

## Résumé

| Couche                     | Compte     | Modèle Chemin                          |
| -------------------------- | ---------- | -------------------------------------- |
| **Bronze (Entrées)**       | 45 groupes | `bronze/{YYYY}/{MM}/` ou `bronze/ref/` |
| **Silver (Intermédiaire)** | 5 tables   | `silver/{YYYY}/{MM}/`                  |
| **Gold (Final)**           | 7 tables   | `gold/{YYYY}/{MM}/`                    |

---

## Sorties Couche Gold (7 Tables)

### PTF_MVT

| #   | Table                           | Description                       |
| --- | ------------------------------- | --------------------------------- |
| 1   | `mvt_ptf_{vision}.parquet`      | Mouvements consolidés (AZ + AZEC) |
| 2   | `ird_risk_q45_{vision}.parquet` | Risque IRD Q45                    |
| 3   | `ird_risk_q46_{vision}.parquet` | Risque IRD Q46                    |

### Capitaux

| #   | Table                               | Description         |
| --- | ----------------------------------- | ------------------- |
| 4   | `az_azec_capitaux_{vision}.parquet` | Capitaux consolidés |

### Émissions

| #   | Table                                     | Description                   |
| --- | ----------------------------------------- | ----------------------------- |
| 5   | `primes_emises_{vision}_pol_garp.parquet` | Primes par police et garantie |
| 6   | `primes_emises_{vision}_pol.parquet`      | Primes par police (agrégées)  |

---

## Couche Silver (4 Tables)

| #   | Table                       | Source               | Destination             |
| --- | --------------------------- | -------------------- | ----------------------- |
| 1   | `mvt_const_ptf_{vision}`    | Processeur AZ        | → Gold mvt_ptf          |
| 2   | `azec_ptf_{vision}`         | Processeur AZEC      | → Gold mvt_ptf          |
| 3   | `az_capitaux_{vision}`      | Capitaux AZ          | → Gold az_azec_capitaux |
| 4   | `azec_capitaux_{vision}`    | Capitaux AZEC        | → Gold az_azec_capitaux |
| 5   | `emissions_silver_{vision}` | Processeur Émissions | → Gold primes_emises    |

---

## Entrées Couche Bronze

### Fichiers IMS (Mensuel)

| Groupe Fichier | Pattern                                    | Utilisé Par       |
| -------------- | ------------------------------------------ | ----------------- |
| `ipf`          | *IPFE16_IPF_*.csv.gz, *IPFE36_IPF_*.csv.gz | PTF_MVT, Capitaux |
| `ipfm99_az`    | *SPEIPFM99_IPF_*.csv.gz                    | PTF_MVT           |

### Fichiers OneBI (Mensuel)

| Groupe Fichier             | Pattern                        | Utilisé Par |
| -------------------------- | ------------------------------ | ----------- |
| `rf_fr1_prm_dtl_midcorp_m` | rf_fr1_prm_dtl_midcorp_m_*.csv | Émissions   |

### Fichiers AZEC (Référence)

| Groupe Fichier  | Pattern      | Utilisé Par       |
| --------------- | ------------ | ----------------- |
| `polic_cu_azec` | polic_cu.csv | PTF_MVT           |
| `capitxcu_azec` | capitxcu.csv | PTF_MVT, Capitaux |
| `incendcu_azec` | incendcu.csv | PTF_MVT, Capitaux |
| `constrcu_azec` | constru.csv  | PTF_MVT           |
| `mulprocu_azec` | mulprocu.csv | PTF_MVT           |

### Risque IRD (Mensuel)

| Groupe Fichier | Pattern            | Vision   |
| -------------- | ------------------ | -------- |
| `ird_risk_q45` | ird_risk_q45_*.csv | ≥ 202210 |
| `ird_risk_q46` | ird_risk_q46_*.csv | ≥ 202210 |
| `ird_risk_qan` | ird_risk_qan_*.csv | < 202210 |

### Tables de Référence

| Groupe Fichier     | Pattern           | Utilisé Par    |
| ------------------ | ----------------- | -------------- |
| `segmentprdt`      | segmentprdt_*.csv | Tous pipelines |
| `segmprdt_prdpfa1` | prdpfa1.csv       | PTF_MVT        |
| `segmprdt_prdpfa3` | prdpfa3.csv       | PTF_MVT        |
| `table_pt_gest`    | ptgst_*.csv       | PTF_MVT        |
| `ptgst_static`     | ptgst.csv         | PTF_MVT        |
| `cproduit`         | cproduit.csv      | PTF_MVT        |
| `prdcap`           | prdcap.csv        | PTF_MVT        |

### Nouvelles Tables Référence

| Groupe Fichier           | Pattern                    | Objectif                 |
| ------------------------ | -------------------------- | ------------------------ |
| `ref_mig_azec_vs_ims`    | ref_mig_azec_vs_ims.csv    | Mappage migration AZEC   |
| `indices`                | indices.csv                | Indexation capitaux      |
| `mapping_isic_const_act` | mapping_isic_const_act.csv | Mappage activité ISIC    |
| `do_dest_isic`           | do_dest_isic.csv           | Mappage destination ISIC |
| `ref_isic`               | ref_isic.csv               | Grades de risque ISIC    |

---

## Conventions de Chemins

| Couche           | Chemin                |
| ---------------- | --------------------- |
| Bronze Mensuel   | `bronze/{YYYY}/{MM}/` |
| Bronze Référence | `bronze/ref/`         |
| Silver           | `silver/{YYYY}/{MM}/` |
| Gold             | `gold/{YYYY}/{MM}/`   |

**Format Vision:** YYYYMM (ex: 202512)

---

**Dernière Mise à Jour** : 06/02/2026
**Version** : 1.1
