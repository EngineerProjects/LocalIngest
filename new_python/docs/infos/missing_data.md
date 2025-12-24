# Fichiers Manquants - Correspondance SAS/Python

## 📋 Tableau des Fichiers à Récupérer

| # | Table SAS | Fichier Python | LIBNAME SAS | Localisation Bronze |
|---|-----------|----------------|-------------|---------------------|
| 1 | `PRM.rf_fr1_prm_dtl_midcorp_m` | `rf_fr1_prm_dtl_midcorp_m_202509.csv` | `PRM` | `bronze/2025/09/` |
| 2 | `SEG.segmentprdt_202509` | `segmentprdt_202509.csv` | `SEG` | `bronze/ref/` |
| 3 | `REF.ref_mig_azec_vs_ims` | `ref_mig_azec_vs_ims.csv` | `REF` | `bronze/ref/` |
| 4 | `INDICES` (format $INDICE) | `indices.csv` | `INDICES` | `bronze/ref/` |
| 5 | `W6.basecli_inv` | `basecli_inv.csv` | `W6` | `bronze/ref/` |
| 6 | `BINSEE.histo_note_risque` | `histo_note_risque.csv` | `BINSEE` | `bronze/ref/` |
| 7 | `DEST.do_dest202110` | `do_dest_202110.csv` OU `do_dest.csv` | `DEST` | `bronze/ref/` |
| 8 | `REF.table_segmentation_azec_mml` | `table_segmentation_azec_mml.csv` | `REF` | `bronze/ref/` |

---

## � Instructions Extraction SAS

### Pour chaque fichier :

```sas
/* Template export SAS vers CSV */
PROC EXPORT DATA=<LIBNAME>.<TABLE>
    OUTFILE="/export/path/<fichier>.csv"
    DBMS=CSV
    REPLACE;
    DELIMITER='|';
RUN;
```

### Exemples spécifiques :

#### 1. One BI Premium Data (Emissions)
```sas
LIBNAME PRM "/var/opt/data/data_project/sas94/bifr/azfronebi/data/shared_lib/prm";

PROC EXPORT DATA=PRM.rf_fr1_prm_dtl_midcorp_m
    OUTFILE="/export/rf_fr1_prm_dtl_midcorp_m_202509.csv"
    DBMS=CSV REPLACE;
    DELIMITER=',';
    WHERE DT_CPTA_CTS <= "202509";
RUN;
```

#### 2. Segmentation Produit
```sas
LIBNAME SEG "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/SEG_PRDTS";

PROC EXPORT DATA=SEG.segmentprdt_202509
    OUTFILE="/export/segmentprdt_202509.csv"
    DBMS=CSV REPLACE;
    DELIMITER='|';
RUN;
```

#### 3. Référence Migration AZEC
```sas
/* À créer manuellement si n'existe pas */
/* Voir PTF_MVTS_AZEC_MACRO.sas L94-106 pour structure */
```

#### 4. Indices Construction
```sas
/* Format $INDICE - À exporter depuis OPTIONS FMTSEARCH=(INDICES) */
PROC FORMAT CNTLOUT=indices_export;
RUN;

DATA indices;
    SET indices_export;
    WHERE fmtname = 'INDICE';
RUN;

PROC EXPORT DATA=indices
    OUTFILE="/export/indices.csv"
    DBMS=CSV REPLACE;
RUN;
```

#### 5. Base Clients W6
```sas
LIBNAME W6 "<chemin_W6>";

PROC EXPORT DATA=W6.basecli_inv
    OUTFILE="/export/basecli_inv.csv"
    DBMS=CSV REPLACE;
    DELIMITER='|';
RUN;
```

#### 6. Euler Hermes Notation Risque
```sas
LIBNAME BINSEE "<chemin_BINSEE>";

PROC EXPORT DATA=BINSEE.histo_note_risque
    OUTFILE="/export/histo_note_risque.csv"
    DBMS=CSV REPLACE;
    DELIMITER='|';
RUN;
```

#### 7. Destination Chantier
```sas
LIBNAME DEST "<chemin_DEST>";

PROC EXPORT DATA=DEST.do_dest202110
    OUTFILE="/export/do_dest_202110.csv"
    DBMS=CSV REPLACE;
    DELIMITER=',';
RUN;
```

#### 8. Segmentation AZEC
```sas
LIBNAME REF "<chemin_REF>";

PROC EXPORT DATA=REF.table_segmentation_azec_mml
    OUTFILE="/export/table_segmentation_azec_mml.csv"
    DBMS=CSV REPLACE;
    DELIMITER=',';
RUN;
```

---

## � Où Placer les Fichiers

Une fois exportés depuis SAS :

```bash
# Fichiers monthly (vision 202509)
/workspace/datalake/bronze/2025/09/
├── rf_fr1_prm_dtl_midcorp_m_202509.csv

# Fichiers reference
/workspace/datalake/bronze/ref/
├── segmentprdt_202509.csv
├── ref_mig_azec_vs_ims.csv
├── indices.csv
├── basecli_inv.csv
├── histo_note_risque.csv
├── do_dest_202110.csv (ou do_dest.csv)
└── table_segmentation_azec_mml.csv
```