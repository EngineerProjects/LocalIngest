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