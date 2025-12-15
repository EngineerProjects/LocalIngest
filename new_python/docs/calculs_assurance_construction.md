# 📐 Calculs d'Assurance Construction - Guide Technique

## 🎯 Vue d'Ensemble

Ce document détaille **tous les calculs métier** effectués dans le pipeline d'assurance construction, avec formules mathématiques et terminologie assurance.

**Domaines couverts** :
1. Mouvements de portefeuille (AFN, RES, PTF)
2. Expositions et suspensions
3. Primes et cotisations
4. Capitaux assurés (SMP, LCI, PE, RD)
5. Coassurance et quote-part
6. Emissions et exercice comptable

---

## 📚 Glossaire Assurance (Jargon Technique)

### Termes Clés à Maîtriser

| Terme | Sigle | Définition |
|-------|-------|------------|
| **Affaire Nouvelle** | AFN | Nouveau contrat souscrit durant la période |
| **Résiliation** | RES | Contrat résilié durant la période |
| **Portefeuille** | PTF | Contrats en vigueur à la fin de période |
| **Exposition** | EXPO | Durée de couverture effective d'un risque |
| **Prime Totale** | PRIMETO | Prime TTC avant application de la part compagnie |
| **Part Compagnie** | PARTCIE | Quote-part conservée par l'assureur (vs réassurance/coassurance) |
| **Sinistre Maximum Possible** | SMP | Capital maximum que l'assureur pourrait payer |
| **Limite Contractuelle d'Indemnité** | LCI | Plafond d'indemnisation contractuel |
| **Perte d'Exploitation** | PE | Capital couvrant la perte de chiffre d'affaires |
| **Risque Direct** | RD | Capital couvrant les dommages matériels |
| **Coassurance** | COASS | Partage du risque entre plusieurs assureurs |
| **Tacite Reconduction** | TR | Renouvellement automatique du contrat |
| **Exercice de Rattachement** | EXER | Année comptable de comptabilisation |

---

## 1️⃣ MOUVEMENTS DE PORTEFEUILLE (AFN/RES/PTF)

### 1.1 Affaires Nouvelles (AFN)

**Définition** : Contrats entrés en vigueur durant la période

**Formule Générale** :
```
AFN = 1  SI (
    ETATPOL = "R"  ET
    PRODUIT ∉ {CNR, DO0}  ET
    NBPTF_NON_MIGRES = 1  ET
    Condition_Temporelle_AFN = VRAI
)
SINON AFN = 0
```

**Condition Temporelle (Produits Spécifiques)** :
```
Pour PRODUIT ∈ {A00, A01, AA1, ...} (47 produits) :
    AFN = 1  SI  mois(DATAFN) ≤ mois_vision  ET  année(DATAFN) = année_vision

Pour Autres Produits :
    AFN = 1  SI (
        (DTDEBN ≤ EFFETPOL ≤ DTFINMN  ET  DATAFN ≤ DTFINMN) OU
        (EFFETPOL < DTDEBN  ET  DTDEBN ≤ DATAFN ≤ DTFINMN)
    )
```

**Où** :
- `DTDEBN` = 01/01/année_vision (début année)
- `DTFINMN` = dernier jour du mois de vision
- `DATAFN` = date d'effet du contrat
- `EFFETPOL` = date de prise d'effet

**Ton code** (AZEC processor, utils/business_logic.py) :
```python
# Produits spécifiques (47 produits AZEC)
afn_specific = (
    (col("etatpol") == "R") &
    (~col("produit").isin(['CNR', 'DO0'])) &
    (col("nbptf_non_migres_azec") == 1) &
    (col("produit").isin(azec_products)) &
    (month(col("datafn")) <= mois) &
    (year(col("datafn")) == annee)
)

# Autres produits
afn_other = (
    (col("etatpol") == "R") &
    (~col("produit").isin(['CNR', 'DO0'])) &
    (col("nbptf_non_migres_azec") == 1) &
    (~col("produit").isin(azec_products)) &
    (
        ((col("effetpol") >= dtdebn) & (col("effetpol") <= dtfinmn) & (col("datafn") <= dtfinmn)) |
        ((col("effetpol") < dtdebn) & (col("datafn") >= dtdebn) & (col("datafn") <= dtfinmn))
    )
)
```

---

### 1.2 Résiliations (RES)

**Définition** : Contrats résiliés durant la période

**Formule Générale** :
```
RES = 1  SI (
    ETATPOL = "R"  ET
    PRODUIT ∉ {CNR, DO0}  ET
    NBPTF_NON_MIGRES = 1  ET
    Condition_Temporelle_RES = VRAI
)
SINON RES = 0
```

**Condition Temporelle (Produits Spécifiques)** :
```
Pour PRODUIT ∈ {A00, A01, AA1, ...} :
    RES = 1  SI  mois(DATRESIL) ≤ mois_vision  ET  année(DATRESIL) = année_vision

Pour Autres Produits :
    RES = 1  SI (
        (DTDEBN ≤ DATFIN ≤ DTFINMN  ET  DATRESIL ≤ DTFINMN) OU
        (DATFIN ≤ DTFINMN  ET  DTDEBN ≤ DATRESIL ≤ DTFINMN)
    )
```

**Ajustements NBRES** (logique métier spécifique) :
```
NBRES = 0  SI (
    RMPLCANT ≠ ""  ET  MOTIFRES = "RP"  (Remplacement) OU
    MOTIFRES ∈ {SE, SA}  (Sans effet, Sans avenant) OU
    CSSSEG = "5"  (Segment exclu)
)
```

---

### 1.3 Portefeuille (PTF)

**Définition** : Contrats actifs à la fin de la période

**Formule** :
```
NBPTF = 1  SI (
    NBPTF_NON_MIGRES = 1  ET
    EFFETPOL ≤ DTFINMN  ET
    DATAFN ≤ DTFINMN  ET
    (DATFIN = NULL  OU  DATFIN > DTFINMN  OU  DATRESIL > DTFINMN)  ET
    (ETATPOL = 'E'  OU  (ETATPOL = 'R'  ET  DATFIN ≥ DTFINMN))  ET
    PRODUIT ∉ {DO0, TRC, CTR, CNR}
)
SINON NBPTF = 0
```

**Où** :
- `ETATPOL = 'E'` : En cours
- `ETATPOL = 'R'` : Résilié
- `DATFIN` : Date de fin de contrat
- `DATRESIL` : Date de résiliation

---

## 2️⃣ EXPOSITION (EXPO)

### 2.1 Exposition Year-To-Date (EXPO_YTD)

**Définition** : Taux d'exposition du risque depuis le début de l'année

**Formule Mathématique** :
```
                    min(DATFIN, DTFINMN) - max(EFFETPOL, DTDEBN) + 1
EXPO_YTD = max(0, ──────────────────────────────────────────────────── )
                              DTFINMN - DTDEBN + 1
```

**Composantes** :
- **Numérateur** : Nombre de jours de couverture effective
  - `min(DATFIN, DTFINMN)` : Date de fin effective (contrat ou période)
  - `max(EFFETPOL, DTDEBN)` : Date de début effective
  - `+1` : Inclusion des deux bornes

- **Dénominateur** : Nombre total de jours dans la période
  - `DTFINMN - DTDEBN + 1` : Du 01/01 au dernier jour du mois

**Exemple** :
```
Vision: 202509 (septembre 2025)
DTDEBN = 2025-01-01
DTFINMN = 2025-09-30
Jours totaux = 273 jours

Contrat: EFFETPOL = 2025-03-15, DATFIN = 2025-12-31
Jours effectifs = min(2025-12-31, 2025-09-30) - max(2025-03-15, 2025-01-01) + 1
                = 2025-09-30 - 2025-03-15 + 1
                = 200 jours

EXPO_YTD = 200 / 273 = 0.7326 (73.26%)
```

**Ton code** (utils/transformations):
```python
expo_ytd = (
    greatest(
        lit(0),
        (least(col("datfin"), lit(dtfinmn)) - greatest(col("effetpol"), lit(dtdebn)) + 1)
    ) / (datediff(lit(dtfinmn), lit(dtdebn)) + 1)
)
```

---

### 2.2 Exposition Glissante (EXPO_GLI)

**Définition** : Taux d'exposition sur le mois courant uniquement

**Formule** :
```
                    min(DATFIN, DTFINMN) - max(EFFETPOL, DTFINMN_PREV + 1) + 1
EXPO_GLI = max(0, ──────────────────────────────────────────────────────────── )
                                    DTFINMN - DTFINMN_PREV
```

**Où** :
- `DTFINMN_PREV` : Dernier jour du mois précédent
- Dénominateur : Nombre de jours dans le mois courant

**Exemple** :
```
Vision: 202509
DTFINMN = 2025-09-30
DTFINMN_PREV = 2025-08-31
Jours du mois = 30

Contrat: EFFETPOL = 2025-08-15, DATFIN = 2025-12-31
Jours effectifs mois = 2025-09-30 - (2025-08-31 + 1) + 1 = 30 jours

EXPO_GLI = 30 / 30 = 1.0 (100%)
```

---

### 2.3 Suspension (NBJ_SUSP_YTD)

**Définition** : Nombre de jours de suspension dans la période

**Formule** :
```
NBJ_SUSP_YTD = CASE
    WHEN (DTDEBN ≤ DATRESIL ≤ DTFINMN  OU  DTDEBN ≤ DATFIN ≤ DTFINMN) THEN
        min(DATFIN, DTFINMN, DATEXPIR) - max(DTDEBN - 1, DATRESIL - 1)
    
    WHEN (DATRESIL ≤ DTDEBN  ET  DATFIN ≥ DTFINMN) THEN
        DTFINMN - DTDEBN + 1
    
    ELSE 0
END
```

**Où** :
- `DATRESIL` : Date de résiliation
- `DATEXPIR` : Date d'expiration
- Si contrat résilié durant période → jours depuis résiliation
- Si contrat résilié avant période → toute la période

---

## 3️⃣ PRIMES ET COTISATIONS

### 3.1 Prime Totale (PRIMETO)

**Définition** : Prime totale avant application part compagnie

**Formule** :
```
PRIMETO = PRIME × PARTCIE
```

**Où** :
- `PRIME` : Prime de base
- `PARTCIE` : Part compagnie (quote-part conservée)

**Calcul PARTCIE** :
```
              PRCDCIE
PARTCIE = ─────────────  SI coassurance
              100

PARTCIE = 1  SI pas de coassurance
```

---

### 3.2 Prime CUA (Prime au Quote-Part)

**Définition** : Prime nette après application du taux de cession

**Formule** :
```
                PRIME × PARTBRUT
PRIMECUA = ──────────────────────── + CPCUA
                    100
```

**Où** :
- `PARTBRUT` : Part brute (%)
- `CPCUA` : Complément de prime

**Exemple** :
```
PRIME = 1000 €
PARTBRUT = 75% (25% cédé en réassurance)
CPCUA = 50 €

PRIMECUA = (1000 × 75) / 100 + 50 = 750 + 50 = 800 €
```

**Ton code** (AZEC processor L174):
```python
primecua_expr = (col("prime") * col("partbrut") / 100.0) + col("cpcua")
```

---

### 3.3 Cotisation à 100% (COTIS_100)

**Définition** : Prime technique à 100% (hors coassurance)

**Formule** :
```
                           ⎧ PRIME                    SI PARTBRUT = 0
COTIS_100 = ⎨
                           ⎩ PRIME + (CPCUA / PARTCIE)  SINON
```

**Logique** :
- Si pas de cession (`PARTBRUT = 0`) → Prime directe
- Sinon → Reconstitution à 100% technique

**Exemple** :
```
PRIME = 1000 €
CPCUA = 50 €
PARTCIE = 0.75 (75%)

COTIS_100 = 1000 + (50 / 0.75) = 1000 + 66.67 = 1066.67 €
```

---

### 3.4 Primes AFN/RES/PTF

**Formule de Filtrage** :
```
PRIMES_AFN = PRIMECUA  SI (NBAFN = 1  ET  CSSSEG ≠ "5")  SINON 0

PRIMES_RES = PRIMECUA  SI (NBRES = 1  ET  CSSSEG ≠ "5")  SINON 0

PRIMES_PTF = PRIMETO   SI (NBPTF = 1)  SINON 0
```

**Où** :
- `CSSSEG = "5"` : Segment exclu des AFN/RES
- Primes AFN/RES utilisent `PRIMECUA` (nette)
- Primes PTF utilisent `PRIMETO` (brute)

---

## 4️⃣ CAPITAUX ASSURÉS

### 4.1 SMP (Sinistre Maximum Possible)

**Définition** : Capital maximum que l'assureur pourrait indemniser

**Formule de Base** :
```
SMP_100 = max(valeurs extraites des 14 garanties où LBCAPI contient "SMP")
```

**Calcul avec Indexation** :
```
SMP_100_IND = SMP_100 × Coefficient_Indexation

Où Coefficient_Indexation = f(PRPRVC, DTECHANN, DTEFSITT)
```

**Normalisation à 100%** (base technique) :
```
                        SMP_100_IND × 100
SMP_100_IND_NORM = ──────────────────────
                           PRCDCIE
```

**Logique Métier** :
```
SMP_100 = max(SMP_100, SMP_PE_100 + SMP_RD_100)
```
→ SMP global ≥ somme PE + RD

**Ton code** (capitaux AZ L291-292):
```python
# Règle métier SMP
df = df.withColumn("smp_100_ind", 
    greatest(col("smp_100_ind"), col("smp_pe_100_ind") + col("smp_rd_100_ind")))
```

---

### 4.2 LCI (Limite Contractuelle d'Indemnité)

**Définition** : Plafond maximum d'indemnisation prévu au contrat

**Formule** :
```
LCI_100 = max(valeurs extraites où LBCAPI contient "LCI GLOBAL")
```

**Types de LCI** :
```
LCI_PE (Perte d'Exploitation) + LCI_DD (Dommages Directs) = LCI_GLOBAL
```

**Normalisation** :
```
                     LCI_100 × 100
LCI_100_NORM = ──────────────────
                    PRCDCIE
```

---

### 4.3 PE (Perte d'Exploitation)

**Définition** : Capital couvrant la perte de chiffre d'affaires

**Formule** :
```
PERTE_EXP_100 = max(valeurs extraites où LBCAPI contient "PERTE")
```

**Patterns reconnus** :
- "PERTE EXPLOITATION (MARGE BRUTE)"
- "PERTE D EXPLOITATION"
- "PERTE D'EXPLOITATION"
- "CAPITAL PERTES EXPLOITATION"
- "CAPITAUX TOTAUX P.E."

**Source** : Garanties incendie (INCENDCU)

**Ton code** (capitaux extraction):
```python
# Pattern matching pour PE
if any(pattern in label for pattern in [
    "PERTE EXPLOITATION", "PERTE D'EXPLOITATION", "PERTE D EXPLOITATION"
]):
    PERTE_EXP_100_IND = max(PERTE_EXP_100_IND, MTCAPI_INDEXED)
```

---

### 4.4 RD (Risque Direct)

**Définition** : Capital couvrant les dommages matériels directs

**Formule** :
```
RISQUE_DIRECT_100 = max(valeurs extraites où LBCAPI contient "RISQUE DIRECT")
```

**Patterns reconnus** :
- "RISQUE DIRECT" (sauf "SINIS MAX POSSIBLE RISQUE DIRECT")
- "CAPITAUX DOMMAGES DIR"

**Valeur Assurée Totale** :
```
VALUE_INSURED = PERTE_EXP + RISQUE_DIRECT
```

---

### 4.5 Limites RC (Responsabilité Civile)

**Limite RC par Sinistre** :
```
LIMITE_RC_PAR_SIN = max(valeurs où LBCAPI contient "DOMMAGES CORPORELS" 
                                                 OU "DOMM. MAT/IMMAT"
                                                 OU "TOUS DOMMAGES CONFONDUS")
```

**Limite RC par An** :
```
LIMITE_RC_PAR_AN = max(valeurs où LBCAPI contient "TOUS DOMMAGES CONFONDUS (AL)"
                                                OU "RC AL"
                                                OU "RCP TOUS DOM")
```

**Limite RC Globale** :
```
LIMITE_RC_100 = max(LIMITE_RC_PAR_SIN, LIMITE_RC_PAR_AN)
```

---

## 5️⃣ COASSURANCE

### 5.1 Part Compagnie (PARTCIE)

**Formule** :
```
              ⎧ PRCDCIE / 100      SI coassurance (CDPOLQPL = '1')
PARTCIE = ⎨
              ⎩ 1                  SI sans coassurance (CDPOLQPL ≠ '1')
```

**Exemple** :
```
Coassurance 30/70:
- Assureur A conserve 30% → PRCDCIE = 30 → PARTCIE = 0.30
- Assureur B conserve 70% → PRCDCIE = 70 → PARTCIE = 0.70
```

---

### 5.2 Types de Coassurance (COASS)

**Classification** :
```
COASS = ⎧ "SANS COASSURANCE"    SI CODECOAS = '0'
        ⎪ "APERITION"           SI CODECOAS = 'A'
        ⎪ "COASS. ACCEPTEE"     SI CODECOAS = 'C'
        ⎨ "REASS. ACCEPTEE"     SI TYPCONTR = 'A' ET CODECOAS = 'R'
        ⎩ "AUTRES"              SINON
```

**Flag Binaire** :
```
TOP_COASS = ⎧ 0  SI CODECOAS = '0'
            ⎩ 1  SINON
```

---

## 6️⃣ EMISSIONS (Primes Émises)

### 6.1 Exercice de Rattachement

**Définition** : Année comptable de rattachement de la prime

**Formule** :
```
EXERCICE = ⎧ "cou" (courant)     SI NU_EX_RATT_CTS ≥ année_vision
           ⎩ "ant" (antérieur)    SI NU_EX_RATT_CTS < année_vision
```

**Où** :
- `NU_EX_RATT_CTS` : Numéro d'exercice de rattachement du contrat
- `année_vision` : Année de la vision en cours

**Exemple** :
```
Vision: 202509 (année 2025)
Contrat A: NU_EX_RATT_CTS = 2025 → EXERCICE = "cou"
Contrat B: NU_EX_RATT_CTS = 2024 → EXERCICE = "ant"
```

---

### 6.2 Primes Émises

**Prime Totale (PRIMES_X)** : Toutes années confondues
```
PRIMES_X = Σ MT_HT_CTS  (pour tous exercices)
```

**Prime Exercice Courant (PRIMES_N)** : Année courante uniquement
```
PRIMES_N = Σ MT_HT_CTS  WHERE EXERCICE = "cou"
```

**Ton code** (emissions processor L151-159):
```python
# Filtre pour exercice courant
df_current = df.filter(col('exercice') == 'cou')

df_current_agg = df_current.groupBy(...).agg(
    sum('mt_ht_cts').alias('primes_n_temp')
)

# JOIN pour ajouter PRIMES_N à toutes les lignes
df = df.join(df_current_agg, ..., how='left')
df = df.withColumn('primes_n', coalesce(col('primes_n_temp'), lit(0.0)))
```

---

### 6.3 Code Garantie (CGARP)

**Extraction** :
```
CGARP = substr(CD_GAR_PROSPCTIV, 3, 3)
```

**Exemple** :
```
CD_GAR_PROSPCTIV = "AB123XY"
CGARP = "123" (caractères 3 à 5)
```

---

## 7️⃣ AGRÉGATIONS

### 7.1 Agrégation par Police + Garantie (POL_GARP)

**Groupe** :
```
GROUP BY: vision, dircom, cdpole, nopol, cdprod, noint, cgarp, 
          cmarch, cseg, cssseg, cd_cat_min
```

**Métriques** :
```
PRIMES_X = Σ MT_HT_CTS
PRIMES_N = Σ PRIMES_N  (déjà filtrée)
MTCOM_X = Σ MTCOM
```

---

### 7.2 Agrégation par Police (POL)

**Groupe** :
```
GROUP BY: vision, dircom, nopol, noint, cdpole, cdprod,
          cmarch, cseg, cssseg
```

**Métriques** :
```
PRIMES_X = Σ PRIMES_X  (depuis POL_GARP)
PRIMES_N = Σ PRIMES_N
MTCOM_X = Σ MTCOM_X
```

→ Agrégation de niveau 2 (depuis POL_GARP)

---

## 🎯 Récapitulatif des Formules Clés

### Mouvements
```
AFN = f(ETATPOL, dates, PRODUIT)
RES = f(ETATPOL, dates, PRODUIT, MOTIFRES)
PTF = f(ETATPOL, dates, PRODUIT)
```

### Exposition
```
EXPO_YTD = (min(DATFIN, DTFINMN) - max(EFFETPOL, DTDEBN) + 1) / nbj_tot
EXPO_GLI = (min(DATFIN, DTFINMN) - max(EFFETPOL, DTFINMN_PREV+1) + 1) / nbj_mois
```

### Primes
```
PRIMETO = PRIME × PARTCIE
PRIMECUA = (PRIME × PARTBRUT / 100) + CPCUA
COTIS_100 = PRIME + (CPCUA / PARTCIE)  [si PARTBRUT ≠ 0]
```

### Capitaux
```
SMP_100 = max(SMP_100, SMP_PE + SMP_RD)
LCI_100 = max(valeurs LCI)
VALUE_INSURED = PE + RD
NORM_100 = CAPITAL × 100 / PRCDCIE
```

### Coassurance
```
PARTCIE = PRCDCIE / 100  [si coassurance]
PARTCIE = 1              [sinon]
```
