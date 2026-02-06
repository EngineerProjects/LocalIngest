# Business Calculations - Construction Insurance

> **Audience**: Business users, analysts, product owners  
> **Purpose**: Explains insurance calculations in plain language, without technical code

---

## 📊 Overview

This document explains the business logic and formulas used in the Construction Data Pipeline. All calculations are based on SAS business rules and insurance standards.

---

## 🎯 Movement Indicators

### What are Movements?

Movements track changes in the insurance portfolio over time. Each policy has **exactly ONE movement indicator = 1** per period.

### AFN (Affaires Nouvelles) - New Policies

**Definition**: Policies created or renewed during the period.

**Business Rule**:
- Policy creation date (`dtcrepol`) is within the processing period
- Policy is in "R" (Résilié) or "E" (En cours) status
- For AZEC: Depends on product type (47 specific products use creation date logic)

**Example**:
```
Policy A123 created on 2025-12-05
Vision: 202512 (December 2025)
→ NBAFN = 1
```

---

### RES (Résiliations) - Terminations

**Definition**: Policies terminated or not renewed during the period.

**Business Rule**:
- Policy termination date (`dtresilp`) is within the processing period
- Policy status changed to "Résilié"
- Exclusions: DO0, TRC, CTR, CNR product types (AZEC specific)

**Example**:
```
Policy B456 terminated on 2025-12-20
Vision: 202512
→ NBRES = 1
```

---

### PTF (Portefeuille) - Active Portfolio

**Definition**: Policies that remain active with no movement during the period.

**Business Rule**:
- Policy is active (status "E" = En cours)
- No creation or termination in this period
- Anniversary date is in the period (for annual tracking)

**Formula**:
```
NBPTF = 1 if (NBAFN = 0 AND NBRES = 0 AND policy is active)
```

**Example**:
```
Policy C789 created in 2024, still active
Vision: 202512
→ NBPTF = 1
```

---

### RPT (Remise en Portefeuille - New Premium)

**Definition**: Policies reinstated into portfolio with a **new premium** amount.

**Business Rule**:
- Previous policy was suspended or terminated
- New policy number assigned with different premium
- Replaces another policy (`cdpolrv` field populated)

---

### RPC (Remise en Portefeuille - Same Premium)

**Definition**: Policies reinstated into portfolio with the **same premium** amount.

**Business Rule**:
- Previous policy was suspended
- Same policy number, same premium
- Administrative reinstatement only

---

## 💰 Premium Calculations

### PRIMES_PTF - Portfolio Premiums

**Definition**: Total annual premium for active policies (100% basis, before coinsurance).

**Formula**:
```
PRIMES_PTF = PRIMETO × (1 - TXCESSCNT/100)

Where:
- PRIMETO = Gross premium
- TXCESSCNT = Ceding rate percentage
```

**Example**:
```
PRIMETO = €10,000
TXCESSCNT = 20% (ceding to reinsurer)
→ PRIMES_PTF = €10,000 × (1 - 0.20) = €8,000
```

---

### PART_CIE - Company Share

**Definition**: Company's actual premium share after coinsurance.

**Formula**:
```
PART_CIE = PRIMES_PTF × (PART/100)

Where PART = Company's coinsurance percentage
```

**Example**:
```
PRIMES_PTF = €8,000
PART = 50% (coinsurance with another company)
→ PART_CIE = €8,000 × 0.50 = €4,000
```

---

### PRIMES_AFN / PRIMES_RES

**Definition**: Premiums associated with new policies (AFN) or terminated policies (RES).

**Business Rule**:
```
PRIMES_AFN = PRIMES_PTF if NBAFN = 1, else 0
PRIMES_RES = PRIMES_PTF if NBRES = 1, else 0
```

**Exclusions (AZEC only)**:
- CSSSEG = '5' excluded from AFN calculations
- DO0/TRC/CTR/CNR products excluded from RES

---

## 🏗️ Capital Amounts

### What are Capitals?

Capitals represent the maximum amounts the insurer might have to pay in case of claims. They are extracted from policy details using keyword matching.

### SMP_100 (Sinistre Maximum Possible)

**Definition**: Maximum possible claim amount the insurer could pay.

**Extraction Keywords**:
- "SMP GLOBAL"
- "SMP RETENU"
- "SINISTRE MAXIMUM POSSIBLE"

**Formula (AZEC)**:
```
SMP_100 = SMP_PE_100 + SMP_DD_100

Where:
- SMP_PE_100 = Business Interruption SMP
- SMP_DD_100 = Direct Damage SMP
```

**Example**:
```
Policy has:
- PE (Business Interruption): €500,000
- DD (Direct Damage): €2,000,000
→ SMP_100 = €2,500,000
```

---

### LCI_100 (Limite Contractuelle d'Indemnité)

**Definition**: Contractual limit - maximum amount stated in the insurance contract.

**Extraction Keywords**:
- "LCI GLOBAL"
- "CAPITAL REFERENCE"
- "LIMITE CONTRACTUELLE"

---

### PE (Perte d'Exploitation) - Business Interruption

**Definition**: Coverage for financial losses due to business interruption.

**Extraction Keywords**:
- "PERTE D EXPLOITATION"
- "PERTE EXPLOITATION"
- "PE"

**Example**:
```
Restaurant fire forces 6-month closure
Estimated revenue loss: €300,000
→ PERTE_EXP = €300,000
```

---

### RD (Risque Direct) - Direct Damage

**Definition**: Coverage for physical damage to insured property.

**Extraction Keywords**:
- "RISQUE DIRECT"
- "DOMMAGES DIRECTS"
- "RD"

**Example**:
```
Factory building and equipment value: €5,000,000
→ RISQUE_DIRECT = €5,000,000
```

---

## 📅 Exposure Calculations

### EXPO_YTD (Year-to-Date Exposure)

**Definition**: Proportion of the year the policy was active, expressed as a decimal.

**Formula**:
```
EXPO_YTD = Active Days in Year / Total Days in Year

Active Days = MIN(dtresilp, End of Year) - MAX(dtcrepol, Start of Year) + 1

For leap year: Total Days = 366
For normal year: Total Days = 365
```

**Example**:
```
Policy created: 2025-03-15
Vision: 202512 (December 2025)
Still active on 2025-12-31

Active Days = 2025-12-31 - 2025-03-15 + 1 = 292 days
Total Days = 365
→ EXPO_YTD = 292/365 = 0.80 (80% of the year)
```

---

### EXPO_GLI (Monthly Exposure)

**Definition**: Proportion of the month the policy was active.

**Formula**:
```
EXPO_GLI = Active Days in Month / Total Days in Month

Active Days = MIN(dtresilp, End of Month) - MAX(dtcrepol, Start of Month) + 1
```

**Example**:
```
Policy created: 2025-12-10
Vision: 202512
Still active on 2025-12-31

Active Days = 31 - 10 + 1 = 22 days
Total Days in December = 31
→ EXPO_GLI = 22/31 = 0.71
```

---

## 🤝 Coassurance

### What is Coassurance?

Coassurance is when multiple insurance companies share the risk on a single policy. One company is the "leader" and others are "followers".

### COASS Types

| Type               | Description                         | Example                          |
| ------------------ | ----------------------------------- | -------------------------------- |
| **APÉRITION**      | Leader role - manages the policy    | Company A leads with 60% share   |
| **COASS ACCEPTEE** | Follower role - accepts coassurance | Company B follows with 40% share |
| **ACCEPTATION**    | Financial reinsurance accepted      | Traditional reinsurance          |

### TOP_COASS (Leadership Flag)

**Definition**: Indicates if the company is the coassurance leader.

**Business Rule**:
```
TOP_COASS = 1 if COASS = "APÉRITION"
TOP_COASS = 0 otherwise
```

### PARTCIE Calculation

**Formula**:
```
PARTCIE = Company's share percentage in the coinsurance agreement

Total premium distribution:
Company A (Leader, 60%): PARTCIE = 60
Company B (Follower, 40%): PARTCIE = 40
```

---

## 🏢 Segmentation

### SEGMENT2

**Definition**: Business segment classification (e.g., SME, Corporate, Large Corporate).

**Data Source**: `PRDPFA1` (Agent) or `PRDPFA3` (Courtage)

### TYPE_PRODUIT_2

**Definition**: Product type classification (e.g., Standard Construction, Special Risks).

**Data Source**: Product reference tables

### UPPER_MID

**Definition**: Upper-mid market flag for specific portfolio management strategies.

**Data Source**: `TABLE_PT_GEST` joined on `PTGST` field

---

## 🔢 Indexation (FFB Index)

### What is FFB Indexation?

FFB (Fédération Française du Bâtiment) provides construction cost indices. Insurance capitals are adjusted annually to account for construction cost inflation.

### Indexed Capitals

**CAPITAUX Pipeline** produces both indexed and non-indexed values:

| Capital | Non-Indexed         | Indexed (_IND suffix)   |
| ------- | ------------------- | ----------------------- |
| SMP     | `smp_100`           | `smp_100_ind`           |
| LCI     | `lci_100`           | `lci_100_ind`           |
| PE      | `perte_exp_100`     | `perte_exp_100_ind`     |
| RD      | `risque_direct_100` | `risque_direct_100_ind` |

**Formula**:
```
Capital_IND = Capital × (Current FFB Index / Base FFB Index)
```

**Example**:
```
Original SMP (2020): €1,000,000
FFB Index 2020: 100
FFB Index 2025: 115
→ SMP_100_IND = €1,000,000 × (115/100) = €1,150,000
```

---

## 📊 ISIC Classification

### What is ISIC?

ISIC (International Standard Industrial Classification) categorizes businesses by economic activity. Used for risk assessment and pricing.

### NAF to ISIC Mapping

**Process**:
1. Extract client's NAF code (French classification)
2. Map NAF → ISIC using reference tables
3. Apply hardcoded corrections (11 known exceptions)
4. Derive ISIC_GLOBAL for hazard grading

**Example**:
```
Client: Construction company
NAF Code: 4120A (Single-family home construction)
→ ISIC Code: 4100
→ ISIC Global Category: Construction
→ Hazard Grades: Fire=3, BI=2, RCA=1
```

---

## 🎓 Business Rules Summary

| Rule                    | Pipeline      | Description                                     |
| ----------------------- | ------------- | ----------------------------------------------- |
| **Construction Filter** | PTF_MVT       | CMARCH=6 AND CSEG=2                             |
| **Vision Threshold**    | PTF_MVT       | <201211: AZ only, >=201211: AZ+AZEC             |
| **Migration Filter**    | AZEC          | Vision >202009: Exclude migrated contracts      |
| **Product Exclusions**  | AZEC          | DO0, TRC, CTR, CNR                              |
| **CSSSEG=5 Exclusion**  | AZEC          | Excluded from AFN calculations                  |
| **Deduplication**       | Consolidation | AZ priority if NOPOL exists in both AZ and AZEC |

---

## 📖 Glossary

| Term     | Full Name                                        | Meaning                        |
| -------- | ------------------------------------------------ | ------------------------------ |
| **AFN**  | Affaire Nouvelle                                 | New policy                     |
| **RES**  | Résiliation                                      | Termination                    |
| **PTF**  | Portefeuille                                     | Active portfolio               |
| **SMP**  | Sinistre Maximum Possible                        | Maximum possible claim         |
| **LCI**  | Limite Contractuelle d'Indemnité                 | Contract limit                 |
| **PE**   | Perte d'Exploitation                             | Business interruption coverage |
| **RD**   | Risque Direct                                    | Direct damage coverage         |
| **FFB**  | Fédération Française du Bâtiment                 | French construction federation |
| **ISIC** | International Standard Industrial Classification | Economic activity code         |
| **NAF**  | Nomenclature d'Activités Française               | French activity code           |

---

**Last Updated**: 2026-02-06  
**Version**: 1.0  
**For Technical Implementation**: See code comments in `src/processors/`
