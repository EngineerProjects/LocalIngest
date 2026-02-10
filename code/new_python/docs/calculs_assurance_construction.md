# Calculs Métier - Assurance Construction

> **Public** : Utilisateurs métier, analystes, product owners
> **Objectif** : Explique les calculs d'assurance en langage clair, sans détails techniques de code

---

## 📊 Vue d'Ensemble

Ce document explique la logique métier et les formules utilisées dans le Pipeline de Données Construction. Tous les calculs sont basés sur les règles de gestion et les normes d'assurance.

---

## 🎯 Indicateurs de Mouvement

### Qu'est-ce qu'un Mouvement ?

Les mouvements suivent l'évolution du portefeuille d'assurance au fil du temps. Chaque police a **exactement UN indicateur de mouvement = 1** par période.

### AFN (Affaires Nouvelles)

**Définition** : Polices créées ou renouvelées au cours de la période.

**Règle de Gestion** :
- La date de création de la police (`dtcrepol`) est dans la période de traitement
- La police est en statut "R" (Résilié) ou "E" (En cours)
- Pour AZEC : Dépend du type de produit (47 produits spécifiques utilisent la logique de date de création)

**Exemple** :
```
Police A123 créée le 05/12/2025
Vision : 202512 (Décembre 2025)
→ NBAFN = 1
```

---

### RES (Résiliations)

**Définition** : Polices résiliées ou non renouvelées au cours de la période.

**Règle de Gestion** :
- La date de résiliation de la police (`dtresilp`) est dans la période de traitement
- Le statut de la police est passé à "Résilié"
- Exclusions : Types de produits DO0, TRC, CTR, CNR (spécifique AZEC)

**Exemple** :
```
Police B456 résiliée le 20/12/2025
Vision : 202512
→ NBRES = 1
```

---

### PTF (Portefeuille)

**Définition** : Polices restant actives sans mouvement durant la période.

**Règle de Gestion** :
- La police est active (statut "E" = En cours)
- Pas de création ni de résiliation dans cette période
- La date d'anniversaire est dans la période (pour le suivi annuel)

**Formule** :
```
NBPTF = 1 si (NBAFN = 0 ET NBRES = 0 ET police est active)
```

**Exemple** :
```
Police C789 créée en 2024, toujours active
Vision : 202512
→ NBPTF = 1
```

---

### RPT (Remise en Portefeuille - Nouvelle Prime)

**Définition** : Polices remises en portefeuille avec un **nouveau montant de prime**.

**Règle de Gestion** :
- La police précédente était suspendue ou résiliée
- Nouveau numéro de police attribué avec une prime différente
- Remplace une autre police (champ `cdpolrv` renseigné)

---

### RPC (Remise en Portefeuille - Même Prime)

**Définition** : Polices remises en portefeuille avec le **même montant de prime**.

**Règle de Gestion** :
- La police précédente était suspendue
- Même numéro de police, même prime
- Remise en vigueur administrative uniquement

---

## 💰 Calculs des Primes

### PRIMES_PTF - Primes Portefeuille

**Définition** : Prime annuelle totale pour les polices actives (base 100%, avant coassurance).

**Formule** :
```
PRIMES_PTF = PRIMETO × (1 - TXCESSCNT/100)

Où :
- PRIMETO = Prime brute
- TXCESSCNT = Taux de cession en réassurance
```

**Exemple** :
```
PRIMETO = 10 000 €
TXCESSCNT = 20% (cession au réassureur)
→ PRIMES_PTF = 10 000 € × (1 - 0,20) = 8 000 €
```

---

### PART_CIE - Part Compagnie

**Définition** : Part réelle de prime de la compagnie après coassurance.

**Formule** :
```
PART_CIE = PRIMES_PTF × (PART/100)

Où PART = Pourcentage de coassurance de la compagnie
```

**Exemple** :
```
PRIMES_PTF = 8 000 €
PART = 50% (coassurance avec une autre compagnie)
→ PART_CIE = 8 000 € × 0,50 = 4 000 €
```

---

### PRIMES_AFN / PRIMES_RES

**Définition** : Primes associées aux nouvelles affaires (AFN) ou aux polices résiliées (RES).

**Règle de Gestion** :
```
PRIMES_AFN = PRIMES_PTF si NBAFN = 1, sinon 0
PRIMES_RES = PRIMES_PTF si NBRES = 1, sinon 0
```

**Exclusions (AZEC uniquement)** :
- CSSSEG = '5' exclus des calculs AFN
- Produits DO0/TRC/CTR/CNR exclus des RES

---

## 🏗️ Montants de Capitaux

### Que sont les Capitaux ?

Les capitaux représentent les montants maximums que l'assureur pourrait avoir à payer en cas de sinistre. Ils sont extraits des détails de la police par recherche de mots-clés.

### SMP_100 (Sinistre Maximum Possible)

**Définition** : Montant maximum possible de sinistre que l'assureur pourrait payer.

**Mots-clés d'Extraction** :
- "SMP GLOBAL"
- "SMP RETENU"
- "SINISTRE MAXIMUM POSSIBLE"

**Formule (AZEC)** :
```
SMP_100 = SMP_PE_100 + SMP_DD_100

Où :
- SMP_PE_100 = SMP Perte d'Exploitation
- SMP_DD_100 = SMP Dommages Directs
```

**Exemple** :
```
La police a :
- PE (Perte d'Exploitation) : 500 000 €
- DD (Dommages Directs) : 2 000 000 €
→ SMP_100 = 2 500 000 €
```

---

### LCI_100 (Limite Contractuelle d'Indemnité)

**Définition** : Limite contractuelle - montant maximum stipulé dans le contrat d'assurance.

**Mots-clés d'Extraction** :
- "LCI GLOBAL"
- "CAPITAL REFERENCE"
- "LIMITE CONTRACTUELLE"

---

### PE (Perte d'Exploitation)

**Définition** : Couverture pour les pertes financières dues à l'interruption d'activité.

**Mots-clés d'Extraction** :
- "PERTE D EXPLOITATION"
- "PERTE EXPLOITATION"
- "PE"

**Exemple** :
```
Incendie de restaurant forçant une fermeture de 6 mois
Perte de revenus estimée : 300 000 €
→ PERTE_EXP = 300 000 €
```

---

### RD (Risque Direct) - Dommages Directs

**Définition** : Couverture pour les dommages physiques aux biens assurés.

**Mots-clés d'Extraction** :
- "RISQUE DIRECT"
- "DOMMAGES DIRECTS"
- "RD"

**Exemple** :
```
Valeur du bâtiment d'usine et de l'équipement : 5 000 000 €
→ RISQUE_DIRECT = 5 000 000 €
```

---

## 📅 Calculs d'Exposition

### EXPO_YTD (Exposition Annuelle à Date)

**Définition** : Proportion de l'année durant laquelle la police était active, exprimée en décimale.

**Formule** :
```
EXPO_YTD = Jours Actifs dans l'Année / Total Jours dans l'Année

Jours Actifs = MIN(dtresilp, Fin d'Année) - MAX(dtcrepol, Début d'Année) + 1

Pour une année bissextile : Total Jours = 366
Pour une année normale : Total Jours = 365
```

**Exemple** :
```
Police créée : 15/03/2025
Vision : 202512 (Décembre 2025)
Toujours active au 31/12/2025

Jours Actifs = 31/12/2025 - 15/03/2025 + 1 = 292 jours
Total Jours = 365
→ EXPO_YTD = 292/365 = 0,80 (80% de l'année)
```

---

### EXPO_GLI (Exposition Mensuelle)

**Définition** : Proportion du mois durant laquelle la police était active.

**Formule** :
```
EXPO_GLI = Jours Actifs dans le Mois / Total Jours dans le Mois

Jours Actifs = MIN(dtresilp, Fin du Mois) - MAX(dtcrepol, Début du Mois) + 1
```

**Exemple** :
```
Police créée : 10/12/2025
Vision : 202512
Toujours active au 31/12/2025

Jours Actifs = 31 - 10 + 1 = 22 jours
Total Jours en Décembre = 31
→ EXPO_GLI = 22/31 = 0,71
```

---

## 🤝 Coassurance

### Qu'est-ce que la Coassurance ?

La coassurance est lorsque plusieurs compagnies d'assurance partagent le risque sur une seule police. Une compagnie est "l'apériteur" (leader) et les autres sont "co-assureurs" (suiveurs).

### Types de COASS

| Type               | Description                              | Exemple                           |
| ------------------ | ---------------------------------------- | --------------------------------- |
| **APÉRITION**      | Rôle de leader - gère la police          | Compagnie A mène avec 60% de part |
| **COASS ACCEPTEE** | Rôle de suiveur - accepte la coassurance | Compagnie B suit avec 40% de part |
| **ACCEPTATION**    | Réassurance financière acceptée          | Réassurance traditionnelle        |

### TOP_COASS (Indicateur Leader)

**Définition** : Indique si la compagnie est l'apériteur (leader) de la coassurance.

**Règle de Gestion** :
```
TOP_COASS = 1 si COASS = "APÉRITION"
TOP_COASS = 0 sinon
```

### Calcul de PARTCIE

**Formule** :
```
PARTCIE = Pourcentage de part de la compagnie dans l'accord de coassurance

Distribution totale de la prime :
Compagnie A (Leader, 60%) : PARTCIE = 60
Compagnie B (Suiveur, 40%) : PARTCIE = 40
```

---

## 🏢 Segmentation

### SEGMENT2

**Définition** : Classification du segment commercial (ex : PME, Corporate, Grands Comptes).

**Source de Données** : `PRDPFA1` (Agent) ou `PRDPFA3` (Courtage)

### TYPE_PRODUIT_2

**Définition** : Classification par type de produit (ex : Construction Standard, Risques Spéciaux).

**Source de Données** : Tables de référence produits

### UPPER_MID

**Définition** : Indicateur Upper-mid market pour des stratégies de gestion de portefeuille spécifiques.

**Source de Données** : `TABLE_PT_GEST` joint sur le champ `PTGST`

---

## 🔢 Indexation (Indice FFB)

### Qu'est-ce que l'Indexation FFB ?

La FFB (Fédération Française du Bâtiment) fournit des indices de coût de construction. Les capitaux d'assurance sont ajustés annuellement pour tenir compte de l'inflation des coûts de construction.

### Capitaux Indexés

Le Pipeline **CAPITAUX** produit des valeurs indexées et non indexées :

| Capital | Non-Indexé          | Indexé (suffixe _IND)   |
| ------- | ------------------- | ----------------------- |
| SMP     | `smp_100`           | `smp_100_ind`           |
| LCI     | `lci_100`           | `lci_100_ind`           |
| PE      | `perte_exp_100`     | `perte_exp_100_ind`     |
| RD      | `risque_direct_100` | `risque_direct_100_ind` |

**Formule** :
```
Capital_IND = Capital × (Indice FFB Actuel / Indice FFB de Base)
```

**Exemple** :
```
SMP Original (2020) : 1 000 000 €
Indice FFB 2020 : 100
Indice FFB 2025 : 115
→ SMP_100_IND = 1 000 000 € × (115/100) = 1 150 000 €
```

---

## 📊 Classification ISIC

### Qu'est-ce que l'ISIC ?

ISIC (International Standard Industrial Classification) catégorise les entreprises par activité économique. Utilisé pour l'évaluation des risques et la tarification.

### Mappage NAF vers ISIC

**Processus** :
1. Extraire le code NAF du client (classification française)
2. Mapper NAF → ISIC via des tables de référence
3. Appliquer des corrections codées en dur (11 exceptions connues)
4. Dériver ISIC_GLOBAL pour le classement des risques

**Exemple** :
```
Client : Entreprise de construction
Code NAF : 4120A (Construction de maisons individuelles)
→ Code ISIC : 4100
→ Catégorie Globale ISIC : Construction
→ Grades de Risque : Incendie=3, BI=2, RCA=1
```

---

## 🎓 Résumé des Règles de Gestion

| Règle                   | Pipeline      | Description                                  |
| ----------------------- | ------------- | -------------------------------------------- |
| **Filtre Construction** | PTF_MVT       | CMARCH=6 ET CSEG=2                           |
| **Seuil de Vision**     | PTF_MVT       | <201211 : AZ seul, >=201211 : AZ+AZEC        |
| **Filtre Migration**    | AZEC          | Vision >202009 : Exclure les contrats migrés |
| **Exclusions Produit**  | AZEC          | DO0, TRC, CTR, CNR                           |
| **Exclusion CSSSEG=5**  | AZEC          | Exclus des calculs AFN                       |
| **Déduplication**       | Consolidation | Priorité AZ si NOPOL existe dans AZ et AZEC  |

---

## 📖 Glossaire

| Terme    | Nom Complet                                      | Signification                    |
| -------- | ------------------------------------------------ | -------------------------------- |
| **AFN**  | Affaire Nouvelle                                 | Nouvelle police                  |
| **RES**  | Résiliation                                      | Police résiliée                  |
| **PTF**  | Portefeuille                                     | Portefeuille actif               |
| **SMP**  | Sinistre Maximum Possible                        | Sinistre maximum possible        |
| **LCI**  | Limite Contractuelle d'Indemnité                 | Limite du contrat                |
| **PE**   | Perte d'Exploitation                             | Couverture interruption activité |
| **RD**   | Risque Direct                                    | Couverture dommages directs      |
| **FFB**  | Fédération Française du Bâtiment                 | Fédération construction          |
| **ISIC** | International Standard Industrial Classification | Code activité économique         |
| **NAF**  | Nomenclature d'Activités Française               | Code activité français           |

---

**Dernière Mise à Jour** : 06/02/2026
**Version** : 1.0
**Pour l'Implémentation Technique** : Voir les commentaires du code dans `src/processors/`
