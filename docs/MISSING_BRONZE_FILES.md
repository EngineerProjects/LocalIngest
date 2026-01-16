# Fichiers Manquants dans Bronze

**Date**: 2026-01-15  
**Statut**: Fichiers à charger dans Azure Bronze

---

## 🚨 FICHIERS CRITIQUES (Pipeline crashera sans eux)

### 📁 Destination : `bronze/2025/09/` (vision actuelle)

| Fichier à charger | Pattern attendu | Usage | Impact si absent |
|-------------------|-----------------|-------|------------------|
| `ird_risk_q45_202509.csv` | `ird_risk_q45_*.csv` | IRD Risk Q45 pour Consolidation | ❌ **CRASH Consolidation** |
| `ird_risk_q46_202509.csv` | `ird_risk_q46_*.csv` | IRD Risk Q46 pour Consolidation | ❌ **CRASH Consolidation** |
| `ird_risk_qan_202509.csv` | `ird_risk_qan_*.csv` | IRD Risk QAN pour Consolidation (si vision < 202210) | ❌ **CRASH Consolidation** |
| `*3SPEIPFM99_IPF_202509.csv.gz` | `*3SPEIPFM99_IPF_*.csv.gz` | CA data IPFM99 Pole 3 (Courtage) pour AZ | ❌ **CRASH AZ** |
| `*E1SPEIPFM99_IPF_202509.csv.gz` | `*E1SPEIPFM99_IPF_*.csv.gz` | CA data IPFM99 Pole 1 (Agent) pour AZ | ❌ **CRASH AZ** |

### Source SAS :
- **Q45/Q46/QAN** : `IRD.IRD_SUIVI_RISQUE_Q45`, `Q46`, `QAN`
- **IPFM99** : `PTF16a.IPFM99`, `PTF36a.IPFM99`

---

## ⚠️ FICHIERS OPTIONNELS (Fonctionnalités dégradées)

### 📁 Destination : `bronze/2025/09/`

| Fichier à charger | Pattern attendu | Usage | Impact si absent |
|-------------------|-----------------|-------|------------------|
| `rf_fr1_prm_dtl_midcorp_m_202509.csv` | `rf_fr1_prm_dtl_midcorp_m_*.csv` | Emissions One BI | ⚠️ Module Emissions non disponible |

### 📁 Destination : `bronze/ref/`

| Fichier à charger | Pattern attendu | Usage | Impact si absent |
|-------------------|-----------------|-------|------------------|
| `indice.csv` | `indice.csv` | Construction cost indices | ⚠️ Calculs indices non disponibles |

---

## 📋 CHECKLIST POUR CHARGEMENT

Copier cette checklist pour validation :

```
## Fichiers CRITIQUES (5) - À charger dans bronze/2025/09/ :
- [ ] ird_risk_q45_202509.csv
- [ ] ird_risk_q46_202509.csv
- [ ] ird_risk_qan_202509.csv (si vision < 202210)
- [ ] *3SPEIPFM99_IPF_202509.csv.gz (Pole 3 - Courtage)
- [ ] *E1SPEIPFM99_IPF_202509.csv.gz (Pole 1 - Agent)

## Fichiers OPTIONNELS (2) :
- [ ] bronze/2025/09/rf_fr1_prm_dtl_midcorp_m_202509.csv
- [ ] bronze/ref/indice.csv
```

---

## 🔍 VÉRIFICATION POST-CHARGEMENT

Après chargement des fichiers, vérifier :

1. **Noms exacts** :
   ```bash
   # Dans bronze/2025/09/
   ls -la bronze/2025/09/ird_risk_q*.csv
   ls -la bronze/2025/09/*IPFM99*.csv.gz
   ```

2. **Permissions** : Fichiers lisibles par le compte service

3. **Encodage** : Fichiers en `LATIN9` (ISO-8859-15)

4. **Séparateur** : CSV avec `|` comme délimiteur

5. **Headers** : Première ligne contient les noms de colonnes

---

## 📊 IMPACT MODULES

| Module | Fichiers nécessaires | Peut fonctionner sans ? |
|--------|---------------------|------------------------|
| **AZ Processor** | IPFM99 (×2) | ❌ NON |
| **AZEC Processor** | (Tous présents) | ✅ OUI |
| **Consolidation** | Q45, Q46, QAN | ❌ NON |
| **Emissions** | rf_fr1_prm_dtl_midcorp_m | ❌ NON (si module utilisé) |

---

## 🎯 PRIORITÉ

1. **Urgent** : IRD Q45, Q46, QAN + IPFM99 (×2)
2. **Optionnel** : rf_fr1 (si Emissions nécessaire), indice.csv
