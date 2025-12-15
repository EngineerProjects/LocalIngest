# Documentation Technique - Construction Data Pipeline

Ce dossier contient la documentation technique complète du pipeline de données Construction.

## 📚 Documents Disponibles

### 🔢 Calculs Métier
- **[calculs_assurance_construction.md](calculs_assurance_construction.md)** - Guide technique complet des formules et calculs d'assurance
  - Mouvements de portefeuille (AFN, RES, PTF)
  - Expositions et suspensions
  - Primes et cotisations
  - Capitaux assurés (SMP, LCI, PE, RD)
  - Coassurance et quote-part
  - Emissions et exercice comptable

### 🔄 Workflows
- **[workflows/PTF_MVT_Workflow.md](workflows/PTF_MVT_Workflow.md)** - Pipeline mouvements de portefeuille
- **[workflows/Capitaux_Workflow.md](workflows/Capitaux_Workflow.md)** - Pipeline extraction capitaux
- **[workflows/Emissions_Workflow.md](workflows/Emissions_Workflow.md)** - Pipeline primes émises

### ⚙️ Configuration
- **[configs/Configuration_Guide.md](configs/Configuration_Guide.md)** - Guide de configuration
- **[configs/Data_Catalog.md](configs/Data_Catalog.md)** - Catalogue des tables input/output

### 📊 Données
- **[infos/available_datas.md](infos/available_datas.md)** - Datasets disponibles dans le datalake

---

## 🎯 Pour Démarrer

**Nouveau dans le projet ?** Commencez par lire dans cet ordre :

1. **[Calculs métier](calculs_assurance_construction.md)** - Comprendre les formules et la logique métier
2. **[Workflows](workflows/)** - Voir comment les pipelines fonctionnent
3. **[Data Catalog](configs/Data_Catalog.md)** - Connaître les tables disponibles
4. **[Configuration Guide](configs/Configuration_Guide.md)** - Configurer le pipeline

---

## 📖 Glossaire Rapide

| Terme | Définition |
|-------|------------|
| **AFN** | Affaire Nouvelle - Nouveau contrat souscrit |
| **RES** | Résiliation - Contrat résilié |
| **PTF** | Portefeuille - Contrats en vigueur |
| **SMP** | Sinistre Maximum Possible - Capital max payable |
| **LCI** | Limite Contractuelle d'Indemnité - Plafond contractuel |
| **PE** | Perte d'Exploitation - Capital perte de CA |
| **RD** | Risque Direct - Capital dommages matériels |
| **PARTCIE** | Part Compagnie - Quote-part conservée |

Pour le glossaire complet, voir [calculs_assurance_construction.md](calculs_assurance_construction.md).

---

## 🚀 Utilisation

**Lire la documentation en local** :
```bash
# Depuis la racine du projet
cd docs
cat calculs_assurance_construction.md
```

**Ou utiliser un viewer Markdown** (VSCode, GitHub, etc.)
