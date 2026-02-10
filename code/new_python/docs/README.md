# 📚 Documentation - Pipeline de Données Construction

Ce dossier contient la **documentation métier** expliquant le fonctionnement du pipeline, sans détails techniques de code.

---

## 🎯 Navigation Rapide

### Pour les Utilisateurs Métier
Commencez ici pour comprendre **ce que** fait le pipeline :

1. **[Calculs Assurance](calculs_assurance_construction.md)** ⭐ Le + important
   - Explique tous les calculs d'assurance en langage clair
   - Mouvements de portefeuille (affaires nouvelles, résiliations, renouvellements)
   - Montants de capitaux et plafonds de garantie
   - Calculs de primes

2. **[Flux de Travail (Workflows)](workflows/)** - Comment les données circulent dans chaque pipeline
   - Flux Mouvements de Portefeuille (PTF_MVT)
   - Flux Extraction Capitaux
   - Flux Émissions de Primes

### Pour les Utilisateurs Data / Analytics
Consultez ces documents pour comprendre les entrées et sorties :

3. **[Catalogue de Données](configs/Data_Catalog.md)** - Toutes les tables d'entrée/sortie
   - Fichiers mensuels
   - Données de référence
   - Jeux de données en sortie

4. **[Données Disponibles](infos/donnees_disponibles.md)** - Inventaire actuel du datalake

---

## 📖 Documents Disponibles

| Document                                                   | Public Cible      | Ce que vous apprendrez                |
| ---------------------------------------------------------- | ----------------- | ------------------------------------- |
| [**Calculs Assurance**](calculs_assurance_construction.md) | Tout le monde     | Formules d'assurance et règles métier |
| [Flux PTF_MVT](workflows/PTF_MVT_Workflow.md)              | Analystes, Métier | Processus mouvements de portefeuille  |
| [Flux Capitaux](workflows/Capitaux_Workflow.md)            | Analystes, Métier | Processus extraction de capitaux      |
| [Flux Émissions](workflows/Emissions_Workflow.md)          | Analystes, Métier | Processus émissions de primes         |
| [Catalogue de Données](configs/Data_Catalog.md)            | Équipes Data      | Référence tables entrée/sortie        |
| [Données Disponibles](infos/donnees_disponibles.md)        | Équipes Data      | Inventaire actuel du datalake         |

---

## 🔑 Concepts Clés d'Assurance

| Terme                         | Signification         | Exemple                                       |
| ----------------------------- | --------------------- | --------------------------------------------- |
| **AFN** (Affaire Nouvelle)    | Nouvelle police       | Client signe un nouveau contrat               |
| **RES** (Résiliation)         | Résiliation           | Client annule son contrat                     |
| **PTF** (Portefeuille)        | Portefeuille Actif    | Toutes les polices actuellement en vigueur    |
| **SMP**                       | Sinistre Max Possible | Plus gros sinistre que nous pourrions payer   |
| **LCI**                       | Limite Contrat        | Montant maximum stipulé au contrat            |
| **PE** (Perte d'Exploitation) | Interruption Activité | Couverture perte de revenus si arrêt activité |
| **RD** (Risque Direct)        | Dommages Directs      | Couverture dommages physiques aux biens       |
| **Coassurance**               | Partage de Risque     | Plusieurs assureurs partagent la même police  |

Pour les définitions complètes, voir [Calculs Assurance](calculs_assurance_construction.md).

---

## 🚀 Comment Utiliser Cette Documentation

1. **Nouveau sur le projet ?**
   - Commencez par [Calculs Assurance](calculs_assurance_construction.md)
   - Puis lisez le flux de travail correspondant à votre intérêt

2. **Besoin de trouver une table spécifique ?**
   - Vérifiez le [Catalogue de Données](configs/Data_Catalog.md)
   - Ou [Données Disponibles](infos/donnees_disponibles.md)

3. **Besoin de comprendre un calcul ?**
   - Allez sur [Calculs Assurance](calculs_assurance_construction.md)
   - Cherchez la métrique (ex: "AFN", "SMP", "Exposition")

---

## 💡 Notes Importantes

- Toute la documentation est **orientée métier** - pas de code de programmation
- Les formules sont expliquées en **langage clair** avec des exemples
- Les détails techniques d'implémentation sont dans les commentaires du code

---

**Dernière Mise à Jour** : 06/02/2026
**Version** : 1.0
