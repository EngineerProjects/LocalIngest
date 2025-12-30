# Guide Réunion - Clarification Table INDICES

**Date prévue** : 2025-12-30  
**Sujet** : Besoin de clarification sur la structure de la table INDICES pour migration Python  
**Durée estimée** : 15-20 minutes

---

## 📋 PARTIE 1 : Contexte à expliquer (5 min)

### Votre introduction

> "Bonjour, je travaille actuellement sur la migration SAS → Python du processus CAPITAUX.  
> J'ai terminé la Phase 1 de l'implémentation de l'indexation des capitaux en Python.  
> Pour compléter la Phase 2, j'ai besoin de comprendre précisément la structure de la table INDICES  
> utilisée par la macro `indexation_v2.sas`.  
> 
> J'ai analysé le code SAS et identifié que cette table est essentielle pour le calcul  
> des indices de coût de construction, mais je n'ai pas accès à sa structure exacte."

### Points clés à mentionner

✅ **Ce qui fonctionne déjà** :
- Le Cas 1 (utilisation PRPRVC directement) est implémenté et testé
- Les formules d'indexation sont correctes
- La logique de dates anniversaire fonctionne

⚠️ **Ce qui est bloqué** :
- Le Cas 2 (lookup dans INDICES) nécessite la vraie structure de la table
- Le format de la clé de lookup n'est pas clair
- Le schéma Python actuel est probablement incomplet

---

## 💻 PARTIE 2 : Code SAS à montrer (10 min)

### Extrait 1 : Déclaration de la librairie INDICES

**Fichier** : `CAPITAUX_RUN.sas` ligne 132

```sas
LIBNAME INDICES 'infp.ima0p6$$.nautind3' DISP=SHR SERVER=SERVEUR;
```

**Ce que vous dites** :
> "Voici où la table INDICES est déclarée. Elle est située sur le mainframe  
> à l'emplacement `infp.ima0p6$$.nautind3`."

---

### Extrait 2 : Utilisation dans indexation_v2.sas

**Fichier** : `indexation_v2.sas` lignes 32-36

```sas
%MACRO indexation_v2(DATE = ., IND = 1, NOMMT = MTCAPI, NOMNAT = CDPRVB, NOMIND = PRPRVC);

    OPTIONS FMTSEARCH=(INDICES);
    FORMAT VAL1 VAL2 $8.;
    FORMAT DATE DDMMYY10.;
```

**Ce que vous dites** :
> "La macro utilise `OPTIONS FMTSEARCH=(INDICES)` ce qui signifie qu'elle cherche  
> un format SAS dans cette librairie."

---

### Extrait 3 : Construction de la clé et lookup

**Fichier** : `indexation_v2.sas` lignes 75-76, 84-86

```sas
/* Ligne 75-76 : Indice d'origine */
VAL1 = &NOMNAT&IND. !! PUT(DTEFSITT, Z5.);
IF SUBSTR(VAL1, 1, 1) = '0' THEN INDXORIG = PUT(VAL1, $INDICE.);
ELSE INDXORIG = 1;

/* Ligne 84-86 : Indice cible */
VAL2 = &NOMNAT&IND. !! PUT(DATE, Z5.);
IF SUBSTR(VAL2, 1, 1) = '0' THEN INDXINTG = PUT(VAL2, $INDICE.);
ELSE INDXINTG = 1;
```

**Ce que vous dites** :
> "Voici la partie critique : le code construit une clé composite en concaténant :
> 1. Le code nature de construction (`CDPRVB1-14`), par exemple '01'  
> 2. Une date formatée avec `PUT(date, Z5.)` - **format que je ne comprends pas**  
> 
> Puis il utilise `PUT(clé, $INDICE.)` pour récupérer la valeur de l'indice.  
> 
> **Question** : Comment ce format `$INDICE` est-il construit ?"

---

### Extrait 4 : Colonnes d'entrée (pour référence)

**Fichier** : `CAPITAUX_AZ_MACRO.sas` lignes 66-69

```sas
CDPRVB1,CDPRVB2,CDPRVB3,...,CDPRVB14,     /* Codes nature construction */
/* Coefficients d'évolution -> indice de la 1ère année */
PRPRVC1,PRPRVC2,PRPRVC3,...,PRPRVC14,
```

**Ce que vous dites** :
> "Ces colonnes viennent de la table IPF. Les PRPRVC sont les 'coefficients d'évolution'  
> qui représentent l'indice de la première année. Pour le Cas 2, on doit chercher  
> de nouveaux indices dans INDICES."

---

## ❓ PARTIE 3 : Questions à poser (5 min)

### Question 1 : Structure de la table INDICES ⭐ CRITIQUE

**Demande précise** :
> "Quelle est la structure exacte de la table `infp.ima0p6$$.nautind3` ?"

**Options à clarifier** :

**Option A** : Format SAS pré-compilé
```
Structure : Catalog de formats
Colonnes : fmtname, start, end, label, type
```

**Option B** : Table source avec colonnes séparées
```
Structure : Table classique
Colonnes : code_nature, annee, mois, indice
OU : code_nature, date_debut, date_fin, indice
```

**Option C** : Table avec clé pré-calculée
```
Structure : Table avec clé composite
Colonnes : cle_composite, indice
```

**Pourquoi c'est important** :
> "Sans connaître la structure exacte, je ne peux pas faire la jointure correctement en Python."

---

### Question 2 : Format Z5. pour les dates ⭐ CRITIQUE

**Demande précise** :
> "Que produit exactement `PUT(date_sas, Z5.)` pour une date ?  
> Par exemple, pour la date 15/01/2020, quelle est la valeur retournée ?"

**Exemples à tester** :
- `PUT('15JAN2020'd, Z5.)` = ?
- `PUT('01JUN1975'd, Z5.)` = ?

**Hypothèses à vérifier** :
- Est-ce le nombre de jours depuis 01/01/1960 ? (ex: `20154`)
- Est-ce un format MMYYD ? (ex: `12754`)
- Est-ce un format YYDDD ? (ex: `20015`)

**Pourquoi c'est important** :
> "En Python, je dois recréer exactement le même format pour construire la clé de lookup."

---

### Question 3 : Échantillon de données

**Demande précise** :
> "Pouvez-vous m'exporter un échantillon de 50-100 lignes de la table INDICES en CSV ?  
> Avec toutes les colonnes, pas seulement un subset."

**Format souhaité** :
```
- Format : CSV
- Séparateur : | ou ;
- Encodage : LATIN9 ou UTF-8
- Nombre de lignes : 50-100
```

**Pourquoi c'est important** :
> "Cela me permettra de :  
> 1. Vérifier la structure réelle  
> 2. Voir des exemples de clés  
> 3. Tester ma logique Python avec de vraies données"

---

### Question 4 : Documentation technique

**Demande précise** :
> "Existe-t-il une documentation technique sur :  
> - La table INDICES / NAUTIND3  
> - Le format $INDICE  
> - Comment il est créé/maintenu ?"

**Si oui, demander** :
- Schéma de la table
- Dictionnaire de données
- Scripts de création
- Fréquence de mise à jour

**Pourquoi c'est important** :
> "Cela m'aidera à comprendre le contexte métier et à anticiper d'éventuels changements."

---

## 📦 PARTIE 4 : Livrables à demander

### Livrable 1 : Structure de table ⭐ PRIORITÉ 1

**Format** :
```sql
PROC SQL;
    DESCRIBE TABLE INDICES.NAUTIND3;
QUIT;
```

OU un simple listing :
```
Nom colonne    | Type      | Longueur | Description
---------------|-----------|----------|-------------
code_nature    | CHAR      | 2        | Code CDPRVB
annee          | CHAR      | 4        | Année
mois           | CHAR      | 2        | Mois
indice         | NUM       | 8        | Valeur indice
```

---

### Livrable 2 : Échantillon CSV ⭐ PRIORITÉ 1

**Exemple de ce que vous attendez** :
```csv
code_nature|annee|mois|indice
01|2020|01|112.5
01|2020|02|113.2
01|2020|03|114.1
02|2020|01|105.3
...
```

OU si c'est un format catalog :
```csv
fmtname|start|end|label|type
$INDICE|0120154|0120154|112.5|C
$INDICE|0120228|0120228|113.2|C
...
```

---

### Livrable 3 : Test format Z5. ⭐ PRIORITÉ 2

**Script SAS simple** :
```sas
data _null_;
    date1 = '15JAN2020'd;
    date2 = '01JUN1975'd;
    
    put "Date 1 (15JAN2020) : " date1 Z5.;
    put "Date 2 (01JUN1975) : " date2 Z5.;
run;
```

**Résultat attendu** :
```
Date 1 (15JAN2020) : XXXXX
Date 2 (01JUN1975) : XXXXX
```

---

### Livrable 4 : Documentation (optionnel)

- PDF/Word de documentation INDICES
- Scripts de création du format $INDICE
- Notes de maintenance

---

## 📝 PARTIE 5 : Récapitulatif de fin de réunion

### Ce que vous devez avoir à la fin

✅ **Minimum vital** :
1. Structure exacte de INDICES (colonnes + types)
2. Échantillon CSV (50-100 lignes)
3. Explication du format Z5. pour dates

✅ **Idéal** :
4. Documentation technique
5. Contact personne ressource si questions
6. Délai de livraison des fichiers

---

## 🎯 Script de clôture

**À dire en conclusion** :
> "Merci beaucoup pour ces informations. Avec la structure de INDICES et l'échantillon CSV,  
> je vais pouvoir :  
> 
> 1. Corriger le schéma Python  
> 2. Implémenter la bonne logique de lookup  
> 3. Tester avec des données réelles  
> 4. Valider que Python produit exactement les mêmes résultats que SAS  
> 
> Je reviendrai vers vous une fois la Phase 2 terminée pour validation.  
> Avez-vous des questions ou des recommandations supplémentaires ?"

---

## 📌 Notes importantes

### Si votre supérieur ne connaît pas les détails

**Alternative** :
> "Pouvez-vous me mettre en contact avec la personne qui maintient cette table  
> ou qui a écrit le code `indexation_v2.sas` à l'origine ?"

### Si la structure est complexe

**Demander** :
> "Pouvons-nous planifier une session de 30 minutes où nous regardons  
> ensemble la table sur SAS pour que je comprenne sa structure ?"

### Si les données sont sensibles

**Rassurer** :
> "Je peux signer une clause de confidentialité si nécessaire.  
> J'ai seulement besoin de quelques exemples pour comprendre la structure,  
> pas de l'historique complet."

---

## ✅ Checklist avant la réunion

- [ ] Lire ce guide complet
- [ ] Préparer les extraits de code SAS (les avoir sous la main)
- [ ] Avoir un bloc-notes pour noter les réponses
- [ ] Préparer un endroit pour recevoir les fichiers (email, Teams, etc.)
- [ ] Relire l'audit (`indexation_sas_vs_python_audit.md`)
- [ ] Être prêt à expliquer pourquoi c'est bloquant

---

## 📊 Tableau de suivi des réponses

| Question | Réponse obtenue | Notes | Statut |
|----------|-----------------|-------|--------|
| Structure INDICES | | | ⏳ |
| Format Z5. | | | ⏳ |
| Échantillon CSV | | | ⏳ |
| Documentation | | | ⏳ |

**Bon courage pour votre réunion ! 💪**
