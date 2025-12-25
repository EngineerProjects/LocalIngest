#!/usr/bin/env python
"""
Générateur de Données Manquantes - Pour Tests Pipeline
=======================================================

Génère UNIQUEMENT les 8 fichiers manquants identifiés pour permettre 
de tester le pipeline en attendant les vraies données de production.

⚠️ ATTENTION : Ces données sont GÉNÉRIQUES et SIMPLIFIÉES
   Pour la comparaison SAS officielle, utilisez les VRAIES données de prod !

Fichiers générés :
   bronze/monthly/
   └── rf_fr1_prm_dtl_midcorp_m_202509.csv
   
   bronze/ref/
   ├── segmentprdt_202509.csv
   ├── ref_mig_azec_vs_ims.csv
   ├── indices.csv
   ├── basecli_inv.csv
   ├── histo_note_risque.csv
   ├── do_dest_202110.csv
   └── table_segmentation_azec_mml.csv

Usage:
    python workenv/generate_missing_data.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random

# Configuration
VISION = "202509"
OUTPUT_BASE = Path().cwd() / "bronze"
MONTHLY_DIR = OUTPUT_BASE / "monthly"
REF_DIR = OUTPUT_BASE / "ref"

# Créer les répertoires
MONTHLY_DIR.mkdir(parents=True, exist_ok=True)
REF_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("GÉNÉRATEUR DE DONNÉES MANQUANTES - TESTS PIPELINE")
print("=" * 80)
print(f"\nGénération pour vision: {VISION}")
print(f"Répertoire de sortie: {OUTPUT_BASE}")
print("\n⚠️  DONNÉES SIMPLIFIÉES - Pour tests uniquement !")
print("=" * 80)


# =============================================================================
# FICHIER #1 : ONE BI PREMIUM DATA (Monthly)
# =============================================================================

def generate_rf_fr1_prm_dtl_midcorp_m():
    """
    Génère rf_fr1_prm_dtl_midcorp_m_202509.csv
    Données One BI Emissions - Respecte TOUS les filtres métier
    """
    print("\n📄 Génération: rf_fr1_prm_dtl_midcorp_m_202509.csv")
    
    n_rows = 502913  # Nombre de lignes
    
    # Produits Construction valides (alignés avec segmentprdt ET PAS 01073 !)
    valid_products = ['01099', '01059', '01111', '01037', '01050']
    
    # Garanties Construction VALIDES (PAS 180, 183, 184, 185)
    valid_guarantees = ['220', '240', '250', '260', '300', '310', '320']
    
    # Catégories VALIDES (éviter 792, 793)
    valid_categories = [f'{i:03d}' for i in range(1, 800) if i not in [792, 793]]
    
    # Intermédiaires VALIDES (EXCLURE les 22 interdits)
    excluded_intermediaries = [
        "102030", "446000", "446118", "446218", "482001", "489090", "500150",
        "4A1400", "4A1500", "4A1600", "4A1700", "4A1800", "4A1900",
        "4F1004", "5B2000", "5R0001",
        "H90036", "H90037", "H90059", "H90061", "H99045", "H99059"
    ]
    all_intermediaries = [f'INT{i:05d}' for i in range(1, 10000)]
    valid_intermediaries = [x for x in all_intermediaries if x not in excluded_intermediaries]
    
    # Canaux distribution
    distribution_channels = ['DCAG', 'DCPS', 'DIGITAL', 'BROKDIV']
    
    # Dates dans vision (CRITIQUE : <= 202509)
    vision_date = datetime(2025, 9, 30)
    
    # Générer des données cohérentes avec les filtres
    data = {
        # ===== FILTRES CRITIQUES =====
        
        # FILTRE #1 : cd_marche='6' (Construction) - MANDATORY
        'cd_marche': ['6'] * n_rows,
        
        # FILTRE #2 : dt_cpta_cts <= 202509
        'dt_cpta_cts': [
            (vision_date - timedelta(days=random.randint(0, 270))).strftime('%Y-%m-%d')
            for _ in range(n_rows)
        ],
        
        # FILTRE #3 : cd_int_stc NOT IN excluded_intermediaries
        'cd_int_stc': [random.choice(valid_intermediaries) for _ in range(n_rows)],
        
        # FILTRE #4 : cd_gar_princ NOT IN ['180','183','184','185']
        'cd_gar_princ': [random.choice(valid_guarantees) for _ in range(n_rows)],
        
        # FILTRE #5 : cd_cat_min NOT IN ['792','793']
        'cd_cat_min': [random.choice(valid_categories) for _ in range(n_rows)],
        
        # FILTRE #6 : cd_prd_prm != '01073'
        'cd_prd_prm': [random.choice(valid_products) for _ in range(n_rows)],
        
        # ===== AUTRES COLONNES REQUISES =====
        
        # Distribution (pour mapping CDPOLE)
        'cd_niv_2_stc': [random.choice(distribution_channels) for _ in range(n_rows)],
        
        # Identifiants
        'nu_cnt_prm': [f'POL{i:08d}' for i in range(1, n_rows + 1)],
        'cd_statu_cts': [random.choice(['ACT', 'VAL']) for _ in range(n_rows)],
        
        # Dates
        'dt_emis_cts': [
            (vision_date - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
            for _ in range(n_rows)
        ],
        'dt_annu_cts': [''] * n_rows,  # Vide
        
        # Montants réalistes (>0 pour éviter filtres)
        'mt_ht_cts': np.random.uniform(100, 50000, n_rows).round(2),
        'mt_cms_cts': np.random.uniform(10, 5000, n_rows).round(2),
        
        # Garantie prospective (format: XX<GAR>YY)
        'cd_gar_prospctiv': [
            f'XX{random.choice(valid_guarantees)}YY'
            for _ in range(n_rows)
        ],
        
        # Exercices (2023-2025 pour mix current/prior)
        'nu_ex_ratt_cts': [
            str(random.choice([2023, 2024, 2025]))
            for _ in range(n_rows)
        ]
    }
    
    df = pd.DataFrame(data)
    
    output_file = MONTHLY_DIR / f"rf_fr1_prm_dtl_midcorp_m_{VISION}.csv"
    df.to_csv(output_file, index=False, sep=',')  # COMMA separator (pas pipe!)
    print(f"   ✓ Créé: {output_file} ({len(df):,} lignes)")
    print(f"   ✓ TOUTES LES LIGNES : cd_marche='6', dt_cpta_cts<='{VISION}'")
    print(f"   ✓ Aucun intermédiaire/garantie/catégorie/produit exclu")
    print(f"   ✓ Ces données devraient SURVIVRE à tous les filtres Emissions")



# =============================================================================
# FICHIER #2 : SEGMENTPRDT (Reference)
# =============================================================================

def generate_segmentprdt():
    """
    Génère segmentprdt_202509.csv
    Segmentation produits - CRITIQUE pour tous les composants
    Doit matcher avec les produits utilisés partout ailleurs
    """
    print("\n📄 Génération: segmentprdt_202509.csv")
    
    # Produits Construction - Alignés avec LOB et autres références
    # Format: 5 caractères commençant par '01'
    products = []
    
    # Produits principaux (utilisés dans emissions, capitaux, etc.)
    main_products = [
        ('01099', '1', '6', '2', '01', 'RC CONSTRUCTEUR'),
        ('01059', '3', '6', '2', '02', 'TRC TOUS RISQUES CHANTIER'),
        ('01111', '1', '6', '2', '03', 'DO DECENNALE'),
        ('01037', '3', '6', '2', '04', 'INCENDIE CONSTRUCTION'),
        ('01050', '1', '6', '2', '05', 'RC ENTREPRISE'),
    ]
    
    for cdprod, cdpole, cmarch, cseg, cssseg, libelle in main_products:
        products.append({
            'CPROD': cdprod,
            'CDPOLE': cdpole,  # 1=Agent, 3=Courtier
            'CMARCH': cmarch,  # 6=Construction (CRITIQUE!)
            'CSEG': cseg,      # 2=Construction segment
            'CSSSEG': cssseg,
            'LIBELLE': libelle
        })
    
    # Ajouter produits additionnels pour volume
    for i in range(6, 301):  # 01006 à 01300
        products.append({
            'CPROD': f'01{i:03d}',
            'CDPOLE': random.choice(['1', '3']),
            'CMARCH': '6',  # Toujours Construction
            'CSEG': '2',    # Toujours segment Construction
            'CSSSEG': f'{(i % 10):02d}',
            'LIBELLE': f'PRODUIT CONSTRUCTION {i}'
        })
    
    df = pd.DataFrame(products)
    
    output_file = REF_DIR / f"segmentprdt_{VISION}.csv"
    df.to_csv(output_file, index=False, sep='|')
    print(f"   ✓ Créé: {output_file} ({len(df):,} lignes)")
    print(f"   ✓ Tous produits: CMARCH='6' (Construction), CSEG='2'")


# =============================================================================
# FICHIER #3 : REF_MIG_AZEC_VS_IMS (Reference)
# =============================================================================

def generate_ref_mig_azec_vs_ims():
    """
    Génère ref_mig_azec_vs_ims.csv
    Référence migration AZEC → IMS (peut être vide)
    """
    print("\n📄 Génération: ref_mig_azec_vs_ims.csv")
    
    # Table vide avec headers corrects (sera remplie manuellement si besoin)
    data = {
        'produit': [],
        'migration_date': [],
        'old_system': [],
        'new_system': []
    }
    
    df = pd.DataFrame(data)
    
    output_file = REF_DIR / "ref_mig_azec_vs_ims.csv"
    df.to_csv(output_file, index=False, sep=',')
    print(f"   ✓ Créé: {output_file} (VIDE - à remplir si nécessaire)")


# =============================================================================
# FICHIER #4 : INDICES (Reference)
# =============================================================================

def generate_indices():
    """
    Génère indices.csv
    Format $INDICE pour indexation capitaux
    """
    print("\n📄 Génération: indices.csv")
    
    # Indices construction simplifiés
    # Format SAS PROC FORMAT: fmtname, start (index_key), label (index_value)
    dates = pd.date_range('2020-01-01', '2025-12-31', freq='M')
    
    data = []
    for nature in ['01', '02', '03']:  # Codes nature construction
        for date in dates:
            # Format MMDDYY (ex: "10925" = Sep 2025)
            date_key = date.strftime('%m%d%y').lstrip('0')
            index_key = f"{nature}{date_key}"
            
            # Indice croissant avec le temps
            base_index = 100
            months_since_2020 = (date.year - 2020) * 12 + date.month
            index_value = base_index + (months_since_2020 * 0.2)  # +0.2% par mois
            
            data.append({
                'fmtname': '$INDICE',
                'start': index_key,
                'end': index_key,
                'label': f"{index_value:.2f}",
                'type': 'C'
            })
    
    df = pd.DataFrame(data)
    
    output_file = REF_DIR / "indices.csv"
    df.to_csv(output_file, index=False, sep=',')
    print(f"   ✓ Créé: {output_file} ({len(df):,} lignes)")


# =============================================================================
# FICHIER #5 : BASECLI_INV (Reference)
# =============================================================================

def generate_basecli_inv():
    """
    Génère basecli_inv.csv
    Base clients W6 - Fallback ISIC via W6
    Doit avoir des NOCLT qui matchent avec les polices
    """
    print("\n📄 Génération: basecli_inv.csv")
    
    n_clients = 500
    
    # NAF 2008 codes pour Construction (cohérents avec le pipeline)
    naf_construction = [
        '4120A', '4120B',  # Construction bâtiments résidentiels
        '4211Z', '4212Z',  # Routes, autoroutes
        '4213A', '4213B',  # Ponts, tunnels
        '4221Z', '4222Z',  # Réseaux fluides
        '4299Z',           # Autres travaux construction spécialisés
        '4311Z', '4312A', '4312B',  # Démolition, préparation
        '4313Z',           # Forages, sondages
        '4321A', '4321B',  # Installation électrique
        '4322A', '4322B',  # Plomberie, chauffage
        '4329A', '4329B',  # Autres travaux installation
        '4331Z', '4332A', '4332B', '4332C',  # Plâtrerie, menuiserie
        '4333Z', '4334Z',  # Revêtements sols/murs, peinture
        '4339Z',           # Autres travaux finition
        '4391A', '4391B',  # Travaux toiture
        '4399A', '4399B', '4399C', '4399D'  # Autres travaux spécialisés
    ]
    
    # Codes W6 Construction (pour fallback ISIC)
    w6_codes = ['4120', '4211', '4299', '4312', '4321', '4332', '4391']
    
    data = {
        'noclt': [f'CLI{i:08d}' for i in range(1, n_clients + 1)],
        'w6': [random.choice(w6_codes) for _ in range(n_clients)],
        'cdnaf': [random.choice(naf_construction) for _ in range(n_clients)],
        'cdnaf_client': [random.choice(naf_construction) for _ in range(n_clients)],
        'raison_sociale': [f'ENTREPRISE CONSTRUCTION {i}' for i in range(1, n_clients + 1)],
        'siren': [f'{random.randint(100000000, 999999999)}' for _ in range(n_clients)]
    }
    
    df = pd.DataFrame(data)
    
    output_file = REF_DIR / "basecli_inv.csv"
    df.to_csv(output_file, index=False, sep='|')
    print(f"   ✓ Créé: {output_file} ({len(df):,} lignes)")
    print(f"   ✓ NAF codes Construction, W6 codes valides")


# =============================================================================
# FICHIER #6 : HISTO_NOTE_RISQUE (Reference)
# =============================================================================

def generate_histo_note_risque():
    """
    Génère histo_note_risque.csv
    Notation risque Euler Hermes - BINSEE
    """
    print("\n📄 Génération: histo_note_risque.csv")
    
    n_notations = 300
    
    # Générer historique de notations
    data = {
        'siren': [f'{random.randint(100000000, 999999999)}' for _ in range(n_notations)],
        'date_notation': [(datetime(2025, 9, 1) - timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d') 
                          for _ in range(n_notations)],
        'note_risque': np.random.choice(['A', 'B', 'C', 'D', 'E'], n_notations),
        'score_defaillance': np.random.uniform(0, 100, n_notations).round(2)
    }
    
    df = pd.DataFrame(data)
    
    output_file = REF_DIR / "histo_note_risque.csv"
    df.to_csv(output_file, index=False, sep='|')
    print(f"   ✓ Créé: {output_file} ({len(df):,} lignes)")


# =============================================================================
# FICHIER #7 : DO_DEST (Reference)
# =============================================================================

def generate_do_dest():
    """
    Génère do_dest_202110.csv
    Référentiel destination chantier construction
    """
    print("\n📄 Génération: do_dest_202110.csv")
    
    # Destinations construction principales
    data = {
        'nopol': [f'POL{i:08d}' for i in range(1, 201)],
        'destination': np.random.choice([
            'RESIDENTIEL', 'BUREAU', 'INDUSTRIE', 'COMMERCE',
            'HOPITAL', 'ECOLE', 'PARKING'
        ], 200),
        'type_chantier': np.random.choice(['NEUF', 'RENOVATION', 'EXTENSION'], 200),
        'surface_m2': np.random.uniform(100, 10000, 200).round(0)
    }
    
    df = pd.DataFrame(data)
    
    output_file = REF_DIR / "do_dest_202110.csv"
    df.to_csv(output_file, index=False, sep=',')
    print(f"   ✓ Créé: {output_file} ({len(df):,} lignes)")


# =============================================================================
# FICHIER #8 : TABLE_SEGMENTATION_AZEC_MML (Reference)
# =============================================================================

def generate_table_segmentation_azec_mml():
    """
    Génère table_segmentation_azec_mml.csv
    Segmentation AZEC spécifique - Doit avoir CMARCH=6
    """
    print("\n📄 Génération: table_segmentation_azec_mml.csv")
    
    # Produits AZEC Construction avec type_produit_2
    # CRITIQUE: CMARCH doit être '6' sinon filtre AZEC vide les données
    data = {
        'produit': ['01099', '01059', '01111', '01037', '01050'],
        'type_produit_2': ['MRP', 'MRP', 'TRC', 'INCENDIE', 'RC'],
        'cmarch': ['6', '6', '6', '6', '6'],  # CRITIQUE: Construction market
        'segment_azec': ['PROF', 'CONST', 'CONST', 'INCEN', 'ENTR'],
        'libelle': [
            'MULTIRISQUE PROFESSIONNEL',
            'MULTIRISQUE PROFESSIONNEL',
            'TOUS RISQUES CHANTIER',
            'INCENDIE',
            'RESPONSABILITE CIVILE'
        ]
    }
    
    df = pd.DataFrame(data)
    
    output_file = REF_DIR / "table_segmentation_azec_mml.csv"
    df.to_csv(output_file, index=False, sep=',')
    print(f"   ✓ Créé: {output_file} ({len(df):,} lignes)")
    print(f"   ✓ Tous produits: CMARCH='6' (CRITIQUE pour filtres AZEC)")


# =============================================================================
# MAIN - GÉNÉRER TOUS LES FICHIERS
# =============================================================================

if __name__ == "__main__":
    try:
        # Fichier #1 - Monthly
        generate_rf_fr1_prm_dtl_midcorp_m()
        
        # Fichiers #2-8 - Reference
        generate_segmentprdt()
        generate_ref_mig_azec_vs_ims()
        generate_indices()
        generate_basecli_inv()
        generate_histo_note_risque()
        generate_do_dest()
        generate_table_segmentation_azec_mml()
        
        print("\n" + "=" * 80)
        print("✅ GÉNÉRATION TERMINÉE")
        print("=" * 80)
        print(f"\nFichiers créés dans: {OUTPUT_BASE}")
        print(f"   - Monthly: {MONTHLY_DIR}")
        print(f"   - Reference: {REF_DIR}")
        print("\nProchaines étapes:")
        print("   1. Copier bronze/monthly/* vers /workspace/datalake/bronze/2025/09/")
        print("   2. Copier bronze/ref/* vers /workspace/datalake/bronze/ref/")
        print("   3. Tester le pipeline PySpark")
        print("\n⚠️  RAPPEL: Utilisez les VRAIES données de prod pour la comparaison SAS finale !")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
