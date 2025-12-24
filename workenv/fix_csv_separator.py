#!/usr/bin/env python
"""
Convertisseur de Séparateur CSV - Optimisé pour Gros Fichiers
==============================================================

Convertit ; vers | dans des fichiers CSV volumineux (40M+ lignes)
en utilisant le streaming pour éviter de saturer la mémoire.

Gère automatiquement l'encodage LATIN9 (ISO-8859-15).

Usage:
    # Un seul fichier
    python workenv/fix_csv_separator.py ~/Downloads/segmentprdt_202509.csv
    
    # Plusieurs fichiers
    python workenv/fix_csv_separator.py ~/Downloads/*.csv
    
    # Conserver l'original
    python workenv/fix_csv_separator.py --keep-original ~/Downloads/file.csv
"""

import sys
import argparse
from pathlib import Path
from typing import List

# Taille du buffer pour lecture/écriture (10 MB)
BUFFER_SIZE = 10 * 1024 * 1024


def convert_separator_streaming(
    input_file: Path,
    output_file: Path = None,
    keep_original: bool = False,
    encoding: str = 'latin-9'
):
    """
    Convertit ; vers | en streaming (mémoire constante).
    
    Args:
        input_file: Fichier CSV source
        output_file: Fichier CSV destination (None = écraser)
        keep_original: Garder l'original (crée .bak)
        encoding: Encodage du fichier (latin-9 pour SAS)
    """
    
    # Déterminer fichier output
    if output_file is None:
        if keep_original:
            # Créer backup
            backup = input_file.with_suffix('.csv.bak')
            input_file.rename(backup)
            output_file = input_file
            input_file = backup
        else:
            # Fichier temporaire
            output_file = input_file.with_suffix('.tmp')
    
    print(f"📄 Conversion: {input_file.name}")
    print(f"   Encodage: {encoding}")
    print(f"   ; -> |")
    
    # Compteurs
    lines_processed = 0
    bytes_processed = 0
    
    try:
        # Ouvrir en streaming
        with open(input_file, 'r', encoding=encoding, errors='replace') as f_in, \
             open(output_file, 'w', encoding=encoding, buffering=BUFFER_SIZE) as f_out:
            
            # Traiter ligne par ligne (streaming)
            for line in f_in:
                # Convertir séparateur
                converted_line = line.replace(';', '|')
                f_out.write(converted_line)
                
                lines_processed += 1
                bytes_processed += len(line.encode(encoding))
                
                # Afficher progression tous les 1M lignes
                if lines_processed % 1_000_000 == 0:
                    mb_processed = bytes_processed / (1024 * 1024)
                    print(f"   ⏳ {lines_processed:,} lignes ({mb_processed:.1f} MB)")
        
        # Si temp file, renommer
        if output_file.suffix == '.tmp' and not keep_original:
            output_file.replace(input_file)
            output_file = input_file
        
        # Stats finales
        final_size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"   ✓ Terminé: {lines_processed:,} lignes ({final_size_mb:.1f} MB)")
        
        if keep_original:
            print(f"   ✓ Original sauvegardé: {input_file}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ ERREUR: {e}")
        
        # Nettoyer fichier temporaire en cas d'erreur
        if output_file.exists() and output_file.suffix == '.tmp':
            output_file.unlink()
        
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Convertit séparateur CSV ; vers | (optimisé gros fichiers)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        'files',
        nargs='+',
        help='Fichiers CSV à convertir'
    )
    
    parser.add_argument(
        '--keep-original',
        action='store_true',
        help='Garder fichier original (.bak)'
    )
    
    parser.add_argument(
        '--encoding',
        default='latin-9',
        help='Encodage fichier (défaut: latin-9 pour SAS)'
    )
    
    args = parser.parse_args()
    
    # Convertir tous les fichiers
    files = []
    for pattern in args.files:
        path = Path(pattern).expanduser()
        if path.is_file():
            files.append(path)
        elif '*' in pattern:
            # Glob pattern
            files.extend(Path().glob(pattern))
    
    if not files:
        print("❌ Aucun fichier trouvé")
        sys.exit(1)
    
    print("=" * 80)
    print(f"CONVERSION SÉPARATEUR CSV - {len(files)} fichier(s)")
    print("=" * 80)
    print()
    
    success_count = 0
    for file in files:
        if convert_separator_streaming(
            file,
            keep_original=args.keep_original,
            encoding=args.encoding
        ):
            success_count += 1
        print()
    
    print("=" * 80)
    print(f"✓ {success_count}/{len(files)} fichiers convertis avec succès")
    print("=" * 80)


if __name__ == "__main__":
    main()
