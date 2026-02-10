"""
Constantes métier pour le pipeline Construction.

Ce module définit les constantes métier utilisées dans les processeurs :
- Codes Direction Commerciale (DIRCOM)
- Codes Pôle (canal de distribution)  
- Codes Marché (MARKET_CODE)
"""

# =============================================================================
# Directions commerciales
# =============================================================================

class DIRCOM:
    """Codes des directions commerciales"""
    AZ = "AZ"        # Canal agents et courtiers
    AZEC = "AZEC"    # Canal construction (courtage)


# =============================================================================
# Canaux de distribution
# =============================================================================

class POLE:
    """
    Codes des canaux de distribution.
    
    Valeurs:
        - '1' = Agents (DCAG, DCPS, DIGITAL)
        - '3' = Courtage (BROKDIV, BROKMID)
    """
    AGENT = "1"      # Agents
    COURTAGE = "3"   # Courtage


# =============================================================================
# Codes marché et segment
# =============================================================================

class MARKET_CODE:
    """
    Codes marché utilisés pour filtrer les données construction.
    
    Convention de nommage des fichiers (couche Bronze) :
    - IPFE**1**6 = Agent (1) + Construction (6)
    - IPFE**3**6 = Courtage (3) + Construction (6)
    
    Premier chiffre : Canal de distribution (1=Agent, 3=Courtage)
    Second chiffre : Marché/Ligne produit (6=Construction, 7=Santé, etc.)
    
    Filtrage des données :
    - Ces constantes filtrent les DONNÉES via les colonnes 'cmarch' et 'csegt'
    - Le filtre est appliqué APRÈS lecture des fichiers
    - cmarch="6" signifie qu'on traite uniquement les enregistrements Construction
    
    Pour changer de marché :
    1. Mettre à jour les patterns de fichiers dans reading_config.json
    2. Mettre à jour ces constantes
    3. Aucun changement de code nécessaire !
    """
    MARKET = "6"   # Code marché Construction (correspond à IPFE1**6**, IPFE3**6**)
    SEGMENT = "2"  # Code segment
