/******************************************************************************
 * CONNEXIONS AUX LIBRAIRIES SAS - DONNÉES MANQUANTES
 * 
 * Description: Déclare simplement les LIBNAME pour accéder aux données
 *              Vous exportez manuellement ce dont vous avez besoin
 * 
 * Utilisation:
 *   1. Exécuter ce script → connexions établies
 *   2. Ouvrir SAS Explorer → chercher les tables listées ci-dessous
 *   3. Clic droit sur table → Export Data → CSV
 *   4. Fermer SAS → connexions automatiquement fermées
 * 
 * Avantages:
 *   ✓ Aucun fichier créé automatiquement
 *   ✓ Vous choisissez exactement ce que vous exportez
 *   ✓ Connexions temporaires (fermées à la sortie)
 *   ✓ Zéro risque de supprimer quoi que ce soit
 ******************************************************************************/

%put ;
%put ========================================================================;
%put DÉCLARATION DES LIBRAIRIES - DONNÉES MANQUANTES;
%put ========================================================================;
%put ;

/* ============================================================================
 * LIBRAIRIE 1 : SEG - Segmentation Produits
 * ============================================================================ */
LIBNAME SEG "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/SEG_PRDTS";

%put ✓ SEG : Segmentation produits;
%put   → Chercher: segmentprdt_202509;
%put ;

/* ============================================================================
 * LIBRAIRIE 2 : MIG_AZEC - Référentiels AZEC
 * ============================================================================ */
LIBNAME MIG_AZEC "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL";

%put ✓ MIG_AZEC : Référentiels migration AZEC;
%put   → Chercher: ref_mig_azec_vs_ims (peut ne pas exister);
%put ;

/* ============================================================================
 * LIBRAIRIE 3 : INDICES - Indices Construction (Mainframe)
 * ============================================================================ */
LIBNAME INDICES 'INFP.IMA0P6$$.NAUTIND3' DISP=SHR SERVER=SERVEUR;

%put ✓ INDICES : Format $INDICE pour indexation capitaux;
%put   → Chercher: Formats catalog (OPTIONS FMTSEARCH=INDICES);
%put   → Note: Export du format $INDICE (voir PROC FORMAT CNTLOUT);
%put ;

/* ============================================================================
 * LIBRAIRIE 4 : W6 - Base Clients W6
 * ============================================================================ */
LIBNAME W6 "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/ALEX_MIS/basecli_cumule";

%put ✓ W6 : Base clients W6;
%put   → Chercher: basecli_inv (peut être compressé .gz);
%put ;

/* ============================================================================
 * LIBRAIRIE 5 : BINSEE - Euler Hermes
 * ============================================================================ */
LIBNAME BINSEE "/sasprod/prod/prod/run/azi/d2d/w6/particuliers/dm_crm/partage/param/pro";

%put ✓ BINSEE : Notation risque Euler Hermes;
%put   → Chercher: histo_note_risque;
%put ;

/* ============================================================================
 * LIBRAIRIE 6 : DEST - Destination Chantiers
 * ============================================================================ */
LIBNAME DEST "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/Construction";

%put ✓ DEST : Destination construction;
%put   → Chercher: do_dest202110;
%put ;

/* ============================================================================
 * LIBRAIRIE 7 : REF - Référentiels Divers
 * ============================================================================ */
LIBNAME REF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref";

%put ✓ REF : Référentiels transverses;
%put   → Chercher: table_segmentation_azec_mml;
%put ;

/* ============================================================================
 * RÉSUMÉ DES TABLES À EXPORTER
 * ============================================================================ */

%put ;
%put ========================================================================;
%put TABLES À RECHERCHER ET EXPORTER MANUELLEMENT;
%put ========================================================================;
%put ;
%put 1. SEG.segmentprdt_202509;
%put    → Export vers: segmentprdt_202509.csv (délimiteur: |);
%put ;
%put 2. MIG_AZEC.ref_mig_azec_vs_ims;
%put    → Export vers: ref_mig_azec_vs_ims.csv (délimiteur: ,);
%put    → (Si n'existe pas: ignorer);
%put ;
%put 3. INDICES - Format $INDICE;
%put    → Utiliser: PROC FORMAT LIB=INDICES CNTLOUT=work.indices;
%put    → Filtrer: WHERE fmtname='INDICE';
%put    → Export vers: indices.csv;
%put ;
%put 4. W6.basecli_inv;
%put    → Export vers: basecli_inv.csv (délimiteur: |);
%put ;
%put 5. BINSEE.histo_note_risque;
%put    → Export vers: histo_note_risque.csv (délimiteur: |);
%put ;
%put 6. DEST.do_dest202110;
%put    → Export vers: do_dest_202110.csv (délimiteur: ,);
%put ;
%put 7. REF.table_segmentation_azec_mml;
%put    → Export vers: table_segmentation_azec_mml.csv (délimiteur: ,);
%put ;
%put ========================================================================;
%put ;
%put INSTRUCTIONS EXPORT MANUEL:;
%put ;
%put   1. Ouvrir SAS Explorer (barre latérale ou View → Explorer);
%put   2. Naviguer vers la librairie (ex: SEG);
%put   3. Clic droit sur la table → Export;
%put   4. Choisir Format: CSV, Délimiteur selon indications ci-dessus;
%put   5. Sauvegarder directement dans votre dossier Downloads;
%put ;
%put À la fermeture de SAS:;
%put   → Connexions automatiquement fermées;
%put   → Aucune trace laissée sur le serveur;
%put ;
%put ========================================================================;
%put ;

/* ============================================================================
 * ONE BI - CONNEXION SÉPARÉE (SI DROITS DISPONIBLES)
 * ============================================================================ */

%put ;
%put ========================================================================;
%put ONE BI - CONNEXION MANUELLE REQUISE;
%put ========================================================================;
%put ;
%put Pour rf_fr1_prm_dtl_midcorp_m_202509:;
%put ;
%put   1. Demander droits One BI à votre supérieur;
%put   2. Utiliser interface web One BI ou SAS Connect;
%put   3. Requête SQL:;
%put      SELECT * FROM rf_fr1_prm_dtl_midcorp_m;
%put      WHERE cd_marche = '6' AND DT_CPTA_CTS <= '202509';
%put   4. Export vers: rf_fr1_prm_dtl_midcorp_m_202509.csv;
%put ;
%put ========================================================================;

/* FIN DU SCRIPT - LIBRAIRIES ACTIVES */
