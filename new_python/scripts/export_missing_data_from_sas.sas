/******************************************************************************
 * SCRIPT D'EXPORT DES DONNÉES MANQUANTES
 * 
 * Description: Exporte tous les fichiers manquants depuis SAS Production
 *              vers des fichiers CSV pour le pipeline PySpark
 * 
 * Prérequis: 
 *   - Accès SAS Production
 *   - Droits lecture sur toutes les librairies
 *   - Connexion One BI (pour fichier #1 uniquement)
 * 
 * Résultat: 8 fichiers CSV dans /export/
 ******************************************************************************/

/* ============================================================================
 * CONFIGURATION - À ADAPTER SELON VOTRE ENVIRONNEMENT
 * ============================================================================ */

%let vision = 202509;

/* ⚠️ IMPORTANT : CHOISIR LE BON CHEMIN D'EXPORT ⚠️ */

/* OPTION 1 : Lecteur réseau monté sur votre PC (RECOMMANDÉ) */
/* %let export_path = H:/exports/sas_missing_data; */

/* OPTION 2 : SAS Desktop installé localement sur votre PC */
/* %let export_path = C:/Users/votre_nom/Downloads/sas_exports; */

/* OPTION 3 : Serveur SAS (nécessite téléchargement manuel après) */
%let export_path = /sasprod/temp/exports/missing_data_&vision.;

/* OPTION 4 : Répertoire temporaire WORK (⚠️ SERA SUPPRIMÉ à la fermeture!) */
/* %let export_path = %sysfunc(pathname(work))/exports; */

/* Vérifier le chemin et afficher l'emplacement */
%put ;
%put ========================================================================;
%put IMPORTANT : EMPLACEMENT DES FICHIERS EXPORTÉS;
%put ========================================================================;
%put Chemin configuré : &export_path.;
%put ;
%if %index(&export_path., :) > 0 or %index(&export_path., //) > 0 %then %do;
    %put ✓ Export vers votre PC ou lecteur réseau;
    %put ✓ Fichiers accessibles directement après export;
%end;
%else %do;
    %put ⚠️ Export vers le SERVEUR SAS : &export_path.;
    %put ⚠️ Vous devrez TÉLÉCHARGER les fichiers manuellement après export;
    %put ⚠️ Les fichiers RESTENT sur le serveur (pas supprimés à la fermeture);
%end;
%put ========================================================================;
%put ;

/* Créer le répertoire d'export */
x "mkdir -p &export_path.";

/* ============================================================================
 * DÉCLARATION DES LIBRAIRIES (DEPUIS PTF_MVTS_RUN.sas)
 * ============================================================================ */

/* SEG - Segmentation produits */
LIBNAME SEG "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/SEG_PRDTS";

/* MIG_AZEC - Référentiels migration AZEC */
LIBNAME MIG_AZEC "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL";

/* INDICES - Indices construction (mainframe) */
LIBNAME INDICES 'INFP.IMA0P6$$.NAUTIND3' DISP=SHR SERVER=SERVEUR;

/* W6 - Base clients W6 */
LIBNAME W6 "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/ALEX_MIS/basecli_cumule";

/* BINSEE - Euler Hermes */
LIBNAME BINSEE "/sasprod/prod/prod/run/azi/d2d/w6/particuliers/dm_crm/partage/param/pro";

/* DEST - Destination chantiers */
LIBNAME DEST "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/Construction";

/* REF - Référentiels divers */
LIBNAME REF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref";


/* ============================================================================
 * FICHIER #1 : ONE BI PREMIUM DATA - NÉCESSITE CONNEXION ONE BI
 * ============================================================================ */

/*
 * ⚠️ ATTENTION : CE FICHIER NÉCESSITE UNE CONNEXION ONE BI
 * 
 * Instructions de connexion (voir EMISSIONS_RUN.sas L65-92):
 * 
 * 1. Vérifier l'encodage UTF-8
 * 2. Se connecter au serveur One BI
 * 3. Allouer la librairie PRM
 * 
 * Décommentez et exécutez ce bloc UNIQUEMENT si vous avez les droits One BI:
 */

/*
%if "&SYSENCODING." = "utf-8" %then %do;
    options source compress=yes;
    %let server_to_use= biaa-sg-prod.srv.allianz;
    %let port_connect =7556;
    options netencryptalgorithm=(AES);
    %let remhost=&server_to_use &port_connect;
    options comamid=tcp remote=remhost;
    
    * REMPLACER &MPSSOX par votre mot de passe One BI ;
    signon noscript userid=%upcase(&sysuserid.) password="&MPSSOX";
    
    * Allocation PRM sur session distante ;
    rsubmit;
        libname prm "/var/opt/data/data_project/sas94/bifr/azfronebi/data/shared_lib/prm";
    endrsubmit;
    
    libname prm server=remhost;
    
    * Export données One BI ;
    rsubmit;
        proc sql;
            create table prm_extract as
            select *
            from PRM.rf_fr1_prm_dtl_midcorp_m
            where cd_marche in ("6") 
              and DT_CPTA_CTS <= "&vision.";
        quit;
        
        proc download data=prm_extract 
                     out=work.rf_fr1_prm_dtl_midcorp_m_&vision.;
        run;
    endrsubmit;
    
    * Export en CSV ;
    proc export data=work.rf_fr1_prm_dtl_midcorp_m_&vision.
        outfile="&export_path./rf_fr1_prm_dtl_midcorp_m_&vision..csv"
        dbms=csv replace;
        delimiter=',';
    run;
    
    * Déconnexion ;
    signoff;
%end;
%else %do;
    %put ERROR: L'encodage doit être UTF-8 pour la connexion One BI;
%end;
*/

%put NOTE: ========================================================;
%put NOTE: Fichier #1 (One BI) SKIPPÉ - Nécessite connexion dédiée;
%put NOTE: Demandez les droits One BI à votre supérieur;
%put NOTE: ========================================================;


/* ============================================================================
 * FICHIER #2 : SEGMENTPRDT - SEGMENTATION PRODUIT
 * ============================================================================ */

%put NOTE: Extraction Fichier #2 - segmentprdt_&vision.;

proc export data=SEG.segmentprdt_&vision.
    outfile="&export_path./segmentprdt_&vision..csv"
    dbms=csv replace;
    delimiter='|';
run;

%put NOTE: ✓ segmentprdt_&vision..csv exporté;


/* ============================================================================
 * FICHIER #3 : REF_MIG_AZEC_VS_IMS - MIGRATION AZEC
 * ============================================================================ */

%put NOTE: Extraction Fichier #3 - ref_mig_azec_vs_ims;

/* Vérifier si la table existe */
%macro export_mig_azec;
    %if %sysfunc(exist(MIG_AZEC.ref_mig_azec_vs_ims)) %then %do;
        proc export data=MIG_AZEC.ref_mig_azec_vs_ims
            outfile="&export_path./ref_mig_azec_vs_ims.csv"
            dbms=csv replace;
            delimiter=',';
        run;
        %put NOTE: ✓ ref_mig_azec_vs_ims.csv exporté;
    %end;
    %else %do;
        %put WARNING: Table MIG_AZEC.ref_mig_azec_vs_ims non trouvée;
        %put WARNING: Créez-la manuellement - voir PTF_MVTS_AZEC_MACRO.sas L94-106;
        
        /* Créer une table vide avec le bon schéma si besoin */
        data work.ref_mig_azec_vs_ims;
            length produit $5 migration_date 8 old_code $10 new_code $10;
            format migration_date date9.;
            stop;
        run;
        
        proc export data=work.ref_mig_azec_vs_ims
            outfile="&export_path./ref_mig_azec_vs_ims.csv"
            dbms=csv replace;
            delimiter=',';
        run;
        %put NOTE: ✓ ref_mig_azec_vs_ims.csv créé (VIDE - à remplir manuellement);
    %end;
%mend;

%export_mig_azec;


/* ============================================================================
 * FICHIER #4 : INDICES - FORMAT $INDICE (INDEXATION CAPITAUX)
 * ============================================================================ */

%put NOTE: Extraction Fichier #4 - indices ($INDICE format);

/* Exporter le format $INDICE depuis INDICES */
proc format lib=INDICES cntlout=work.indices_export;
run;

/* Filtrer uniquement le format INDICE */
data work.indices_clean;
    set work.indices_export;
    where upcase(fmtname) = 'INDICE' or upcase(fmtname) = '$INDICE';
run;

/* Vérifier si des données existent */
%macro export_indices;
    %let dsid = %sysfunc(open(work.indices_clean));
    %let nobs = %sysfunc(attrn(&dsid., nobs));
    %let rc = %sysfunc(close(&dsid.));
    
    %if &nobs. > 0 %then %do;
        proc export data=work.indices_clean
            outfile="&export_path./indices.csv"
            dbms=csv replace;
            delimiter=',';
        run;
        %put NOTE: ✓ indices.csv exporté (&nobs. enregistrements);
    %end;
    %else %do;
        %put WARNING: Format $INDICE non trouvé dans INDICES;
        %put WARNING: L'indexation des capitaux sera désactivée;
        
        /* Créer fichier vide avec header */
        data work.indices_empty;
            length fmtname $32 start $16 end $16 label $256;
            stop;
        run;
        
        proc export data=work.indices_empty
            outfile="&export_path./indices.csv"
            dbms=csv replace;
        run;
        %put NOTE: ✓ indices.csv créé (VIDE);
    %end;
%mend;

%export_indices;


/* ============================================================================
 * FICHIER #5 : W6.BASECLI_INV - BASE CLIENTS W6
 * ============================================================================ */

%put NOTE: Extraction Fichier #5 - basecli_inv;

/* Note: Fichier peut être compressé en .gz dans certains environnements */
/* Voir PTF_MVTS_CONSOLIDATION_MACRO.sas L532 */

%macro export_w6;
    %if %sysfunc(exist(W6.basecli_inv)) %then %do;
        proc export data=W6.basecli_inv
            outfile="&export_path./basecli_inv.csv"
            dbms=csv replace;
            delimiter='|';
        run;
        %put NOTE: ✓ basecli_inv.csv exporté;
    %end;
    %else %do;
        %put WARNING: W6.basecli_inv non trouvé;
        %put WARNING: Codification ISIC via W6 sera désactivée;
    %end;
%mend;

%export_w6;


/* ============================================================================
 * FICHIER #6 : BINSEE.HISTO_NOTE_RISQUE - NOTATION EULER HERMES
 * ============================================================================ */

%put NOTE: Extraction Fichier #6 - histo_note_risque;

%macro export_binsee;
    %if %sysfunc(exist(BINSEE.histo_note_risque)) %then %do;
        proc export data=BINSEE.histo_note_risque
            outfile="&export_path./histo_note_risque.csv"
            dbms=csv replace;
            delimiter='|';
        run;
        %put NOTE: ✓ histo_note_risque.csv exporté;
    %end;
    %else %do;
        %put WARNING: BINSEE.histo_note_risque non trouvé;
        %put WARNING: Notation risque Euler Hermes sera absente;
    %end;
%mend;

%export_binsee;


/* ============================================================================
 * FICHIER #7 : DEST.DO_DEST202110 - DESTINATION CHANTIER
 * ============================================================================ */

%put NOTE: Extraction Fichier #7 - do_dest202110;

%macro export_dest;
    %if %sysfunc(exist(DEST.do_dest202110)) %then %do;
        proc export data=DEST.do_dest202110
            outfile="&export_path./do_dest_202110.csv"
            dbms=csv replace;
            delimiter=',';
        run;
        %put NOTE: ✓ do_dest_202110.csv exporté;
    %end;
    %else %do;
        %put WARNING: DEST.do_dest202110 non trouvé;
        %put WARNING: Référentiel destination chantier manquera;
    %end;
%mend;

%export_dest;


/* ============================================================================
 * FICHIER #8 : REF.TABLE_SEGMENTATION_AZEC_MML - SEGMENTATION AZEC
 * ============================================================================ */

%put NOTE: Extraction Fichier #8 - table_segmentation_azec_mml;

%macro export_azec_seg;
    %if %sysfunc(exist(REF.table_segmentation_azec_mml)) %then %do;
        proc export data=REF.table_segmentation_azec_mml
            outfile="&export_path./table_segmentation_azec_mml.csv"
            dbms=csv replace;
            delimiter=',';
        run;
        %put NOTE: ✓ table_segmentation_azec_mml.csv exporté;
    %end;
    %else %do;
        %put WARNING: REF.table_segmentation_azec_mml non trouvé;
        %put WARNING: Segmentation AZEC utilisera typrd_2 comme fallback;
    %end;
%mend;

%export_azec_seg;


/* ============================================================================
 * RÉSUMÉ DE L'EXPORT
 * ============================================================================ */

%put ;
%put ========================================================================;
%put EXPORT TERMINÉ;
%put ========================================================================;
%put ;
%put Fichiers exportés dans: &export_path.;
%put ;
%put Liste des fichiers:;
%put   1. rf_fr1_prm_dtl_midcorp_m_&vision..csv  (One BI - MANUEL);
%put   2. segmentprdt_&vision..csv                (✓);
%put   3. ref_mig_azec_vs_ims.csv                 (✓ ou VIDE si absent);
%put   4. indices.csv                              (✓ ou VIDE si absent);
%put   5. basecli_inv.csv                          (✓ si disponible);
%put   6. histo_note_risque.csv                    (✓ si disponible);
%put   7. do_dest_202110.csv                       (✓ si disponible);
%put   8. table_segmentation_azec_mml.csv          (✓ si disponible);
%put ;

/* Instructions dépendant de l'emplacement */
%if %index(&export_path., :) > 0 or %index(&export_path., //) > 0 %then %do;
    %put Prochaines étapes (EXPORT LOCAL/RÉSEAU):;
    %put   1. Les fichiers sont DÉJÀ sur votre PC dans: &export_path.;
    %put   2. Récupérer rf_fr1_prm_dtl_midcorp_m via One BI (voir code commenté);
    %put   3. Copier tous les CSV vers: /workspace/datalake/bronze/;
    %put      - Fichier #1: bronze/2025/09/;
    %put      - Fichiers #2-8: bronze/ref/;
%end;
%else %do;
    %put Prochaines étapes (EXPORT SERVEUR SAS):;
    %put   1. TÉLÉCHARGER les fichiers depuis le serveur SAS:;
    %put      - Via FTP/SFTP depuis: &export_path.;
    %put      - Ou via interface web SAS (si disponible);
    %put      - Ou via commande scp: scp user@serveur:&export_path./*.csv ~/Downloads/;
    %put   2. Récupérer rf_fr1_prm_dtl_midcorp_m via One BI (voir code commenté);
    %put   3. Copier tous les CSV vers: /workspace/datalake/bronze/;
    %put      - Fichier #1: bronze/2025/09/;
    %put      - Fichiers #2-8: bronze/ref/;
%end;
%put ;
%put ========================================================================;
