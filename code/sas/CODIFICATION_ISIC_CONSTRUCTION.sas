/******************************************************************************
 * MACRO: code_isic_construction
 * 
 * DESCRIPTION:
 *   Ajoute ou modifie 3 colonnes d'une table table_source avec les valeurs 
 *   de code ISIC, Hazard Grade sur les segments construction uniquement
 * 
 * PARAMETERS:
 *   - table_source: Table source à modifier
 *   - vision: Version/vision des données
 *
 ******************************************************************************/

%macro code_isic_construction(table_source, vision);

    /**************************************************************************
     * SECTION 1: BIBLIOTHEQUES ET INITIALISATIONS
     **************************************************************************/
    
    /* Définition des bibliothèques */
    LIBNAME REF_ISIC '/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/isic/';
    LIBNAME NAF_2008 '/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/Ecran_SUI';
    LIBNAME ISIC_CST '/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/mapping_isic';
    
    /* Variables locales et listes */
    %let liste_champs = CMARCH CSEG CSSSEG CDPROD ACTPRIN;
    %local a;
    %let a = %nrstr(%mend);


    /**************************************************************************
     * SECTION 2: VERIFICATION DE L'EXISTENCE DE LA TABLE
     **************************************************************************/
    
    %if (%sysfunc(exist(&table_source.)) = 1) %then %do;
        
        %let continue = 1;
        %let champs_table = %lister_champs_table(&table_source.);
        
        /* Vérification de la présence des champs requis */
        %do j_seg = 1 %to %sysfunc(countw(&liste_champs., %str( )));
            %if %in_array(%scan(&liste_champs.,&j_seg., %str( )), &champs_table.) = 0 %then %do;
                %gerer_erreur(le champs %scan(&liste_champs.,&j_seg., %str( )) n_a pas été trouvé dans la table &table_source.);
                %let continue = 0;
            %end;
        %end;
        
        
        /**************************************************************************
         * SECTION 3: TRAITEMENT PRINCIPAL
         **************************************************************************/
        
        %if &continue. = 1 %then %do;
            
            /*======================================================================
             * 3.1 - RECUPERATION DU NAF 2008 (SUIVI DES ENGAGEMENTS)
             *======================================================================*/
            
            %if %eval(&vision.) >= 202103 %then %do;
                
                /* Décompression et lecture des données NAF 2008 */
                %let vue = /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/Ecran_SUI;
                x "gunzip &vue./ird_suivi_engagements_&vision..sas7bdat.gz";
                
                data TABLE_SUI_NAF_2008;
                    set NAF_2008.IRD_SUIVI_ENGAGEMENTS_&vision.(encoding='any');
                run;
                
                x "gzip &vue./*.sas7bdat";
                
                /* Jointure avec la table source */
                PROC SQL;
                    CREATE TABLE &table_source._2 AS
                    SELECT t1.*, 
                           t2.CDNAF08 AS CDNAF2008_TEMP, 
                           t2.CDISIC AS ISIC_CODE_SUI_TEMP
                    FROM &table_source. t1
                    LEFT JOIN TABLE_SUI_NAF_2008 t2 
                        ON (t1.NOPOL = t2.NOPOL AND t1.CDPROD = t2.CDPROD);
                QUIT;
                
            %end;
            %else %do;
                
                /* Pour les versions antérieures à 202103 */
                PROC SQL;
                    CREATE TABLE &table_source._2 AS
                    SELECT t1.*, 
                           " " AS CDNAF2008_TEMP, 
                           " " AS ISIC_CODE_SUI_TEMP
                    FROM &table_source. t1;
                QUIT;
                
            %end;
            
            
            /*======================================================================
             * 3.2 - RECUPERATION DES REFERENCES ISIC POUR CONSTRUCTION
             *======================================================================*/
            
            /* Récupération des tables de mapping ISIC pour construction */
            %derniere_version(ISIC_CST, MAPPING_ISIC_CONST_ACT_, &vision., TABLE_ISIC_CST_ACT);
            %derniere_version(ISIC_CST, MAPPING_ISIC_CONST_CHT_, &vision., TABLE_ISIC_CST_CHT);
            
            /* Récupération des tables de référence NAF/ISIC */
            %derniere_version(REF_ISIC, MAPPING_CDNAF2003_ISIC_, &vision., TABLE_REF_ISIC_03);
            %derniere_version(REF_ISIC, MAPPING_CDNAF2008_ISIC_, &vision., TABLE_REF_ISIC_08);
            
            
            /*======================================================================
             * 3.3 - AFFECTATION DES CODES ISIC (ORDRE DE PRIORITE)
             *      1) NAF08-PTF  2) NAF03-PTF  3) NAF03-CLI  4) NAF08-CLI
             *======================================================================*/
            
            PROC SQL;
                CREATE TABLE &table_source._2 AS
                SELECT t1.*,
                       /* Logique en cascade pour déterminer le code ISIC */
                       CASE 
                           WHEN t3.ISIC_CODE ne "" THEN t3.ISIC_CODE 
                           ELSE (CASE 
                               WHEN t2.ISIC_CODE ne "" THEN t2.ISIC_CODE 
                               ELSE (CASE 
                                   WHEN t4.ISIC_CODE ne "" THEN t4.ISIC_CODE 
                                   ELSE (CASE 
                                       WHEN t5.ISIC_CODE ne "" THEN t5.ISIC_CODE 
                                       ELSE "" 
                                   END) 
                               END) 
                           END) 
                       END AS ISIC_CODE_TEMP,
                       /* Origine du code ISIC */
                       CASE 
                           WHEN t3.ISIC_CODE ne "" THEN "NATIF" 
                           ELSE (CASE 
                               WHEN t2.ISIC_CODE ne "" THEN "NAF03" 
                               ELSE (CASE 
                                   WHEN t4.ISIC_CODE ne "" THEN "CLI03" 
                                   ELSE (CASE 
                                       WHEN t5.ISIC_CODE ne "" THEN "CLI08" 
                                       ELSE "" 
                                   END) 
                               END) 
                           END) 
                       END AS ORIGINE_ISIC_TEMP
                FROM &table_source._2 t1
                LEFT JOIN TABLE_REF_ISIC_03 t2 
                    ON (t2.CDNAF_2003 = t1.CDNAF AND t1.CMARCH="6")
                LEFT JOIN TABLE_REF_ISIC_08 t3 
                    ON (t3.CDNAF_2008 = t1.CDNAF2008_TEMP AND t1.CMARCH="6")
                LEFT JOIN TABLE_REF_ISIC_03 t4 
                    ON (t4.CDNAF_2003 = t1.CDNAF03_CLI AND t1.CMARCH="6")
                LEFT JOIN TABLE_REF_ISIC_08 t5 
                    ON (t5.CDNAF_2008 = t1.CDNAF08_W6 AND t1.CMARCH="6");
            QUIT;
            
            
            /*======================================================================
             * 3.4 - AFFECTATION ACTIVITE ISIC (CONTRATS A ACTIVITE)
             *======================================================================*/
            
            PROC SQL;
                CREATE TABLE &table_source._2 AS
                SELECT t1.*, 
                       t2.CDNAF08 AS CDNAF08_CONST_R, 
                       t2.CDTRE AS CDTRE_CONST_R, 
                       t2.CDNAF03 AS CDNAF03_CONST_R, 
                       t2.CDISIC AS CDISIC_CONST_R
                FROM &table_source._2 t1
                LEFT JOIN TABLE_ISIC_CST_ACT t2 
                    ON (t1.ACTPRIN = t2.ACTPRIN AND t1.CMARCH="6" AND t1.CDNATP="R");
            QUIT;
            
            
            /*======================================================================
             * 3.5 - AFFECTATION DESTINATION ISIC (CONTRATS CHANTIER)
             *======================================================================*/
            
            data &table_source._2;
                set &table_source._2;
                
                /* Initialisation pour les contrats chantier */
                if CMARCH="6" and CDNATP="C" then do;
                    DESTI_ISIC=" ";
                    
                    /* Exclusion du produit 01059 */
                    if CDNATP="C" and CDPROD not in ("01059") then do;
                        DESTI_ISIC=" ";
                        
                        /* Analyse par mots-clés de la destination (DSTCSC) */
                        if PRXMATCH("/(COLL)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(MAISON)/", UPCASE(DSTCSC)) then DESTI_ISIC="MAISON";
                        else if PRXMATCH("/(LOGE)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(APPA)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(HABIT)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(INDUS)/", UPCASE(DSTCSC)) then DESTI_ISIC="INDUSTRIE";
                        else if PRXMATCH("/(BUREAU)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(STOCK)/", UPCASE(DSTCSC)) then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH("/(SUPPORT)/", UPCASE(DSTCSC)) then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH("/(COMMER)/", UPCASE(DSTCSC)) then DESTI_ISIC="COMMERCE";
                        else if PRXMATCH("/(GARAGE)/", UPCASE(DSTCSC)) then DESTI_ISIC="INDUSTRIE";
                        else if PRXMATCH("/(TELECOM)/", UPCASE(DSTCSC)) then DESTI_ISIC="INDUSTRIE";
                        else if PRXMATCH("/(R+)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(HOTEL)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(TOURIS)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(VAC)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(LOIS)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(AGRIC)/", UPCASE(DSTCSC)) then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH("/(CLINI )/", UPCASE(DSTCSC)) then DESTI_ISIC="HOPITAL";
                        else if PRXMATCH("/(HOP)/", UPCASE(DSTCSC)) then DESTI_ISIC="HOPITAL";
                        else if PRXMATCH("/(HOSP)/", UPCASE(DSTCSC)) then DESTI_ISIC="HOPITAL";
                        else if PRXMATCH("/(RESID)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(CIAL)/", UPCASE(DSTCSC)) then DESTI_ISIC="COMMERCE";
                        else if PRXMATCH("/(SPOR)/", UPCASE(DSTCSC)) then DESTI_ISIC="COMMERCE";
                        else if PRXMATCH("/(ECOL)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(ENSEI)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(CHIR)/", UPCASE(DSTCSC)) then DESTI_ISIC="HOPITAL";
                        else if PRXMATCH("/(BAT)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(INDIV)/", UPCASE(DSTCSC)) then DESTI_ISIC="MAISON";
                        else if PRXMATCH("/(VRD)/", UPCASE(DSTCSC)) then DESTI_ISIC="VOIRIE";
                        else if PRXMATCH("/(NON SOUMIS)/", UPCASE(DSTCSC)) then DESTI_ISIC="AUTRES_GC";
                        else if PRXMATCH("/(SOUMIS)/", UPCASE(DSTCSC)) then DESTI_ISIC="AUTRES_BAT";
                        else if PRXMATCH("/(PHOTOV)/", UPCASE(DSTCSC)) then DESTI_ISIC="PHOTOV";
                        else if PRXMATCH("/(PARK)/", UPCASE(DSTCSC)) then DESTI_ISIC="VOIRIE";
                        else if PRXMATCH("/(STATIONNEMENT)/", UPCASE(DSTCSC)) then DESTI_ISIC="VOIRIE";
                        else if PRXMATCH("/(MANEGE)/", UPCASE(DSTCSC)) then DESTI_ISIC="NON_RESIDENTIEL";
                        else if PRXMATCH("/(MED)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(BANC)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(BANQ)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(AGENCE)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(CRECHE)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(EHPAD)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(ENTREPOT)/", UPCASE(DSTCSC)) then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH("/(HANGAR)/", UPCASE(DSTCSC)) then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH("/(AQUAT)/", UPCASE(DSTCSC)) then DESTI_ISIC="COMMERCE";
                        else if PRXMATCH("/(LGTS)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(LOGTS)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(LOGS)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH("/(RESTAURA)/", UPCASE(DSTCSC)) then DESTI_ISIC="BUREAU";
                        else if PRXMATCH("/(HAB)/", UPCASE(DSTCSC)) then DESTI_ISIC="RESIDENTIEL";
                        
                        /* Analyse par codes de destination */
                        else if DSTCSC in ("01" "02" "03" "03+22" "04" "06" "08" "1" "2" "3" "4" "6" "8") 
                            then DESTI_ISIC="RESIDENTIEL";
                        else if DSTCSC in ("22") 
                            then DESTI_ISIC="MAISON";
                        else if DSTCSC in ("27" "99") 
                            then DESTI_ISIC="AUTRES_GC";
                        else if DSTCSC in ("05" "07" "09" "10" "13" "14" "16" "5" "7" "9") 
                            then DESTI_ISIC="BUREAU";
                        else if DSTCSC in ("17" "23") 
                            then DESTI_ISIC="COMMERCE";
                        else if DSTCSC in ("15") 
                            then DESTI_ISIC="HOPITAL";
                        else if DSTCSC in ("25") 
                            then DESTI_ISIC="STADE";
                        else if DSTCSC in ("12" "18" "19") 
                            then DESTI_ISIC="INDUSTRIE";
                        else if DSTCSC in ("11" "20" "21") 
                            then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if DSTCSC in ("24" "26" "28") 
                            then DESTI_ISIC="VOIRIE";
                        
                        /* Destination par défaut si clé vide et pas de code ISIC client */
                        if ISIC_CODE_TEMP = "" and DESTI_ISIC = "" then 
                            DESTI_ISIC="AUTRES_BAT";
                    end;
                    
                    /* Traitement spécifique pour le produit 01059 (VENTE) */
                    if CDPROD = "01059" then 
                        DESTI_ISIC="VENTE"; /* code ISIC 410004 */
                    
                    /* Produits 00548 ou 01071: conservation du code ISIC natif ou client */
                    if CDPROD in ("00548" "01071") and ISIC_CODE_TEMP ne "" then 
                        DESTI_ISIC="";
                end;
            run;
            
            
            /*======================================================================
             * 3.6 - JOINTURE AVEC MAPPING CHANTIER
             *======================================================================*/
            
            PROC SQL;
                CREATE TABLE &table_source._2 AS
                SELECT t1.*, 
                       t2.CDNAF08 AS CDNAF08_CONST_C, 
                       t2.CDTRE AS CDTRE_CONST_C, 
                       t2.CDNAF03 AS CDNAF03_CONST_C, 
                       t2.CDISIC AS CDISIC_CONST_C
                FROM &table_source._2 t1
                LEFT JOIN TABLE_ISIC_CST_CHT t2 
                    ON (t1.DESTI_ISIC = t2.DESTI_ISIC AND t1.CMARCH="6" AND t1.CDNATP="C");
            QUIT;
            
        %end; /* Fin du if &continue. = 1 */
        
    %end; /* Fin du if exist(&table_source.) */
    %else %do;
        
        /*======================================================================
         * CAS OU LA TABLE N'EXISTE PAS: CREATION AVEC COLONNES VIDES
         *======================================================================*/
        
        PROC SQL;
            CREATE TABLE &table_source._2 AS
            SELECT t1.*, 
                   " " AS CDNAF2008_TEMP, 
                   " " AS ISIC_CODE_SUI_TEMP, 
                   " " AS ISIC_CODE_TEMP, 
                   " " AS ORIGINE_ISIC_TEMP, 
                   " " AS CDNAF08_CONST_R, 
                   " " AS CDTRE_CONST_R, 
                   " " AS CDNAF03_CONST_R, 
                   " " AS CDISIC_CONST_R, 
                   " " AS CDNAF08_CONST_C, 
                   " " AS CDTRE_CONST_C, 
                   " " AS CDNAF03_CONST_C, 
                   " " AS CDISIC_CONST_C
            FROM &table_source. t1;
        QUIT;
        
    %end;
    
    
    /**************************************************************************
     * SECTION 4: FINALISATION DES CODES ISIC CONSTRUCTION
     **************************************************************************/
    
    data &table_source._2;
        set &table_source._2;
        
        /* Affectation finale pour contrats à activité (CDNATP="R") */
        if CMARCH="6" and CDNATP="R" and CDISIC_CONST_R ne "" then do;
            CDNAF2008_TEMP = CDNAF08_CONST_R;
            CDNAF = CDNAF03_CONST_R;
            CDTRE = CDTRE_CONST_R;
            DESTI_ISIC_TEMP = " ";
            ISIC_CODE_TEMP = CDISIC_CONST_R;
            ORIGINE_ISIC_TEMP = "NCLAT";
        end;
        
        /* Affectation finale pour contrats chantier (CDNATP="C") */
        if CMARCH="6" and CDNATP="C" and CDISIC_CONST_C ne "" then do;
            CDNAF2008_TEMP = CDNAF08_CONST_C;
            CDNAF = CDNAF03_CONST_C;
            CDTRE = CDTRE_CONST_C;
            DESTI_ISIC_TEMP = DESTI_ISIC;
            ISIC_CODE_TEMP = CDISIC_CONST_C;
            ORIGINE_ISIC_TEMP = "DESTI";
        end;
    run;
    
    
    /**************************************************************************
     * SECTION 5: RECUPERATION DES HAZARD GRADES
     **************************************************************************/
    
    /* Récupération de la table de référence ISIC avec Hazard Grades */
    %if %eval(&vision.) >= 202305 %then %do;
        %derniere_version(REF_ISIC, table_isic_tre_naf_, &vision., TABLE_REF_ISIC);
    %end;
    %else %do;
        data TABLE_REF_ISIC;
            set REF_ISIC.table_isic_tre_naf_202305;
        run;
    %end;
    
    /* Jointure pour récupérer les Hazard Grades */
    PROC SQL;
        CREATE TABLE &table_source._2 AS
        SELECT t1.*, 
               t2.HAZARD_GRADES_FIRE as HAZARD_GRADES_FIRE_TEMP, 
               t2.HAZARD_GRADES_BI as HAZARD_GRADES_BI_TEMP, 
               t2.HAZARD_GRADES_RCA as HAZARD_GRADES_RCA_TEMP, 
               t2.HAZARD_GRADES_RCE as HAZARD_GRADES_RCE_TEMP, 
               t2.HAZARD_GRADES_TRC as HAZARD_GRADES_TRC_TEMP, 
               t2.HAZARD_GRADES_RCD as HAZARD_GRADES_RCD_TEMP, 
               t2.HAZARD_GRADES_DO as HAZARD_GRADES_DO_TEMP
        FROM &table_source._2 t1
        LEFT JOIN TABLE_REF_ISIC t2 
            ON (t2.ISIC_CODE = t1.ISIC_CODE_TEMP);
    QUIT;
    
    
    /**************************************************************************
     * SECTION 6: CREATION OU MISE A JOUR DES COLONNES FINALES
     **************************************************************************/
    
    %if %in_array(ISIC_CODE, &champs_table.) = 0 %then %do;
        
        /*======================================================================
         * CAS 1: AJOUT DES COLONNES (COLONNES N'EXISTENT PAS)
         *======================================================================*/
        
        PROC SQL;
            CREATE TABLE &table_source._2 AS
            SELECT *, 
                   CDNAF2008_TEMP AS CDNAF2008,
                   ISIC_CODE_SUI_TEMP as ISIC_CODE_SUI,
                   DESTI_ISIC_TEMP AS DESTINAT_ISIC,
                   ISIC_CODE_TEMP AS ISIC_CODE,
                   ORIGINE_ISIC_TEMP AS ORIGINE_ISIC,
                   HAZARD_GRADES_FIRE_TEMP AS HAZARD_GRADES_FIRE,
                   HAZARD_GRADES_BI_TEMP AS HAZARD_GRADES_BI,
                   HAZARD_GRADES_RCA_TEMP AS HAZARD_GRADES_RCA,
                   HAZARD_GRADES_RCE_TEMP AS HAZARD_GRADES_RCE,
                   HAZARD_GRADES_TRC_TEMP AS HAZARD_GRADES_TRC,
                   HAZARD_GRADES_RCD_TEMP AS HAZARD_GRADES_RCD,
                   HAZARD_GRADES_DO_TEMP AS HAZARD_GRADES_DO
            FROM &table_source._2;
            
            ALTER TABLE &table_source._2
            DROP COLUMN CDNAF08_CONST_R, 
                        CDNAF03_CONST_R, 
                        CDTRE_CONST_R, 
                        CDISIC_CONST_R, 
                        CDNAF08_CONST_C, 
                        CDNAF03_CONST_C, 
                        CDTRE_CONST_C, 
                        CDISIC_CONST_C, 
                        CDNAF2008_TEMP, 
                        CDTRE_TEMP, 
                        ISIC_CODE_SUI_TEMP, 
                        ISIC_CODE_TEMP, 
                        ORIGINE_ISIC_TEMP, 
                        HAZARD_GRADES_FIRE_TEMP, 
                        HAZARD_GRADES_BI_TEMP, 
                        HAZARD_GRADES_RCA_TEMP, 
                        HAZARD_GRADES_RCE_TEMP, 
                        HAZARD_GRADES_TRC_TEMP, 
                        HAZARD_GRADES_RCD_TEMP, 
                        HAZARD_GRADES_DO_TEMP, 
                        DESTI_ISIC, 
                        DESTI_ISIC_TEMP;
        QUIT;
        
    %end;
    %else %do;
        
        /*======================================================================
         * CAS 2: MISE A JOUR DES COLONNES (COLONNES EXISTENT DEJA)
         *======================================================================*/
        
        PROC SQL;
            UPDATE &table_source._2
            SET CDNAF2008 = CDNAF2008_TEMP,
                ISIC_CODE_SUI = ISIC_CODE_SUI_TEMP,
                DESTINAT_ISIC = DESTI_ISIC_TEMP,
                ISIC_CODE = ISIC_CODE_TEMP,
                ORIGINE_ISIC = ORIGINE_ISIC_TEMP,
                HAZARD_GRADES_FIRE = HAZARD_GRADES_FIRE_TEMP,
                HAZARD_GRADES_BI = HAZARD_GRADES_BI_TEMP,
                HAZARD_GRADES_RCA = HAZARD_GRADES_RCA_TEMP,
                HAZARD_GRADES_RCE = HAZARD_GRADES_RCE_TEMP,
                HAZARD_GRADES_TRC = HAZARD_GRADES_TRC_TEMP,
                HAZARD_GRADES_RCD = HAZARD_GRADES_RCD_TEMP,
                HAZARD_GRADES_DO = HAZARD_GRADES_DO_TEMP;
            
            ALTER TABLE &table_source._2
            DROP COLUMN CDNAF08_CONST_R, 
                        CDNAF03_CONST_R, 
                        CDTRE_CONST_R, 
                        CDISIC_CONST_R, 
                        CDNAF08_CONST_C, 
                        CDNAF03_CONST_C, 
                        CDTRE_CONST_C, 
                        CDISIC_CONST_C, 
                        CDNAF2008_TEMP, 
                        CDTRE_TEMP, 
                        ISIC_CODE_SUI_TEMP, 
                        ISIC_CODE_TEMP, 
                        ORIGINE_ISIC_TEMP, 
                        HAZARD_GRADES_FIRE_TEMP, 
                        HAZARD_GRADES_BI_TEMP, 
                        HAZARD_GRADES_RCA_TEMP, 
                        HAZARD_GRADES_RCE_TEMP, 
                        HAZARD_GRADES_TRC_TEMP, 
                        HAZARD_GRADES_RCD_TEMP, 
                        HAZARD_GRADES_DO_TEMP, 
                        DESTI_ISIC, 
                        DESTI_ISIC_TEMP;
        QUIT;
        
    %end;
    
    
    /**************************************************************************
     * SECTION 7: FINALISATION - REMPLACEMENT DE LA TABLE SOURCE
     **************************************************************************/
    
    PROC SQL;
        CREATE TABLE &table_source. AS
        SELECT * FROM &table_source._2;
    QUIT;
    
    /* Suppression des tables temporaires */
    %drop_table(&table_source._2);
    
    
    /**************************************************************************
     * SECTION 8: GESTION DES ERREURS
     **************************************************************************/
    
    %if %sysfunc(exist(&table_source.)) = 0 %then %do;
        %gerer_erreur(La table &table_source. na pas été trouvée);
    %end;

%mend code_isic_construction;
