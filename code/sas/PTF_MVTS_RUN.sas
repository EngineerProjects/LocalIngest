/******************************************************************************
 * FILE: PTF_MVTS_RUN.sas
 * 
 * Description: Main runner script for Portfolio Movements (PTF_MVTS) pipeline
 *              Sets up remote connection to STP3
 *              Allocates necessary libraries (PTF, CLIENT, REF, etc.)
 *              Calculates date variables
 *              Executes the main processing macros:
 *                - az_mvt_ptf
 *                - azec_mvt_ptf
 *                - consolidation_az_azec_mvt_ptf
 *
 * Parameters: None (uses &vision. macro variable)
 *
 * Dependencies:
 *   - generiques_v4.sas
 *   - CODIFICATION_ISIC_CONSTRUCTION.sas
 *   - REF_segmentation_azec.sas
 ******************************************************************************/

%macro run_ptf_mvts();

    /* ========================================================================
     * STEP 1: REMOTE CONNECTION SETUP
     * ======================================================================== */
    
    options comamid=tcp;
    %let serveur=STP3 7013;
    signon noscript remote=serveur user=%upcase(&sysuserid.) password="&MPWINDOWS";

    /* ========================================================================
     * STEP 2: LIBRARY ALLOCATION (STP3)
     * ======================================================================== */
    
    /* LIBRAIRIES STP3 */
    %let annee = %sysfunc(compress(%substr(&vision.,1,4)));
    %let mois = %sysfunc(compress(%substr(&vision.,5,2)));
    %put &annee &mois ;

    data _null_;
        ma_date=mdy(month("&sysdate9."d), 1, year("&sysdate9."d))-1;
        an_date=year(ma_date);
        mois_date=month(ma_date);
        AH0 =85+(&annee. -2011-1)*12 +&mois. ;
        /* Calcul de la génération de la table */
        call symput('AMN0',put(AH0,z3.));
        call symput('ASYS',an_date);
        call symput('MSYS',mois_date);
    run;

    %if &annee = &ASYS. and &mois.=&MSYS. %then %do;
        /* VISION EN COURS */
        LIBNAME PTF16 "INFP.IIA0P6$$.IPFE16" disp=shr server=serveur;
        LIBNAME PTF36 "INFP.IIA0P6$$.IPFE36" disp=shr server=serveur;
        LIBNAME PTF16a "INFP.IIA0P6$$.IPFE1SPE" disp=shr server=serveur; /** Pour extraire des données AGT liées aux produit 01099 */
        LIBNAME PTF36a "INFP.IIA0P6$$.IPF3SPE" disp=shr server=serveur; /** Pour extraire des données CRT liées aux produit 01099 */
    %end;
    %else %do;
        /* HISTORIQUE */
        LIBNAME PTF16 "INFH.IIMMP6$$.IPFE16.G0&AMN0.V00" disp=shr server=serveur;
        LIBNAME PTF36 "INFH.IIMMP6$$.IPFE36.G0&AMN0.V00" disp=shr server=serveur;
        LIBNAME PTF16a "INFP.IIA0P6$$.IPFE1SPE" disp=shr server=serveur; /** Pour extraire des données AGT liées aux produit 01099 */
        LIBNAME PTF36a "INFP.IIA0P6$$.IPF3SPE" disp=shr server=serveur; /** Pour extraire des données CRT liées aux produit 01099 */
    %end;

    proc contents data=PTF16.IPF; run;

    LIBNAME segmprdt "INFH.IDAAP6$$.SEGMPRDT(0)" DISP = SHR SERVER = serveur ;
    LIBNAME PRDCAP 'INFP.IIA0P6$$.PRDCAP' DISP = SHR SERVER = serveur ;
    LIBNAME CLIENT1 'infp.ima0p6$$.cliact14' DISP=SHR SERVER=SERVEUR;
    LIBNAME CLIENT3 'infp.ima0p6$$.cliact3' DISP=SHR SERVER=SERVEUR;
    /* FIN DES LIBRAIRIES STP3 */

    /* ========================================================================
     * STEP 3: LIBRARY ALLOCATION (SAS)
     * ======================================================================== */
    
    /* LIBRAIRIES SAS */
    LIBNAME Dest "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/Construction";
    LIBNAME Ref "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref";
    LIBNAME MIG_AZEC "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL";
    LIBNAME PT_GEST "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/points_gestion";
    LIBNAME AACPRTF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/lkhoumm/sasdata/BDC/AACPRTF";
    LIBNAME BINSEE "/sasprod/prod/prod/run/azi/d2d/w6/particuliers/dm_crm/partage/param/pro";
    LIBNAME W6 "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/ALEX_MIS/basecli_cumule";
    *LIBNAME RISK_REF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/202210";
    /* FIN DES LIBRAIRIES SAS */

    /* ========================================================================
     * STEP 4: LIBRARY ALLOCATION (DTM CONSTRUCTION)
     * ======================================================================== */
    
    /* LIBRAIRIES DTM CONSTRUCTION */
    x "mkdir /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    LIBNAME CUBE "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";

    /* ========================================================================
     * STEP 5: INCLUDE MACROS AND UTILITIES
     * ======================================================================== */
    
    /* MACROS-PGM UTILES */
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/legouil/fonctions/generiques_v4.sas";
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/CODIFICATION_ISIC_CONSTRUCTION.sas";
    OPTIONS sasautos = ("/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/_commun/Exploitation/sasmcr/sasalloc", "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/_commun/Exploitation/sasmcr/sastools", "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/aitihvi/saspgm/Cubes",SASAUTOS) mautosource mrecall;

    /* ========================================================================
     * STEP 6: CONDITIONAL ALLOCATIONS (SPE)
     * ======================================================================== */
    
    %if %substr(&vision.,1,4) < %sysfunc(year(%sysfunc(today()))) - 5 %then %do;
        %if &annee. <= 2014 %then %do;
            %put annee = 15 mois = 12 ;
            %alloc_spe_v3(2015,12,IPFSPE);
        %end;
        %else %do;
            %put annee = &annee. mois = 12 ;
            %alloc_spe_v3(&annee.,12,IPFSPE);
        %end;
    %end;

    %if %substr(&vision.,1,4) = %sysfunc(year(%sysfunc(today()))) - 5 %then %do;
        %if %substr(&vision.,5,2) < %sysfunc(month(%sysfunc(today()))) %then %do;
            %let annee_1 = %eval(&annee. - 1);
            %put &annee_1;
            %put annee = &annee_1 mois = 12 ;
            %alloc_spe_v3(&annee_1.,12,IPFSPE);
        %end;
        %else %do;
            %put annee = &annee. mois = &mois. ;
            %alloc_spe_v3(&annee.,&mois.,IPFSPE);
        %end;
    %end;

    %if %substr(&vision.,1,4) > %sysfunc(year(%sysfunc(today()))) - 5 %then %do;
        %put annee = &annee. mois = &mois. ;
        %alloc_spe_v3(&annee.,&mois.,IPFSPE);
    %end;

    /* ========================================================================
     * STEP 7: CONDITIONAL ALLOCATIONS (AZEC)
     * ======================================================================== */
    
    /* AZEC */
    %if &vision. <=202008 and &vision. >= 201211 %then %do;
        %alloc_azec_v3(&annee., &mois.);
    %end;

    /* ========================================================================
     * STEP 8: DATE CALCULATIONS
     * ======================================================================== */
    
    data _null_;
        format Date ddmmyy10. Dtrefn ddmmyy6. Dtrefn1 ddmmyy10.;
        Date = mdy(substr("&VISION.",5,2),10,substr("&VISION.",1,4));
        /*Date = intnx('month', today(), -1, 'e');*/
        Annee = put(year(date),z4.);
        Mois = put(month(date),z2.);
        Dtrefn=intnx('MONTH',mdy(Mois,1,Annee),1);
        Dtrefn=intnx('DAY',Dtrefn,-1);
        Dtrefn1=intnx('MONTH',mdy(Mois,1,Annee-1),1);
        A = substr(Annee,3,2);
        M = Mois;
        MA = cats(M,A);
        Vision = cats(Annee,Mois);
        call symput('DTFIN',put(dtrefn,date9.));
        call symput('dtfinmn',put(dtrefn,date9.));
        call symput('dtfinmn1',put(dtrefn1,date9.));
        call symput('finmoisn1',put(dtrefn1,date9.));
        call symput('DTOBS',trim(left(Annee))!!Mois);
        call symput('DTFIN_AN',put(mdy(1,1,Annee+1)-1,date9.));
        call symput('DTDEB_AN',put(mdy(1,1,Annee),date9.));
        call symput('dtdebn',put(mdy(1,1,Annee),date9.));
        call symput('DTDEB_MOIS_OBS',put(mdy(Mois,1,Annee),date9.));
        call symput('an',A);
        call symputx('FINMOIS', dtrefn);
        call symputx("mois_an",ma);
        call symput('DATEARRET', quote(put(date,date9.)) || 'd');
        call symput("ANNEE", Annee);
        call symput("MOIS", Mois);
        Call symput("Vision",compress(Vision));
        call symputx("date_run",date_run);
        call symputx("hour",hour);
    run;

    /* ========================================================================
     * STEP 9: EXECUTE MAIN MACROS
     * ======================================================================== */
    
    %az_mvt_ptf(&annee., &mois.);
    
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/REF_segmentation_azec.sas";
    
    %azec_mvt_ptf(&vision.);
    
    %consolidation_az_azec_mvt_ptf;

%mend;

/* ============================================================================
 * EXECUTION
 * ============================================================================ */

%run_ptf_mvts();
