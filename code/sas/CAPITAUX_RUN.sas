/******************************************************************************
 * MACRO: consolidation_az_azec_capitaux
 * 
 * Description: Consolidates capital data (CAPITAUX) from AZ and AZEC sources
 *              into a single table with proper formatting and field mapping
 *
 * Parameters:
 *   - &vision. : Vision parameter (macro variable)
 *
 * Output: 
 *   - CUBE.AZ_AZEC_CAPITAUX_&vision. table
 ******************************************************************************/

%macro consolidation_az_azec_capitaux;

    PROC SQL;
        CREATE TABLE CUBE.AZ_AZEC_CAPITAUX_&vision. AS 
        (
            /* AZ Data Selection */
            SELECT 
                "AZ" AS DIRCOM,
                NOPOL,
                CDPOLE,
                CDPROD,
                CMARCH,
                CSEG,
                CSSSEG,
                (PERTE_EXP_100_IND + RISQUE_DIRECT_100_IND) AS VALUE_INSURED_100_IND,
                PERTE_EXP_100_IND,
                RISQUE_DIRECT_100_IND,
                LIMITE_RC_100_PAR_SIN,
                LIMITE_RC_100_PAR_AN,
                LIMITE_RC_100,
                SMP_100_IND,
                LCI_100_IND,
                (PERTE_EXP_100 + RISQUE_DIRECT_100) AS VALUE_INSURED_100,
                PERTE_EXP_100,
                RISQUE_DIRECT_100,
                SMP_100,
                LCI_100
            FROM AZ_CAPITAUX_&vision.
        )
        OUTER UNION CORR
        (
            /* AZEC Data Selection */
            SELECT 
                "AZEC" AS DIRCOM,
                NOPOL,
                "3" AS CDPOLE,
                CDPROD,
                CMARCH,
                CSEG,
                CSSSEG,
                VALUE_INSURED_100_IND,
                PERTE_EXP_100_IND,
                RISQUE_DIRECT_100_IND,
                . AS LIMITE_RC_100_PAR_SIN,
                . AS LIMITE_RC_100_PAR_AN,
                . AS LIMITE_RC_100,
                SMP_100_IND,
                LCI_100_IND,
                . AS VALUE_INSURED_100,
                . AS PERTE_EXP_100,
                . AS RISQUE_DIRECT_100,
                . AS SMP_100,
                . AS LCI_100
            FROM AZEC_CAPITAUX_&vision.
        );
    QUIT;

%mend;


/******************************************************************************
 * MACRO: run_capitaux
 * 
 * Description: Main orchestration macro for capital data processing
 *              Sets up remote connection, library assignments, date variables,
 *              and executes the complete capital data pipeline
 *
 * Parameters: None (uses &vision. macro variable)
 *
 * Workflow:
 *   1. Establish remote connection to STP3 server
 *   2. Calculate date parameters and vision variables
 *   3. Assign libraries based on current/historical vision
 *   4. Execute indexation and create output directories
 *   5. Run AZ and AZEC capital processing macros
 *   6. Consolidate AZ and AZEC data
 ******************************************************************************/

%macro run_capitaux();

    /* ========================================================================
     * REMOTE SERVER CONNECTION
     * ======================================================================== */
    options comamid=tcp;
    %let serveur=STP3 7013;
    signon noscript remote=serveur user=%upcase(&sysuserid.) password="&MPWINDOWS";

    /* ========================================================================
     * DATE AND VISION CALCULATIONS
     * ======================================================================== */
    %let annee = %sysfunc(compress(%substr(&vision.,1,4)));
    %let mois = %sysfunc(compress(%substr(&vision.,5,2)));
    %put &annee &mois;

    data _null_;
        ma_date = mdy(month("&sysdate9."d), 1, year("&sysdate9."d)) - 1;
        an_date = year(ma_date);
        mois_date = month(ma_date);
        AH0 = 85 + (&annee. - 2011 - 1) * 12 + &mois.; /* Calcul de la génération de la table */
        call symput('AMN0', put(AH0, z3.));
        call symput('ASYS', an_date);
        call symput('MSYS', mois_date);
    run;

    /* ========================================================================
     * LIBRARY ASSIGNMENTS (CURRENT vs HISTORICAL)
     * ======================================================================== */
    %if &annee = &ASYS. and &mois. = &MSYS. %then %do;
        /* VISION EN COURS */
        LIBNAME PTF16 "INFP.IIA0P6$$.IPFE16" disp=shr server=serveur;
        LIBNAME PTF36 "INFP.IIA0P6$$.IPFE36" disp=shr server=serveur;
    %end;
    %else %do;
        /* HISTORIQUE */
        LIBNAME PTF16 "INFH.IIMMP6$$.IPFE16.G0&AMN0.V00" disp=shr server=serveur;
        LIBNAME PTF36 "INFH.IIMMP6$$.IPFE36.G0&AMN0.V00" disp=shr server=serveur;
    %end;

    LIBNAME INDICES 'infp.ima0p6$$.nautind3' DISP=SHR SERVER=SERVEUR;

    /* ========================================================================
     * DATE MACRO VARIABLES SETUP
     * ======================================================================== */
    data _null_;
        Date_run = cats(put(year(today()), z4.), put(month(today()), z2.), put(day(today()), z2.));
        Hour = cats(put(hour(time()), z2.), "H", put(minute(time()), z2.));
        
        format Date ddmmyy10. Dtrefn ddmmyy6. Dtrefn1 ddmmyy10.;
        Date = mdy(substr("&VISION.", 5, 2), 10, substr("&VISION.", 1, 4));
        /*Date = intnx('month', today(), -1, 'e');*/
        
        Annee = put(year(date), z4.);
        Mois = put(month(date), z2.);
        
        Dtrefn = intnx('MONTH', mdy(Mois, 1, Annee), 1);
        Dtrefn = intnx('DAY', Dtrefn, -1);
        Dtrefn1 = intnx('MONTH', mdy(Mois, 1, Annee - 1), 1);
        
        A = substr(Annee, 3, 2);
        M = Mois;
        MA = cats(M, A);
        Vision = cats(Annee, Mois);
        
        call symput('DTFIN', put(dtrefn, date9.));
        call symput('dtfinmn', put(dtrefn, date9.));
        call symput('dtfinmn1', put(dtrefn1, date9.));
        call symput('finmoisn1', put(dtrefn1, date9.));
        call symput('DTOBS', trim(left(Annee)) !! Mois);
        call symput('DTFIN_AN', put(mdy(1, 1, Annee + 1) - 1, date9.));
        call symput('DTDEB_AN', put(mdy(1, 1, Annee), date9.));
        call symput('dtdebn', put(mdy(1, 1, Annee), date9.));
        call symput('DTDEB_MOIS_OBS', put(mdy(Mois, 1, Annee), date9.));
        call symput('an', A);
        call symputx('FINMOIS', dtrefn);
        call symputx("mois_an", ma);
        call symput('DATEARRET', quote(put(date, date9.)) || 'd');
        call symput('date_time', compress(put(datetime(), datetime20.)));
        call symput("ANNEE", Annee);
        call symput("MOIS", Mois);
        Call symput("Vision", compress(Vision));
        call symputx("date_run", date_run);
        call symputx("hour", hour);
        call symput('date_time', compress(put(datetime(), datetime20.)));
    run;

    /* ========================================================================
     * INDEXATION AND OUTPUT DIRECTORY SETUP
     * ======================================================================== */
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/PRO_MIDCORP/saspgm/indexation_v2.sas";
    
    x "mkdir /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    
    LIBNAME CUBE "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    LIBNAME SEG "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/SEG_PRDTS";

    /* ========================================================================
     * CAPITAL DATA PROCESSING PIPELINE
     * ======================================================================== */
    
    /* Process AZ capital data */
    %az_capitaux(&annee., &mois.);
    
    /* Load AZEC segmentation reference */
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/REF_segmentation_azec.sas";
    
    /* Process AZEC capital data */
    %azec_capitaux(&annee., &mois.);
    
    /* Consolidate AZ and AZEC data */
    %consolidation_az_azec_capitaux;

%mend;

/* ============================================================================
 * MACRO EXECUTION
 * ============================================================================ */
%run_capitaux();
