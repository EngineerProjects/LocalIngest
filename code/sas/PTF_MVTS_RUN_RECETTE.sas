/******************************************************************************
 * FILE: PTF_MVTS_RUN_RECETTE.sas
 * 
 * Description: Quality control (recette) program for PTF_MVTS data
 *              Compares portfolio/movements cube data against finance reference
 *              to identify and calculate discrepancies
 *
 * Input Files:
 *   - TDB_PTF_MVTS_&vision..XLS : Finance reference data (Excel)
 *   - CUBE.MVT_PTF&vision.       : Cube portfolio/movements data
 *
 * Output:
 *   - RESUL.ecart_ptf&vision. : PTF discrepancies
 *   - RESUL.ecart_mvt&vision. : MVT discrepancies
 *
 * Macros:
 *   - traitement_ptf_mvts   : Import and process finance data
 *   - calcul_ecart_ptf_mvts : Calculate discrepancies
 *   - exec_recette_ptf_mvts : Main execution orchestrator
 ******************************************************************************/


/******************************************************************************
 * MACRO: traitement_ptf_mvts
 * 
 * Description: Imports finance PTF/MVT data from Excel and adapts structure
 *
 * Parameters:
 *   - cdpole : Distribution channel code (1=Agent, 3=Courtage)
 *   - annee  : Year
 *   - mois   : Month
 *   - sheet  : Excel sheet name
 ******************************************************************************/

%MACRO traitement_ptf_mvts(cdpole, annee, mois, sheet);

    /* ========================================================================
     * IMPORT FINANCE DATA FROM EXCEL
     * ======================================================================== */
    
    proc import datafile = "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/TDB_PTF_MVTS_&vision..XLS"
        out = mvt_&cdpole._&annee.&mois.
        dbms = XLS replace;
        sheet = "&sheet.";
    run;

    /* ========================================================================
     * CALCULATE DERIVED FIELDS
     * ======================================================================== */
    
    data mvt2_&cdpole.;
        set mvt_&cdpole._&annee.&mois.;
        Prime_resil = Nbre_resil * "Prime moyenne sorties N"n;
        Prime_AFN = Nbre_AFN * "Prime moyenne entrées N"n;
        Pri_moy_Ptf_act = "Prime moyenne clôture N"n;
        Pri_moy_an_act = "Prime moyenne entrées N"n;
        Pri_moy_res_act = "Prime moyenne sorties N"n;
    run;

    /* ========================================================================
     * ADAPT STRUCTURE TO CUBE FORMAT
     * ======================================================================== */
    
    data mvt_&cdpole._fin;
        set mvt2_&cdpole.(keep = Cdpole Lob Nbre_pol Nbre_AFN Nbre_resil prime_cloture 
                                  Prime_AFN prime_resil Pri_moy_Ptf_act Pri_moy_an_act Pri_moy_res_act);
        if LoB in ("TOTAL CONSTRUCTION") then
            do;
                cmarch='6';
                cseg='2';
            end;
    run;

%MEND;


/******************************************************************************
 * MACRO: calcul_ecart_ptf_mvts
 * 
 * Description: Calculates discrepancies between cube and finance data
 *
 * Parameters:
 *   - annee : Year
 *   - mois  : Month
 ******************************************************************************/

%MACRO calcul_ecart_ptf_mvts(annee, mois);
    %local a; 
    %let a = %nrstr(%mend);

    /* ========================================================================
     * STEP 1: CONSOLIDATE FINANCE DATA
     * ======================================================================== */
    
    PROC SQL;
        CREATE TABLE table_finale AS
        SELECT * FROM mvt_1_fin
        OUTER UNION CORR
        SELECT * FROM mvt_3_fin;
    QUIT;

    proc sort data=table_finale;
        by cdpole cmarch cseg;
    run;

    /* ========================================================================
     * STEP 2: AGGREGATE CUBE MOVEMENTS DATA
     * ======================================================================== */
    
    PROC SQL;
        CREATE TABLE cube&annee.&mois. AS
        SELECT 
            CDPOLE, CMARCH, CSEG,
            SUM(NBAFN) AS SUM_of_NBAFN,
            SUM(NBRES) AS SUM_of_NBRES,
            SUM(PRIMES_AFN) AS SUM_of_Primes_AFN,
            SUM(PRIMES_RES) AS SUM_of_Primes_RES
        FROM cube.mvt_ptf&vision.
        GROUP BY CDPOLE, CMARCH, CSEG;
    QUIT;

    proc sort data=cube&annee.&mois.;
        by cdpole cmarch cseg;
    run;

    /* ========================================================================
     * STEP 3: MERGE FINANCE AND CUBE DATA
     * ======================================================================== */
    
    data base_finale;
        merge table_finale (in=a) cube&annee.&mois.;
        by cdpole cmarch cseg;
        if a;
    run;

    /* ========================================================================
     * STEP 4: ADD PORTFOLIO DATA
     * ======================================================================== */
    
    PROC SQL;
        CREATE TABLE ptf&annee.&mois. AS
        SELECT 
            t1.vision, t1.CDPOLE, t1.CMARCH, t1.CSEG,
            SUM(t1.NBPTF) AS SUM_of_NBPTF,
            SUM(t1.Primes_PTF) AS SUM_of_Primes_PTF
        FROM cube.mvt_ptf&vision. t1
        GROUP BY t1.vision, t1.CDPOLE, t1.CMARCH, t1.CSEG;
    QUIT;

    proc sort data=ptf&annee.&mois.;
        by vision cdpole cmarch cseg;
    run;

    data base_finale;
        merge base_finale (in=a) ptf&annee.&mois.;
        by vision cdpole cmarch cseg;
        if a;
    run;

    /* ========================================================================
     * STEP 5: CALCULATE DISCREPANCIES
     * ======================================================================== */
    
    data ecart_mvt_ptf&annee.&mois.;
        set base_finale;
        format Pri_moy_Ptf_dt 10.2;
        vision=&vision.;

        if cmarch = '6' and cseg='2' then libelle_segment='CONSTRUCTION';

        /* Portfolio average premium */
        Pri_moy_Ptf_dt = SUM_of_Primes_PTF / SUM_of_NBPTF;

        /* New business average premium */
        Pri_moy_an_dt = SUM_of_Primes_AFN / SUM_of_NBAFN;

        /* Termination average premium */
        Pri_moy_res_dt = SUM_of_Primes_RES / SUM_of_NBRES;

        /* Calculate discrepancies */
        ecart_nbptf = SUM_of_NBPTF - Nbre_pol;
        ecart_nbafn = SUM_of_NBAFN - Nbre_afn;
        ecart_nbres = SUM_of_NBRES - Nbre_resil;
        ecart_PM_PTF = ROUND(Pri_moy_Ptf_dt - Pri_moy_Ptf_act, 1);
        ecart_PM_AN = round(Pri_moy_an_dt - Pri_moy_an_act, 1);
        ecart_PM_RES = round(Pri_moy_res_dt - Pri_moy_res_act, 1);
        ecart_Primes_Ptf = ROUND(SUM_of_Primes_PTF - prime_cloture, 1);
        ecart_Primeafn = round(SUM_of_Primes_AFN - prime_afn, 1);
        ecart_Primeres = round(SUM_of_Primes_RES - prime_resil, 1);
    run;

    /* ========================================================================
     * STEP 6: SAVE RESULTS
     * ======================================================================== */
    
    proc sql;
        create table RESUL.ecart_ptf&annee.&mois. as
        select 
            vision, cdpole, cmarch, cseg, libelle_segment, 
            SUM_of_NBPTF, Nbre_pol, ecart_nbptf, Pri_moy_Ptf_dt, Pri_moy_Ptf_act, ecart_PM_PTF,
            SUM_of_Primes_PTF, prime_cloture, ecart_Primes_Ptf
        from ecart_mvt_ptf&annee.&mois.;
    quit;

    proc sql;
        create table RESUL.ecart_mvt&annee.&mois. as
        select 
            vision, cdpole, cmarch, cseg, libelle_segment,
            SUM_of_NBAFN, Nbre_afn, SUM_of_NBRES, Nbre_resil, Pri_moy_an_dt, Pri_moy_res_dt,
            Pri_moy_an_act, Pri_moy_res_act, SUM_of_Primes_AFN, prime_afn, SUM_of_Primes_RES, prime_resil,
            ecart_nbafn, ecart_nbres, ecart_PM_AN, ecart_PM_RES, ecart_Primeafn, ecart_Primeres
        from ecart_mvt_ptf&annee.&mois.;
    quit;

%MEND;


/* ============================================================================
 * DATE VARIABLES SETUP
 * ============================================================================ */

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


/* ============================================================================
 * LIBRARY ASSIGNMENTS
 * ============================================================================ */

LIBNAME CUBE "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
LIBNAME RESUL "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Resultat";


/******************************************************************************
 * MACRO: exec_recette_ptf_mvts
 * 
 * Description: Main orchestration - executes quality control if input exists
 ******************************************************************************/

%macro exec_recette_ptf_mvts;

    %if %sysfunc(fileexist("/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/TDB_PTF_MVTS_&vision..XLS")) = 1
    %then %do;
        %traitement_ptf_mvts(cdpole=1, annee=&annee., mois=&mois., sheet=Agent);
        %traitement_ptf_mvts(cdpole=3, annee=&annee., mois=&mois., sheet=Courtage);
        %calcul_ecart_ptf_mvts(&annee., &mois.);

        PROC PRINT data=RESUL.ecart_ptf&annee.&mois.; 
        run;
        PROC PRINT data=RESUL.ecart_mvt&annee.&mois.; 
        run;

    %end;

%mend;


/* ============================================================================
 * EXECUTION
 * ============================================================================ */

%exec_recette_ptf_mvts;
