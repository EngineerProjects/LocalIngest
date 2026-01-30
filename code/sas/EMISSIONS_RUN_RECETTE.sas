/******************************************************************************
 * FILE: EMISSIONS_RUN_RECETTE.sas
 * 
 * Description: Quality control (recette) program for emissions data
 *              Compares cube emissions data against finance reference data
 *              to identify and calculate discrepancies
 *
 * Input Files:
 *   - TDB_PRIMES_&vision..XLS : Finance reference data (Excel format)
 *   - CUBE.PRIMES_EMISES&vision._POL_GARP : Cube emissions data
 *
 * Output:
 *   - RESUL.ecart_Emissions&vision. : Discrepancies between cube and finance
 *
 * Macros:
 *   - traitement_primes    : Import and process finance data
 *   - calcul_ecart_primes  : Calculate discrepancies
 *   - recette_emissions    : Main orchestration macro
 ******************************************************************************/


/******************************************************************************
 * MACRO: traitement_primes
 * 
 * Description: Imports finance reference data from Excel and prepares it
 *              for comparison with cube data
 *
 * Parameters:
 *   - annee : Year for processing (YYYY format)
 *   - mois  : Month for processing (MM format)
 *
 * Output:
 *   - BASE_FINANCE : Formatted finance data ready for comparison
 ******************************************************************************/

%MACRO traitement_primes(annee, mois);
    %local a;
    %let a = %nrstr(%mend);

    /* ========================================================================
     * STEP 1: IMPORT FINANCE DATA FROM EXCEL
     * ======================================================================== */
    
    /* Importation dans la work au format sas */
    proc import datafile = "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/TDB_PRIMES_&vision..XLS"
        out = tdb_primes_&annee.&mois.
        dbms = XLS replace;
        sheet = "SYNTHESE yc BLZ hs MAF" /*"SYNTHESE"*/; /* SYNTHESE ALLIANZ*/
    run;

    /* ========================================================================
     * STEP 2: EXTRACT COLUMN NAMES DYNAMICALLY
     * ======================================================================== */
    
    PROC CONTENTS DATA = tdb_primes_&annee.&mois. OUT = WORK.nom_variable (keep=name varnum) noprint; 
    RUN;

    /* Obtention du nom des colonnes ayant la position 1,3,6 */
    data _null_;
        set nom_variable;
        if varnum = 1 then do;
            call symput('col1', name);
        end;
        if varnum = 3 then do;
            call symput('col3', name);
        end;
        if varnum = 6 then do;
            call symput('col6', name);
        end;
    run;

    /* ========================================================================
     * STEP 3: ADAPT FINANCE DATA STRUCTURE TO CUBE FORMAT
     * ======================================================================== */
    
    /* Adaptation de la base de la finance à celle du cube */
    data tdb_primes_2;
        set tdb_primes_&annee.&mois.(rename=("&col1."n= LoB &col3.=Primes_emises_X &col6.=Primes_emises_N));
        length cdpole $1.;
        if (Primes_emises_X="" or LoB= "") then delete; /* on supprime les lignes blanches */
    run;

    Proc sort data=tdb_primes_2;
        by lob;
    run;

    /* ========================================================================
     * STEP 4: ASSIGN DISTRIBUTION CHANNEL (CDPOLE)
     * ======================================================================== */
    
    data tdb_primes_2;
        set tdb_primes_2;
        by LoB;
        retain cdpole "";
        if first.LoB then cdpole = '1';  /* Agent */
        else cdpole= '3';                 /* Courtage */
        if last.LoB then cdpole = "";
        
        /* Construction market identification */
        if LoB in ('CONSTRUCTION') then do;
            cmarch='6';
            cseg='2';
        end;
    run;

    /* ========================================================================
     * STEP 5: FORMAT AND FINALIZE FINANCE DATA
     * ======================================================================== */
    
    /* FORMAT */
    data BASE_FINANCE (keep= vision cdpole cmarch cseg Primes_emises_X Primes_emises_N);
        set tdb_primes_2;
        vision= &vision.;
        Primes_emises_X= round(Primes_emises_X * 1000000, 1);
        Primes_emises_N= round(Primes_emises_N * 1000000, 1);
        
        /* on supprime les lignes qui ne nous interesse pas ie celles donc le cdpole ou cmarch(resp cseg) est nul */
        if cmarch="" or cdpole="" then delete;
    run;
%MEND;


/******************************************************************************
 * MACRO: calcul_ecart_primes
 * 
 * Description: Calculates discrepancies between cube emissions data and
 *              finance reference data
 *
 * Parameters:
 *   - annee : Year for processing (YYYY format)
 *   - mois  : Month for processing (MM format)
 *
 * Output:
 *   - RESUL.ecart_Emissions&vision. : Table with calculated discrepancies
 ******************************************************************************/

%MACRO calcul_ecart_primes(annee, mois);
    %local a;
    %let a = %nrstr(%mend);

    /* ========================================================================
     * STEP 1: EXTRACT CUBE EMISSIONS DATA
     * ======================================================================== */
    
    /* le cube */
    PROC SQL;
        CREATE TABLE prime&vision. AS
        (SELECT 
            t1.VISION,
            t1.CDPOLE, 
            t1.CMARCH, 
            t1.CSEG, 
            SUM(t1.Primes_X) AS Primes_emises_X_cube, 
            SUM(t1.Primes_N) AS Primes_emises_N_cube
         FROM cube.primes_emises&vision._POL_GARP t1
         GROUP BY t1.VISION, t1.CDPOLE, t1.CMARCH, t1.CSEG);
    QUIT;

    /* ========================================================================
     * STEP 2: FORMAT CUBE DATA
     * ======================================================================== */
    
    /* FORMAT */
    data prime&vision.(keep= Vision cdpole cmarch cseg Primes_emises_X_cube Primes_emises_N_cube);
        set prime&vision.;
        Primes_emises_X_cube= round(Primes_emises_X_cube, 1);
        Primes_emises_N_cube= round(Primes_emises_N_cube, 1);
    run;

    /* ========================================================================
     * STEP 3: MERGE CUBE AND FINANCE DATA
     * ======================================================================== */
    
    /* MERGE */
    proc sort data= BASE_FINANCE;
        by vision cdpole cmarch cseg;
    run;

    proc sort data= prime&vision.;
        by vision cdpole cmarch cseg;
    run;

    data fusion;
        merge prime&vision.(in=a) BASE_FINANCE;
        by vision cdpole cmarch cseg;
        if a;
    run;

    /* ========================================================================
     * STEP 4: CALCULATE DISCREPANCIES
     * ======================================================================== */
    
    /* Calcul les ecarts (cube - finance) */
    data RESUL.ecart_Emissions&vision.;
        set fusion;
        if cmarch = '6' and cseg='2' then libelle_segment='CONSTRUCTION';
        Ecart_Primes_emises_X = round(Primes_emises_X - Primes_emises_X_cube, 1);
        Ecart_Primes_emises_N = round(Primes_emises_N - Primes_emises_N_cube, 1);
    run;
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
 * MACRO: recette_emissions
 * 
 * Description: Main orchestration macro - executes quality control if
 *              finance input file exists
 ******************************************************************************/

%macro recette_emissions;
    %if %sysfunc(fileexist("/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/TDB_PRIMES_&vision..XLS")) = 1
    %then %do;
        %traitement_primes(&annee., &mois.);
        %calcul_ecart_primes(&annee., &mois.);
    %end;
%mend;


/* ============================================================================
 * EXECUTION
 * ============================================================================ */

%recette_emissions;
