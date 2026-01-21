/******************************************************************************
 * FILE: EMISSIONS_RUN.sas
 * 
 * Description: Production program for emissions data extraction from One BI
 *              Extracts premium data from One BI, applies filters, enriches
 *              with segmentation, and creates construction market emissions cubes
 *
 * Input Sources:
 *   - PRM.rf_fr1_prm_dtl_midcorp_m : One BI premium detail data
 *   - SEG.segmentprdt_&vision.      : Product segmentation reference
 *
 * Output Tables:
 *   - CUBE.PRIMES_EMISES&vision._POL_GARP : Emissions by policy and guarantee
 *   - CUBE.PRIMES_EMISES&vision._POL      : Emissions aggregated by policy
 ******************************************************************************/


/* ============================================================================
 * DATE AND VISION VARIABLES SETUP
 * ============================================================================ */

data _null_;
    date_run = cats(put(year(today()), z4.), put(month(today()), z2.), put(day(today()), z2.));
    hour = cats(put(hour(time()), z2.), "H", put(minute(time()), z2.));
    call symputx("date_run", date_run);
    call symputx("hour", hour);
run;

data _null_;
    /*date = intnx('month', today(), -1, 'e');*/
    Date = mdy(substr("&VISION.", 5, 2), 10, substr("&VISION.", 1, 4));
    call symput('DATEARRET', quote(put(date, date9.)) || 'd');
    call symput('date_time', compress(put(datetime(), datetime20.)));
run;

/*%let DATEARRET = "31JAN2021"d;*/

data _null_;
    /* Année sélectionnée */
    call symput("ANNEE", put(year(&dateArret.), z4.));
    /* Mois sélectionnée en avec zéro significatif */
    call symput("MOIS", put(month(&dateArret.), z2.));
run;

data _null_;
    /* Partie specifique au Projet */
    m = compress(put(&mois., z2.));
    a = substr(put(&annee., z4.), 3, 2);
    call symput('vision', compress(&annee. || m));
run;


/******************************************************************************
 * MACRO: emissions_libname
 * 
 * Description: Sets up library assignments and remote connections
 *              - Establishes One BI connection (if UTF-8 encoding)
 *              - Allocates PRM library on remote server
 *              - Assigns local SAS and DTM Construction libraries
 *              - Includes transcodification macro
 ******************************************************************************/

%macro emissions_libname;

    /* ========================================================================
     * ONE BI REMOTE CONNECTION SETUP (UTF-8 ENCODING ONLY)
     * ======================================================================== */
    
    %if "&SYSENCODING." = "utf-8" %then %do;
        /* Connexion a One BI */
        options source compress=yes;
        %let server_to_use= biaa-sg-prod.srv.allianz;
        %let port_connect =7556;
        options netencryptalgorithm=(AES);
        %let remhost=&server_to_use &port_connect;
        options comamid=tcp remote=remhost;
        signon noscript userid=%upcase(&sysuserid.) password="&MPSSOX";
    %end;

    /* ========================================================================
     * ONE BI LIBRARY ALLOCATION
     * ======================================================================== */
    
    %if %sysfunc(libref(prm)) %then %do;
        rsubmit;
            /* ALLOCATION BASES ONE BI sur une session distante */
            libname prm "/var/opt/data/data_project/sas94/bifr/azfronebi/data/shared_lib/prm";
        endrsubmit;

        /* Rendre prm et Work visibles sur SASApp_AMOS */
        libname prm server=remhost;
        libname rwork slibref=work server=remhost;
    %end;
    %else %do;
        %put libref exists;
    %end;

    /* FIN DES LIBRAIRIES ONE-BI */

    /* ========================================================================
     * LOCAL SAS LIBRARIES
     * ======================================================================== */
    
    /* LIBRAIRIES SAS */
    LIBNAME SEG "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/SEG_PRDTS";
    /* FIN DES LIBRAIRIES SAS*/

    /* ========================================================================
     * DTM CONSTRUCTION LIBRARIES
     * ======================================================================== */
    
    /* LIBRAIRIES DTM CONSTRUCTION */
    x "mkdir /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    LIBNAME CUBE "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    /* FIN DE LIBRAIRIES DTM CONSTRUCTION */

    /* ========================================================================
     * INCLUDE TRANSCODIFICATION MACRO
     * ======================================================================== */
    
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/02_TRANSCODIFICATION_ABS.sas";
%mend;

%emissions_libname;


/* ============================================================================
 * REMOTE VARIABLE ASSIGNMENT
 * ============================================================================ */

%syslput annee = &annee.;
%syslput mois = &mois.;
%syslput vision = &vision.;


/* ============================================================================
 * REMOTE EMISSIONS DATA EXTRACTION FROM ONE BI
 * ============================================================================ */

rsubmit;
    /* ========================================================================
     * FILTER DEFINITION - EXCLUDED INTERMEDIARIES
     * ======================================================================== */
    
    %let nointFIC =('102030' '446000' '446118' '446218' '482001' '489090' '500150' '4A1400' '4A1500' '4A1600' '4A1700' '4A1800' '4A1900' '4F1004' '5B2000' '5R0001' 'H90036' 'H90037' 'H90059' 'H90061' 'H99045' 'H99059');

    /* ========================================================================
     * STEP 1: EXTRACT RAW PREMIUM DATA FROM ONE BI
     * ======================================================================== */
    
    proc sql;
        create table PRIMES_ONE_BI as
        select 
            t1.CD_NIV_2_STC, t1.CD_INT_STC, t1.NU_CNT_PRM, t1.CD_PRD_PRM, 
            t1.CD_STATU_CTS, t1.DT_CPTA_CTS, t1.DT_EMIS_CTS, t1.DT_ANNU_CTS, 
            t1.MT_HT_CTS, t1.MT_CMS_CTS, t1.CD_CAT_MIN, t1.CD_GAR_PRINC, 
            t1.CD_GAR_PROSPCTIV, t1.nu_ex_ratt_cts, t1.cd_marche
        from PRM.rf_fr1_prm_dtl_midcorp_m t1
        where t1.cd_marche in ("6") and t1.DT_CPTA_CTS<= "&vision";
    quit;

    /* ========================================================================
     * STEP 2: APPLY BUSINESS FILTERS AND TRANSFORMATIONS
     * ======================================================================== */
    
    PROC SQL;
        CREATE TABLE PRIMES_EMISES_1 AS
        SELECT 
            t1.NU_CNT_PRM AS NOPOL, 
            t1.CD_PRD_PRM AS CDPROD, 
            t1.CD_INT_STC AS NOINT, 
            compress(substr(t1.cd_gar_prospctiv, 3, 3)) as CGAR,
            (CASE WHEN compress(t1.NU_EX_RATT_CTS) ge "&annee." THEN 'cou' ELSE 'ant' END) AS EXERCICE,
            (CASE 
                WHEN t1.CD_NIV_2_STC in('DCAG','DCPS','DIGITAL') THEN '1'
                WHEN t1.CD_NIV_2_STC = 'BROKDIV' THEN '3'
             END) AS CDPOLE,
            'AZ ' as DIRCOM format=$4.,
            t1.MT_HT_CTS, 
            t1.MT_CMS_CTS AS MTCOM, 
            t1.CD_GAR_PRINC, 
            t1.CD_GAR_PROSPCTIV, 
            t1.CD_CAT_MIN
        FROM PRIMES_ONE_BI t1
        WHERE t1.CD_INT_STC not in &nointFIC.
            AND not(substr(t1.CD_PRD_PRM, 1, 2) IN ('19') and t1.CD_GAR_PROSPCTIV IN ('220'))
            and not(t1.CD_INT_STC in ("567ME0") and t1.CD_PRD_PRM in ("00200"))
            and not(t1.CD_GAR_PROSPCTIV IN ('180','183','184','185'))
            and not(t1.CD_PRD_PRM in ("01073"));
    QUIT;

    /* ========================================================================
     * STEP 3: EXCLUDE SPECIFIC CATEGORIES
     * ======================================================================== */
    
    data PRIMES_EMISES_1;
        set PRIMES_EMISES_1;
        if (CD_CAT_MIN in ("792","793") or (CD_GAR_PROSPCTIV="00400")) then delete;
    run;

    /* ========================================================================
     * STEP 4: AGGREGATE PREMIUMS (CURRENT YEAR vs TOTAL)
     * ======================================================================== */
    
    PROC SQL;
        CREATE TABLE PRIMES_EMISES&vision._OBI AS
        SELECT distinct 
            t1.CDPOLE, t1.CDPROD, t1.NOPOL, t1.NOINT, 
            t1.CD_GAR_PRINC, t1.CD_GAR_PROSPCTIV, t1.DIRCOM, t1.CD_CAT_MIN,
            SUM(t1.MT_HT_CTS) AS PRIMES_X format=8.2,
            t2.PRIMES_N format=8.2,
            SUM(t1.MTCOM) AS MTCOM_X format=8.2
        FROM PRIMES_EMISES_1 t1
        LEFT JOIN (
            SELECT DISTINCT 
                CDPOLE, CDPROD, NOPOL, NOINT, CD_GAR_PRINC, CD_GAR_PROSPCTIV, 
                CD_CAT_MIN, DIRCOM, sum(MT_HT_CTS) as PRIMES_N format=best32. 
            from PRIMES_EMISES_1 
            where EXERCICE="cou" 
            group by 1, 2, 3, 4, 5, 6, 7, 8
        ) as t2
        on t1.NOPOL = t2.NOPOL 
            and t1.NOINT = t2.NOINT 
            and t1.CDPOLE = t2.CDPOLE 
            and t1.CDPROD = t2.CDPROD 
            and t1.DIRCOM = t2.DIRCOM 
            and t1.CD_GAR_PRINC = t2.CD_GAR_PRINC 
            and t1.CD_GAR_PROSPCTIV = t2.CD_GAR_PROSPCTIV 
            and t1.CD_CAT_MIN = t2.CD_CAT_MIN
        GROUP BY t1.CDPOLE, t1.NOINT, t1.NOPOL, t1.CDPROD, t1.CD_GAR_PRINC, t1.CD_GAR_PROSPCTIV, t1.DIRCOM, t1.CD_CAT_MIN
        ORDER BY t1.NOPOL, T1.CDPROD, T1.NOINT;
    QUIT;

    /* ========================================================================
     * STEP 5: ADD VISION AND DOWNLOAD FROM REMOTE
     * ======================================================================== */
    
    DATA PRIMES_EMISES&vision._OBI;
        retain VISION;
        set PRIMES_EMISES&vision._OBI;
        VISION = &vision.;
    run;

    proc download data=PRIMES_EMISES&vision._OBI out =PRIMES_EMISES&vision._OBI; 
    run;
endrsubmit;


/* ============================================================================
 * LOCAL PROCESSING - TRANSCODIFICATION AND ENRICHMENT
 * ============================================================================ */

/* Transcodification UTF-8 to LATIN9 */
%transco_tab(PRIMES_EMISES&vision._OBI, PRIMES_EMISES&vision._OBI_TR);

/* ========================================================================
 * ENRICH WITH SEGMENTATION DATA
 * ======================================================================== */

PROC SQL;
    CREATE TABLE PRIMES_EMISES&vision._OBI_TR AS
    SELECT t1.*, t2.CMARCH, t2.CSEG, t2.CSSSEG
    FROM PRIMES_EMISES&vision._OBI_TR t1
    LEFT JOIN SEG.segmentprdt_&vision. t2 
        ON (t1.CDPROD = t2.CPROD AND t1.CDPOLE = t2.CDPOLE)
    ORDER BY t1.NOPOL, T1.CDPROD, T1.NOINT;
QUIT;

/* ========================================================================
 * FILTER FOR CONSTRUCTION MARKET AND PREPARE FINAL OUTPUT
 * ======================================================================== */

data PRIMES_EMISES&vision._OBI_TR;
    set PRIMES_EMISES&vision._OBI_TR(where=(CMARCH = "6"));
    CGARP = SUBSTR(CD_GAR_PROSPCTIV, 3, 5);
    keep NOPOL CDPROD NOINT CGARP CMARCH CSEG CSSSEG cdpole VISION DIRCOM CD_CAT_MIN Primes_X Primes_N MTCOM_X;
run;

/* ========================================================================
 * CREATE FINAL CUBE TABLES
 * ======================================================================== */

/* Table with guarantee detail (POL_GARP) */
proc sql;
    create table CUBE.PRIMES_EMISES&vision._POL_GARP as
    select 
        VISION, DIRCOM, cdpole, NOPOL, CDPROD, NOINT, CGARP, 
        CMARCH, CSEG, CSSSEG, CD_CAT_MIN,
        sum(Primes_X) as Primes_X,
        sum(Primes_N) as Primes_N,
        sum(MTCOM_X) as MTCOM_X
    from PRIMES_EMISES&vision._OBI_TR
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;
quit;

/* Table aggregated by policy (POL) */
proc sql;
    create table CUBE.PRIMES_EMISES&vision._POL as
    select 
        VISION, DIRCOM, NOPOL, NOINT, cdpole, CDPROD, 
        CMARCH, CSEG, CSSSEG,
        sum(Primes_X) as Primes_X,
        sum(Primes_N) as Primes_N,
        sum(MTCOM_X) as MTCOM_X
    from CUBE.PRIMES_EMISES&vision._POL_GARP
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9;
quit;
