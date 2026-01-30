/******************************************************************************
 * MACRO: consolidation_az_azec_mvt_ptf
 * 
 * Description: Consolidates portfolio/movements data from AZ and AZEC sources
 *              Applies vision-dependent logic for AZEC integration
 *              Enriches with RISK data (Q45, Q46, QAN) from One BI
 *              Adds client, product, and ISIC code enrichment
 *
 * Parameters: None (uses &vision. macro variable)
 *
 * Input Tables:
 *   - MVT_CONST_PTF&vision.        : AZ portfolio/movements data
 *   - AZEC_PTF&vision.             : AZEC portfolio/movements data
 *   - CUBE.IRD_RISK_Q46_&vision.   : RISK questionnaire 46 data
 *   - CUBE.IRD_RISK_Q45_&vision.   : RISK questionnaire 45 data
 *   - CUBE.IRD_RISK_QAN_&vision.   : RISK annual questionnaire data
 *   - CLIENT1.CLACENT1 / CLIENT3.CLACENT3 : Client data
 *   - IPFSPE1/3.IPFM0024/63/99     : Special product data
 *
 * Output:
 *   - CUBE.MVT_PTF&vision. : Consolidated AZ+AZEC data with full enrichment
 *
 * Logic:
 *   - If vision >= 201211: Union AZ + AZEC data
 *   - If vision < 201211: AZ data only
 *   - If vision >= 202210: Use current vision RISK data
 *   - If vision < 202210: Use 202210 reference RISK data
 ******************************************************************************/

%macro consolidation_az_azec_mvt_ptf;

    /* ========================================================================
     * STEP 1: CONSOLIDATE AZ AND AZEC DATA (VISION-DEPENDENT)
     * ======================================================================== */
    
    %if &vision. >=201211 %then %do;
        /* ====================================================================
         * PATH A: AZ + AZEC CONSOLIDATION (VISION >= 201211)
         * ==================================================================== */
        
        PROC SQL;
            /* Portefeuille AZ + AZEC */
            CREATE TABLE MVT_PTF&vision. AS
            (
                /* AZ Portfolio Data */
                SELECT 
                    NOPOL, NOPOLLi1, DIRCOM, CDPOLE, NOINT, CDPROD, CMARCH, CSEG, CSSSEG, DTCREPOL,
                    posacta_ri, rueacta_ri, cediacta_ri,
                    input(substr(put(DTECHANN, 5.), length(put(DTECHANN, 5.)) - 3, 2), 4.) AS MOIS_ECHEANCE,
                    input(substr(put(DTECHANN, 5.), length(put(DTECHANN, 5.)) - 1, 2), 4.) AS JOUR_ECHEANCE,
                    DTRESILP, DTTRAAR, CDNATP, CDSITP, PTGST, CDREG, CDGECENT, MTCA, CDNAF,
                    (CASE substr(CDTRE, 1, 1) when '*' THEN substr(CDTRE, 2, 3) ELSE CDTRE END) AS CDTRE,
                    CDCOAS AS CDCOASS, COASS, TOP_COASS,
                    /* Dans IMS on a uniquement des affaires directes */
                    "D" AS TYPE_AFFAIRE,
                    Primeto as PRIMES_PTF_INTEMP,
                    Cotis_100 as PRIMES_PTF_100_INTEMP,
                    PARTCIE as PART_CIE,
                    PRIMES_PTF, NBPTF, EXPO_YTD, EXPO_GLI, TOP_TEMP,
                    CDMOTRES, CDCASRES, CDPOLRVI, VISION, EXEVUE, MOISVUE, NOCLT, NMCLT,
                    CDFRACT, QUARISQ, NMRISQ, NMSRISQ, RESRISQ, RUERISQ, LIDIRISQ, POSRISQ, VILRISQ,
                    NBAFN, NBRES, NBAFN_ANTICIPE, NBRES_ANTICIPE, PRIMES_AFN, PRIMES_RES, UPPER_MID,
                    DT_DEB_EXPO, DT_FIN_EXPO,
                    segment2, type_produit_2, CTDEFTRA, DSTCSC, DTOUCHAN, DTRECTRX, DTRCPPR, LBQLTSOU,
                    DTEFFAN, DTTRAAN, ACTPRIN, CTPRVTRV, MTSMPR, LBNATTRV,
                    TOP_REVISABLE, CRITERE_REVISION, CDGREV, NBRPT, NBRPC, Primes_RPC, Primes_RPT, 
                    MTCAENP, MTCASST, MTCAVNT
                FROM MVT_CONST_PTF&vision.
            )
            OUTER UNION CORR
            (
                /* AZEC Portfolio Data */
                SELECT 
                    POLICE AS NOPOL, POLICE AS NOPOLLi1, "AZEC" AS DIRCOM, "3" AS CDPOLE, 
                    INTERMED AS NOINT, PRODUIT AS CDPROD, CMARCH, CSEG, CSSSEG, EFFETPOL AS DTCREPOL,
                    month(DTECHANN) AS MOIS_ECHEANCE, day(DTECHANN) AS JOUR_ECHEANCE,
                    DATFIN AS DTRESILP, DATRESIL AS DTTRAAR, CDNATP, "" AS CDSITP, POINGEST AS PTGST, 
                    "" AS CDREG, . AS CDGECENT, MTCA, CODE_NAF AS CDNAF,
                    (CASE substr(CODE_TRE, 1, 1) when '*' THEN substr(CODE_TRE, 2, 3) ELSE CODE_TRE END) AS CDTRE,
                    CODECOAS AS CDCOASS, COASS, TYPE_AFFAIRE,  /* Attention à la réass. acceptée V5 */
                    TOP_COASS,
                    Primeto as PRIMES_PTF_INTEMP,
                    Cotis_100 as PRIMES_PTF_100_INTEMP,
                    PARTCIE as PART_CIE,
                    PRIMES_PTF, NBPTF, EXPO_YTD, EXPO_GLI, 0 AS TOP_TEMP,
                    /* Modification gestion motif/orig résiliation AZEC */
                    (CASE ORIGRES 
                        WHEN "CL" THEN "A" 
                        WHEN "AP" THEN "C" 
                        WHEN "CI" THEN "Z" 
                        WHEN "" THEN "" 
                        ELSE "X" 
                     END) AS CDMOTRES,
                    MOTIFRES AS CDCASRES, "" AS CDPOLRVI, VISION, EXEVUE, MOISVUE, "" AS NOCLT, 
                    NOMCLI AS NMCLT,
                    "" AS CDFRACT, "" AS QUARISQ, "" AS NMRISQ, "" AS NMSRISQ, "" AS RESRISQ, 
                    "" AS RUERISQ, "" AS LIDIRISQ, "" AS POSRISQ, EFFETPOL AS DTEFFAN, EFFETPOL AS DTTRAAN, 
                    "" AS VILRISQ,
                    NBAFN, NBRES, NBAFN_ANTICIPE, NBRES_ANTICIPE, PRIMES_AFN, PRIMES_RES, 
                    segment2, type_produit_2, MNT_GLOB as CTDEFTRA, DEST_LOC as DSTCSC, DATOUVCH as DTOUCHAN,
                    DATRECEP as DTRECTRX, DATFINCH as DTRCPPR, LQUALITE as LBQLTSOU, UPPER_MID, 
                    DT_DEB_EXPO, DT_FIN_EXPO, "" as ACTPRIN, . as CTPRVTRV, . as MTSMPR, "" as LBNATTRV,
                    TOP_REVISABLE, CRITERE_REVISION, CDGREV, 0 as NBRPT, 0 as NBRPC, 0 as Primes_RPC, 
                    0 as Primes_RPT, 0 as MTCAENP, 0 as MTCASST, 0 as MTCAVNT
                FROM AZEC_PTF&vision.
            );
        QUIT;
    %end;
    %else %do;
        /* ====================================================================
         * PATH B: AZ ONLY (VISION < 201211)
         * ==================================================================== */
        
        PROC SQL;
            /* Portefeuille AZ */
            CREATE TABLE MVT_PTF&vision. AS
            (
                SELECT 
                    NOPOL, NOPOLLi1, DIRCOM, CDPOLE, NOINT, CDPROD, CMARCH, CSEG, CSSSEG, DTCREPOL,
                    posacta_ri, rueacta_ri, cediacta_ri,
                    input(substr(put(DTECHANN, 5.), length(put(DTECHANN, 5.)) - 3, 2), 4.) AS MOIS_ECHEANCE,
                    input(substr(put(DTECHANN, 5.), length(put(DTECHANN, 5.)) - 1, 2), 4.) AS JOUR_ECHEANCE,
                    DTRESILP, DTTRAAR, CDNATP, CDSITP, PTGST, CDREG, CDGECENT, MTCA, CDNAF,
                    (CASE substr(CDTRE, 1, 1) when '*' THEN substr(CDTRE, 2, 3) ELSE CDTRE END) AS CDTRE,
                    CDCOAS AS CDCOASS, COASS, TOP_COASS,
                    /* Dans IMS on a uniquement des affaires directes */
                    "D" AS TYPE_AFFAIRE,
                    Primeto as PRIMES_PTF_INTEMP,
                    Cotis_100 as PRIMES_PTF_100_INTEMP,
                    PARTCIE as PART_CIE,
                    PRIMES_PTF, NBPTF, EXPO_YTD, EXPO_GLI, TOP_TEMP,
                    CDMOTRES, CDCASRES, CDPOLRVI, VISION, EXEVUE, MOISVUE, NOCLT, NMCLT,
                    CDFRACT, QUARISQ, NMRISQ, NMSRISQ, RESRISQ, RUERISQ, LIDIRISQ, POSRISQ, VILRISQ,
                    NBAFN, NBRES, NBAFN_ANTICIPE, NBRES_ANTICIPE, PRIMES_AFN, PRIMES_RES, UPPER_MID,
                    DT_DEB_EXPO, DT_FIN_EXPO,
                    segment2, type_produit_2, CTDEFTRA, DSTCSC, DTOUCHAN, DTRECTRX, DTRCPPR, LBQLTSOU,
                    DTEFFAN, DTTRAAN, ACTPRIN, CTPRVTRV, MTSMPR, LBNATTRV,
                    TOP_REVISABLE, CRITERE_REVISION, CDGREV, NBRPT, NBRPC, Primes_RPC, Primes_RPT, 
                    MTCAENP, MTCASST, MTCAVNT
                FROM MVT_CONST_PTF&vision.
            );
        QUIT;
    %end;

    /* ========================================================================
     * STEP 2: ENRICH WITH RISK DATA (VISION-DEPENDENT)
     * ======================================================================== */
    
    %if &vision. >= 202210 %then %do;
        /* ====================================================================
         * PATH A: USE CURRENT VISION RISK DATA (VISION >= 202210)
         * ==================================================================== */
        
        /* ================================================================
         * Q46 RISK DATA ENRICHMENT
         * ================================================================ */
        
        data ird_risk_q46_&vision.;
            set cube.ird_risk_q46_&vision.;
            format DTOUCHAN_RISK ddmmyy10. DTRECTRX_RISK ddmmyy10. DTREFFIN_RISK ddmmyy10.;
            DTOUCHAN_RISK = datepart(DTOUCHAN);
            DTRECTRX_RISK = datepart(DTRECTRX);
            DTREFFIN_RISK = datepart(DTREFFIN);
            CTPRVTRV_RISK = CTPRVTRV;
            CTDEFTRA_RISK = CTDEFTRA;
            LBNATTRV_RISK = LBNATTRV;
            LBDSTCSC_RISK = LBDSTCSC;
            keep NOPOL DTOUCHAN_RISK DTRECTRX_RISK DTREFFIN_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
        run;

        proc sql;
            create table MVT_PTF&vision. as
            select a.*, b.* 
            from MVT_PTF&vision. a 
            left join ird_risk_q46_&vision. b on a.nopol=b.nopol;
        quit;

        data MVT_PTF&vision.;
            set MVT_PTF&vision.;
            if missing(DTOUCHAN) then DTOUCHAN=DTOUCHAN_RISK;
            if missing(DTRECTRX) then DTRECTRX=DTRECTRX_RISK;
            if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
            if missing(CTDEFTRA) then CTDEFTRA=CTDEFTRA_RISK;
            if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
            if missing(DSTCSC) then DSTCSC=LBDSTCSC_RISK;
            DTREFFIN=DTREFFIN_RISK;
            Drop DTOUCHAN_RISK DTRECTRX_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK DTREFFIN_RISK LBDSTCSC_RISK;
        run;

        /* ================================================================
         * Q45 RISK DATA ENRICHMENT
         * ================================================================ */
        
        data ird_risk_q45_&vision.;
            set cube.ird_risk_q45_&vision.;
            format DTOUCHAN_RISK ddmmyy10. DTRECTRX_RISK ddmmyy10. DTREFFIN_RISK ddmmyy10.;
            DTOUCHAN_RISK = datepart(DTOUCHAN);
            DTRECTRX_RISK = datepart(DTRECTRX);
            DTREFFIN_RISK = datepart(DTREFFIN);
            CTPRVTRV_RISK = CTPRVTRV;
            CTDEFTRA_RISK = CTDEFTRA;
            LBNATTRV_RISK = LBNATTRV;
            LBDSTCSC_RISK = LBDSTCSC;
            keep NOPOL DTOUCHAN_RISK DTRECTRX_RISK DTREFFIN_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
        run;

        proc sql;
            create table MVT_PTF&vision. as
            select a.*, b.* 
            from MVT_PTF&vision. a 
            left join ird_risk_q45_&vision. b on a.nopol=b.nopol;
        quit;

        data MVT_PTF&vision.;
            set MVT_PTF&vision.;
            if missing(DTOUCHAN) then DTOUCHAN=DTOUCHAN_RISK;
            if missing(DTRECTRX) then DTRECTRX=DTRECTRX_RISK;
            if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
            if missing(CTDEFTRA) then CTDEFTRA=CTDEFTRA_RISK;
            if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
            if missing(DTREFFIN) then DTREFFIN=DTREFFIN_RISK;
            if missing(DSTCSC) then DSTCSC=LBDSTCSC_RISK;
            Drop DTOUCHAN_RISK DTRECTRX_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK DTREFFIN_RISK LBDSTCSC_RISK;
        run;

        /* ================================================================
         * QAN RISK DATA ENRICHMENT
         * ================================================================ */
        
        data ird_risk_qan_&vision.;
            set cube.ird_risk_qan_&vision.;
            format DTOUCHAN_RISK ddmmyy10. DTRCPPR_RISK ddmmyy10.;
            DTOUCHAN_RISK = datepart(DTOUCHAN);
            DTRCPPR_RISK = datepart(DTRCPPR);
            CTPRVTRV_RISK = CTPRVTRV;
            CTDEFTRA_RISK = CTDEFTRA;
            LBNATTRV_RISK = LBNATTRV;
            LBDSTCSC_RISK = DSTCSC;
            keep NOPOL DTOUCHAN_RISK DTRCPPR_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
        run;

        proc sql;
            create table MVT_PTF&vision. as
            select a.*, b.* 
            from MVT_PTF&vision. a 
            left join ird_risk_qan_&vision. b on a.nopol=b.nopol;
        quit;

        data MVT_PTF&vision.;
            set MVT_PTF&vision.;
            if missing(DTOUCHAN) then DTOUCHAN=DTOUCHAN_RISK;
            if missing(DTRCPPR) then DTRCPPR=DTRCPPR_RISK;
            if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
            if missing(CTDEFTRA) then CTDEFTRA=CTDEFTRA_RISK;
            if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
            if missing(DSTCSC) then DSTCSC=LBDSTCSC_RISK;
            Drop DTOUCHAN_RISK DTRCPPR_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
        run;
    %end;
    %else %do;
        /* ====================================================================
         * PATH B: USE 202210 REFERENCE RISK DATA (VISION < 202210)
         * ==================================================================== */
        
        /* Same pattern as above but using RISK_REF.ird_risk_*_202210 */
        /* Q46 */
        data ird_risk_q46_202210;
            set RISK_REF.ird_risk_q46_202210;
            format DTOUCHAN_RISK ddmmyy10. DTRECTRX_RISK ddmmyy10. DTREFFIN_RISK ddmmyy10.;
            DTOUCHAN_RISK = datepart(DTOUCHAN);
            DTRECTRX_RISK = datepart(DTRECTRX);
            DTREFFIN_RISK = datepart(DTREFFIN);
            CTPRVTRV_RISK = CTPRVTRV;
            CTDEFTRA_RISK = CTDEFTRA;
            LBNATTRV_RISK = LBNATTRV;
            LBDSTCSC_RISK = LBDSTCSC;
            keep NOPOL DTOUCHAN_RISK DTRECTRX_RISK DTREFFIN_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
        run;

        proc sql;
            create table MVT_PTF&vision. as
            select a.*, b.* 
            from MVT_PTF&vision. a 
            left join ird_risk_q46_202210 b on a.nopol=b.nopol;
        quit;

        data MVT_PTF&vision.;
            set MVT_PTF&vision.;
            if missing(DTOUCHAN) then DTOUCHAN=DTOUCHAN_RISK;
            if missing(DTRECTRX) then DTRECTRX=DTRECTRX_RISK;
            if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
            if missing(CTDEFTRA) then CTDEFTRA=CTDEFTRA_RISK;
            if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
            DTREFFIN=DTREFFIN_RISK;
            if missing(DSTCSC) then DSTCSC=LBDSTCSC_RISK;
            Drop DTOUCHAN_RISK DTRECTRX_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK DTREFFIN_RISK LBDSTCSC_RISK;
        run;

        /* Q45 */
        data ird_risk_q45_202210;
            set RISK_REF.ird_risk_q45_202210;
            format DTOUCHAN_RISK ddmmyy10. DTRECTRX_RISK ddmmyy10. DTREFFIN_RISK ddmmyy10.;
            DTOUCHAN_RISK = datepart(DTOUCHAN);
            DTRECTRX_RISK = datepart(DTRECTRX);
            DTREFFIN_RISK = datepart(DTREFFIN);
            CTPRVTRV_RISK = CTPRVTRV;
            CTDEFTRA_RISK = CTDEFTRA;
            LBNATTRV_RISK = LBNATTRV;
            LBDSTCSC_RISK = LBDSTCSC;
            keep NOPOL DTOUCHAN_RISK DTRECTRX_RISK DTREFFIN_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
        run;

        proc sql;
            create table MVT_PTF&vision. as
            select a.*, b.* 
            from MVT_PTF&vision. a 
            left join ird_risk_q45_202210 b on a.nopol=b.nopol;
        quit;

        data MVT_PTF&vision.;
            set MVT_PTF&vision.;
            if missing(DTOUCHAN) then DTOUCHAN=DTOUCHAN_RISK;
            if missing(DTRECTRX) then DTRECTRX=DTRECTRX_RISK;
            if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
            if missing(CTDEFTRA) then CTDEFTRA=CTDEFTRA_RISK;
            if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
            if missing(DTREFFIN) then DTREFFIN=DTREFFIN_RISK;
            if missing(DSTCSC) then DSTCSC=LBDSTCSC_RISK;
            Drop DTOUCHAN_RISK DTRECTRX_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK DTREFFIN_RISK LBDSTCSC_RISK;
        run;

        /* QAN */
        data ird_risk_qan_202210;
            set RISK_REF.ird_risk_qan_202210;
            format DTOUCHAN_RISK ddmmyy10. DTRCPPR_RISK ddmmyy10.;
            DTOUCHAN_RISK = datepart(DTOUCHAN);
            DTRCPPR_RISK = datepart(DTRCPPR);
            CTPRVTRV_RISK = CTPRVTRV;
            CTDEFTRA_RISK = CTDEFTRA;
            LBNATTRV_RISK = LBNATTRV;
            LBDSTCSC_RISK = DSTCSC;
            keep NOPOL DTOUCHAN_RISK DTRCPPR_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
        run;

        proc sql;
            create table MVT_PTF&vision. as
            select a.*, b.* 
            from MVT_PTF&vision. a 
            left join ird_risk_qan_202210 b on a.nopol=b.nopol;
        quit;

        data MVT_PTF&vision.;
            set MVT_PTF&vision.;
            if missing(DTOUCHAN) then DTOUCHAN=DTOUCHAN_RISK;
            if missing(DTRCPPR) then DTRCPPR=DTRCPPR_RISK;
            if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
            if missing(CTDEFTRA) then CTDEFTRA=CTDEFTRA_RISK;
            if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
            if missing(DSTCSC) then DSTCSC=LBDSTCSC_RISK;
            Drop DTOUCHAN_RISK DTRCPPR_RISK CTPRVTRV_RISK CTDEFTRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
        run;
    %end;

    /* ========================================================================
     * STEP 3: DATA QUALITY ADJUSTMENTS
     * ======================================================================== */
    
    data MVT_PTF&vision.;
        set MVT_PTF&vision.;
        format DTREFFIN ddmmyy10.;
    run;

    /* Fill missing receipt date with end date for construction sites */
    data MVT_PTF&vision.;
        set MVT_PTF&vision.;
        if missing(DTRCPPR) and not missing(DTREFFIN) then do;
            DTRCPPR=DTREFFIN;
        end;
    run;

    /* ========================================================================
     * STEP 4: ENRICH WITH CLIENT DATA (SIRET/SIREN)
     * ======================================================================== */
    
    proc sql;
        create table mvt_ptf&vision. as
        select 
            a.*, 
            coalesce(c1.cdsiret, c3.cdsiret) as cdsiret,
            coalesce(c1.cdsiren, c3.cdsiren) as cdsiren
        from mvt_ptf&vision. a
        left join client1.clacent1 c1 on a.noclt=c1.noclt
        left join client3.clacent3 c3 on a.noclt=c3.noclt;
    quit;

    /* ========================================================================
     * STEP 5: ADD EULER RISK NOTE
     * ======================================================================== */
    
    proc sql;
        create table mvt_ptf&vision. as
        select 
            a.*, 
            (case when t2.cdnote in ("00") then "" else t2.cdnote end) as note_euler
        from mvt_ptf&vision. a
        left join binsee.histo_note_risque t2 
            on a.cdsiren=t2.cdsiren 
            and t2.dtdeb_valid <="&dtfinmn"d 
            and t2.dtfin_valid >="&dtfinmn"d;
    quit;

    /* ========================================================================
     * STEP 6: ADD CONSTRUCTION SITE DESTINATION
     * ======================================================================== */
    
    proc sql;
        create table MVT_PTF&vision. as
        select t1.*, t2.DESTINAT
        from MVT_PTF&vision. t1
        left join DEST.DO_DEST202110 t2 on t1.NOPOL= t2.NOPOL;
    quit;

    /* Apply business rules for destination identification */
    data CUBE.MVT_PTF&vision.;
        set MVT_PTF&vision.;
        if segment2="Chantiers" and missing(DESTINAT) then do;
            if PRXMATCH ( "/(HABIT)/" , UPCASE ( DSTCSC ) ) OR PRXMATCH ( "/(HABIT)/" , UPCASE ( LBNATTRV ) ) 
               OR PRXMATCH ( "/(LOG)/" , UPCASE ( DSTCSC ) ) OR PRXMATCH ( "/(LOG)/" , UPCASE ( LBNATTRV ) ) 
               OR PRXMATCH ( "/(LGT)/" , UPCASE ( DSTCSC ) ) OR PRXMATCH ( "/(LGT)/" , UPCASE ( LBNATTRV ) ) 
               OR PRXMATCH ( "/(MAIS)/" , UPCASE ( DSTCSC ) ) OR PRXMATCH ( "/(MAIS)/" , UPCASE ( LBNATTRV ) ) 
               OR PRXMATCH ( "/(APPA)/" , UPCASE ( DSTCSC ) ) OR PRXMATCH ( "/(APPA)/" , UPCASE ( LBNATTRV ) ) 
               OR PRXMATCH ( "/(VILLA)/" , UPCASE ( DSTCSC ) ) OR PRXMATCH ( "/(VILLA)/" , UPCASE ( LBNATTRV ) ) 
               OR PRXMATCH ( "/(INDIV)/" , UPCASE ( DSTCSC ) ) OR PRXMATCH ( "/(INDIV)/" , UPCASE ( LBNATTRV ) ) 
               THEN DESTINAT= 'Habitation';
            ELSE DESTINAT='Autres';
            if DSTCSC in("01","02","03","04","1","2","3","4","22") then DESTINAT ='Habitation';
        end;
    run;

    data CUBE.MVT_PTF&vision.;
        retain nopol dircom noint cdpole cdprod cmarch cseg cssseg;
        set CUBE.MVT_PTF&vision.;
    run;

    /* ========================================================================
     * STEP 7: EXTRACT SPECIAL PRODUCT ACTIVITY DATA
     * ======================================================================== */
    
    /* Initialize empty table */
    data TABSPEC_CONST_&vision.;
        NOPOL="";
        NOINT="";
        CDPROD="";
        CDPOLE="";
        CDACTCONST="";
        CDACTCONST2="";
        CDNAF="";
        MTCA_RIS=.;
    run;

    /* Extract from IPFM0024 if exists */
    %if %sysfunc(exist(IPFSPE1.IPFM0024))=1 %then %do;
        PROC SQL;
            CREATE TABLE TABSPEC_CONST_&vision. AS
            select * from TABSPEC_CONST_&vision.
            OUTER UNION CORR
            (select NOPOL, NOINT, CDPROD, "1" AS CDPOLE, CDACTPRF01 AS CDACTCONST, CDACTPRF02 as CDACTCONST2, "" AS CDNAF, . as MTCA_RIS from IPFSPE1.IPFM0024)
            OUTER UNION CORR
            (select NOPOL, NOINT, CDPROD, "3" AS CDPOLE, CDACTPRF01 AS CDACTCONST, CDACTPRF02 as CDACTCONST2, "" AS CDNAF, . as MTCA_RIS from IPFSPE3.IPFM0024);
        QUIT;
    %end;

    /* Extract from IPFM63 if exists */
    %if %sysfunc(exist(IPFSPE1.IPFM63))=1 %then %do;
        PROC SQL;
            CREATE TABLE TABSPEC_CONST_&vision. AS
            select * from TABSPEC_CONST_&vision.
            OUTER UNION CORR
            (select NOPOL, NOINT, CDPROD, "1" AS CDPOLE, ACTPRIN AS CDACTCONST, ACTSEC1 as CDACTCONST2, CDNAF, MTCA1 AS MTCA_RIS from IPFSPE1.IPFM63)
            OUTER UNION CORR
            (select NOPOL, NOINT, CDPROD, "3" AS CDPOLE, ACTPRIN AS CDACTCONST, ACTSEC1 as CDACTCONST2, CDNAF, MTCA1 AS MTCA_RIS from IPFSPE3.IPFM63);
        QUIT;
    %end;

    /* Extract from IPFM99 if exists */
    %if %sysfunc(exist(IPFSPE1.IPFM99))=1 %then %do;
        PROC SQL;
            CREATE TABLE TABSPEC_CONST_&vision. AS
            select * from TABSPEC_CONST_&vision.
            OUTER UNION CORR
            (select NOPOL, NOINT, CDPROD, "1" AS CDPOLE, substr(CDACPR1, 1, 4) AS CDACTCONST, CDACPR2 as CDACTCONST2, "" AS CDNAF, MTCA AS MTCA_RIS from IPFSPE1.IPFM99)
            OUTER UNION CORR
            (select NOPOL, NOINT, CDPROD, "3" AS CDPOLE, substr(CDACPR1, 1, 4) AS CDACTCONST, CDACPR2 as CDACTCONST2, "" AS CDNAF, MTCA AS MTCA_RIS from IPFSPE3.IPFM99);
        QUIT;
    %end;

    /* Clean empty records */
    data TABSPEC_CONST_&vision.;
        set TABSPEC_CONST_&vision.;
        if missing(nopol) then delete;
    run;

    /* ========================================================================
     * STEP 8: JOIN SPECIAL PRODUCT DATA
     * ======================================================================== */
    
    PROC SQL;
        CREATE TABLE CUBE.MVT_PTF&vision. AS
        SELECT 
            t1.*, 
            coalesce(t3.CDACTCONST, t1.ACTPRIN) AS ACTPRIN2, 
            CASE WHEN t3.CDNAF ne "" then t3.CDNAF ELSE t1.CDNAF END AS CDNAF2,
            t3.CDACTCONST2
        FROM CUBE.mvt_ptf&vision. AS T1
        LEFT JOIN TABSPEC_CONST_&vision. t3 ON (t1.NOPOL=t3.NOPOL and t1.CDPROD=t3.CDPROD);
    QUIT;

    data CUBE.MVT_PTF&vision.(DROP=ACTPRIN2 CDNAF2 CDACTCONST2 CDACTCONST);
        set CUBE.MVT_PTF&vision.;
        length TypeAct $10;
        ACTPRIN=ACTPRIN2;
        CDNAF=CDNAF2;
        if not missing(CDACTCONST2) then TypeAct="Multi"; 
        else TypeAct="Mono";
    run;

    /* ========================================================================
     * STEP 9: ADD NAF CODE ENRICHMENT FROM VARIOUS SOURCES
     * ======================================================================== */
    
    %let vue= /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/ALEX_MIS/basecli_cumule;
    x "gunzip &vue./basecli_inv.sas7bdat.gz";

    PROC SQL;
        CREATE TABLE CUBE.MVT_PTF&vision. AS
        SELECT 
            PTF.*, 
            W6.CDAPET as CDNAF08_W6,
            coalesce(C1.CDNAF, C3.CDNAF) AS CDNAF03_CLI
        FROM CUBE.MVT_PTF&vision. PTF
        LEFT JOIN W6.BASECLI_INV W6 ON (PTF.NOCLT= W6.NOCLT)
        LEFT JOIN CLIENT1.CLACENT1 C1 ON (PTF.NOCLT=C1.NOCLT)
        LEFT JOIN CLIENT3.CLACENT3 C3 ON (PTF.NOCLT=C3.NOCLT);
    QUIT;

    x "gzip &vue./basecli_inv.sas7bdat";

    data CUBE.MVT_PTF&vision.;
        set CUBE.MVT_PTF&vision.;
        if CDNAF03_CLI in ("00" "000Z" "9999") then CDNAF03_CLI="";
        if CDNAF08_W6 in ("0000Z") then CDNAF08_W6="";
    run;

    /* ========================================================================
     * STEP 10: APPLY ISIC CONSTRUCTION CODIFICATION
     * ======================================================================== */
    
    %code_isic_construction(CUBE.MVT_PTF&vision., &vision.);

    /* Détermination du code ISIC Global */
    libname ISIC "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/ALEX_MIS/Ref";

    PROC SQL;
        CREATE TABLE CUBE.MVT_PTF&vision. AS
        SELECT 
            t1.*, 
            t2.ISIC_Global as ISIC_CODE_GBL
        FROM CUBE.MVT_PTF&vision. t1
        LEFT JOIN ISIC.ISIC_LG_202306 t2 on ( t2.ISIC_Local=t1.ISIC_CODE );
    QUIT;

    /* ========================================================================
     * STEP 11: APPLY ISIC CODE CORRECTIONS
     * ======================================================================== */
    
    data CUBE.MVT_PTF&vision.;
        set CUBE.MVT_PTF&vision.;
        if ISIC_CODE="22000" then ISIC_CODE_GBL="022000";
        if ISIC_CODE=" 22000" then ISIC_CODE_GBL="022000";
        if ISIC_CODE="24021" then ISIC_CODE_GBL="024000";
        if ISIC_CODE=" 24021" then ISIC_CODE_GBL="024000";
        if ISIC_CODE="242025" then ISIC_CODE_GBL="242005";
        if ISIC_CODE="329020" then ISIC_CODE_GBL="329000";
        if ISIC_CODE="731024" then ISIC_CODE_GBL="731000";
        if ISIC_CODE="81020" then ISIC_CODE_GBL="081000";
        if ISIC_CODE=" 81020" then ISIC_CODE_GBL="081000";
        if ISIC_CODE="81023" then ISIC_CODE_GBL="081000";
        if ISIC_CODE=" 81023" then ISIC_CODE_GBL="081000";
        if ISIC_CODE="981020" then ISIC_CODE_GBL="981000";
    run;

    /* ========================================================================
     * STEP 12: ADD SPECIAL BUSINESS FLAGS
     * ======================================================================== */
    
    data CUBE.MVT_PTF&vision.;
        set CUBE.MVT_PTF&vision.;
        if NOINT="4A5766" then TOP_BERLIOZ=1;
        if NOINT in ("4A6160","4A6947","4A6956") then TOP_PARTENARIAT=1;
    run;
%mend;
