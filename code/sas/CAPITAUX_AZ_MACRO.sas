/******************************************************************************
 * MACRO: az_capitaux
 * 
 * Description: Processes AZ capital data from portfolio (PTF) including:
 *              - SMP (Sinistre Maximum Possible / Maximum Possible Loss)
 *              - LCI (Limite Contractuelle d'Indemnité / Contract Indemnity Limit)
 *              - Perte d'Exploitation (Loss of Operating Income)
 *              - Risque Direct (Direct Risk)
 *              - Limite RC (Civil Liability Limits)
 *              Processes data with and without indexation
 *
 * Parameters:
 *   - annee : Year for processing (YYYY format)
 *   - mois  : Month for processing (MM format)
 *
 * Input Tables:
 *   - PTF16.IPF                  : Portfolio data (Agent - CDPOLE = 1)
 *   - PTF36.IPF                  : Portfolio data (Courtage - CDPOLE = 3)
 *   - SEG.segmentprdt_&vision.   : Product segmentation reference
 *
 * Output: 
 *   - AZ_CAPITAUX_&vision.       : Processed and segmented AZ capital data at 100%
 ******************************************************************************/

%macro az_capitaux(annee, mois);

    %local a;
    %let a = %nrstr(%mend);

    /* ========================================================================
     * PORTFOLIO FILTERS DEFINITION
     * ======================================================================== */
    
    %let filtres_ptf = 
        cdri not in ("X") 
        and cdsitp not in ("5") 
        and noint not in ('H90061','482001','489090','102030','H90036','H90059','H99045','H99059',
                          '5B2000','446000','5R0001','446118','4F1004','4A1400','4A1500','4A1600',
                          '4A1700','4A1800','4A1900','482001','489090','4F1004','4L1010') 
        and cdprod not in ('01073') 
        and cdnatp in('R','O','T') 
        and cmarch = "6" 
        and csegt = "2";

    /* ========================================================================
     * PORTFOLIO VARIABLE SELECTION
     * ======================================================================== */
    
    %let VAR_PTF = 
        cdprod, nopol, nmclt, noint, dtcrepol, cdnatp, txcede, ptgst, dtresilp, 
        cmarch, csegt AS CSEG, cssegt AS CSSSEG, cdsitp, mtprprto, prcdcie, 
        cdgecent, DTEFSITT, Cdmotres, CDCASRES, DTECHANN, CDTPCOA AS CDCOAS, 
        (CASE WHEN txcede = . THEN 0 ELSE txcede END) AS tx, 
        0 AS primeto, 
        0 AS Primes_PTF, 
        0 AS NBPTF,
        0 AS TOP_TEMP, 
        (CASE 
            WHEN cdpolqpl <> '1' THEN 1                 /* SANS COASSURANCE */
            WHEN cdpolqpl = '1' THEN (PRCDCIE / 100)    /* AVEC COASSURANCE */
         END) AS PARTCIE, 
        MTCAPI1,MTCAPI2,MTCAPI3,MTCAPI4,MTCAPI5,MTCAPI6,MTCAPI7,MTCAPI8,MTCAPI9,MTCAPI10,
        MTCAPI11,MTCAPI12,MTCAPI13,MTCAPI14, 
        LBCAPI1,LBCAPI2,LBCAPI3,LBCAPI4,LBCAPI5,LBCAPI6,LBCAPI7,LBCAPI8,LBCAPI9,LBCAPI10,
        LBCAPI11,LBCAPI12,LBCAPI13,LBCAPI14, 
        CDPRVB1,CDPRVB2,CDPRVB3,CDPRVB4,CDPRVB5,CDPRVB6,CDPRVB7,CDPRVB8,CDPRVB9,CDPRVB10,
        CDPRVB11,CDPRVB12,CDPRVB13,CDPRVB14, 
        /* Coefficients d'évolution -> indice de la 1ère année */
        PRPRVC1,PRPRVC2,PRPRVC3,PRPRVC4,PRPRVC5,PRPRVC6,PRPRVC7,PRPRVC8,PRPRVC9,PRPRVC10,
        PRPRVC11,PRPRVC12,PRPRVC13,PRPRVC14,
        0 as VALUE_INSURED,
        0 as LIMITE_RC_100, 
        0 AS PERTE_EXP_100_IND, 
        0 AS RISQUE_DIRECT_100_IND, 
        0 AS SMP_100_IND, 
        0 AS LCI_100_IND, 
        0 AS LIMITE_RC_100_PAR_SIN, 
        0 AS LIMITE_RC_100_PAR_AN,
        0 AS SMP_PE_100_IND, 
        0 AS SMP_RD_100_IND, 
        0 AS PERTE_EXP_100, 
        0 AS RISQUE_DIRECT_100, 
        0 AS SMP_100, 
        0 AS LCI_100, 
        0 AS SMP_PE_100, 
        0 AS SMP_RD_100;

    /* ========================================================================
     * STEP 1: EXTRACT PORTFOLIO DATA (AGENT + COURTAGE)
     * ======================================================================== */
    
    PROC SQL;
        CREATE TABLE PTF_CONST_SMP_LCI&vision. AS 
        (
            /* Agent Portfolio (CDPOLE = 1) */
            SELECT 
                &VAR_PTF., 
                "1" AS CDPOLE, 
                &vision. AS VISION,
                &annee. AS EXEVUE, 
                &mois. AS MOISVUE
            FROM PTF16.IPF t1
            WHERE &filtres_ptf.
        )
        OUTER UNION CORR 
        (
            /* Courtage Portfolio (CDPOLE = 3) */
            SELECT 
                &VAR_PTF., 
                "3" AS CDPOLE, 
                &vision. AS VISION,
                &annee. AS EXEVUE, 
                &mois. AS MOISVUE
            FROM PTF36.IPF t1
            WHERE &filtres_ptf.
        );
    QUIT;

    /* ========================================================================
     * STEP 2: CAPITAL CALCULATIONS WITH INDEXATION
     * ======================================================================== */
    
    data _00_PTF_CONST_SMP_LCI&vision;
        set PTF_CONST_SMP_LCI&vision;
        
        %Do k = 1 %To 14;
            %indexation_v2(date=&FINMOIS.,ind=&k.);
            
            /* Calcul de la perte d'exploitation, du risque direct i.e. de la valeur assurée, 
               du SMP et de la LCI (définition du gareat) */
            /* AAV 05.07.2020 on utilise les MTCAPI&k.I (indicés) */
            /* Cf Gareat Entreprises & Garages IRD IMS.SAS Steffi Boussemart */
            
            /* --- LCI Global du Contrat --- */
            If (index(lbcapi&k.,"LCI GLOBAL DU CONTRAT") > 0 
                or index(lbcapi&k.,"CAPITAL REFERENCE OU LCI") > 0 
                or index(lbcapi&k.,"LCI IRD (DOM. DIRECTS+P.EXPLOIT)") > 0 
                or index(lbcapi&k.,"LIMITE CONTRACTUELLE INDEMNITE") > 0) 
            Then 
                If mtcapi&k.i > LCI_100_IND Then LCI_100_IND = mtcapi&k.i;
            
            /* --- LIMITES RC (Responsabilité Civile) --- */
            /* Modification JCB le 22/10/2020 sur les limites RC */
            
            /* RC par sinistre */
            IF (index(lbcapi&k.,"DOMMAGES CORPORELS") > 0 
                or index(lbcapi&k.,"DOMM. MAT/IMMAT CONSEC EN COURS") > 0 
                or index(lbcapi&k.,"TOUS DOMMAGES CONFONDUS") > 0) 
            Then 
                If mtcapi&k.i > LIMITE_RC_100_PAR_SIN Then LIMITE_RC_100_PAR_SIN = mtcapi&k.i;
            
            /* RC par an */
            IF (index(lbcapi&k.,"TOUS DOMMAGES CONFONDUS (AL)") > 0 
                or index(lbcapi&k.,"TOUS DOMMAGES CONFONDUS / ANN)") > 0 
                or index(lbcapi&k.,"DOM MAT/IMMAT CONSEC (RC AL)") > 0 
                or index(lbcapi&k.,"RCP TOUS DOM.CONFONDUS") > 0) 
            Then 
                If mtcapi&k.i > LIMITE_RC_100_PAR_AN Then LIMITE_RC_100_PAR_AN = mtcapi&k.i;
            
            /* --- SMP Global du Contrat --- */
            If (index(lbcapi&k.,"SMP GLOBAL DU CONTRAT") > 0 
                or index(lbcapi&k.,"SMP RETENU") > 0 
                or index(lbcapi&k.,"SINISTRE MAXIMUM POSSIBLE") > 0) 
            Then 
                If mtcapi&k.i > SMP_100_IND Then SMP_100_IND = mtcapi&k.i;
            
            /* --- RISQUE DIRECT --- */
            If ((index(lbcapi&k.,"RISQUE DIRECT") > 0 and lbcapi&k. not in ("SINIS MAX POSSIBLE RISQUE DIRECT")) 
                or index(lbcapi&k.,"CAPITAUX DOMMAGES DIR") > 0) 
            Then 
                If mtcapi&k.i > RISQUE_DIRECT_100_IND Then RISQUE_DIRECT_100_IND = mtcapi&k.i;
            
            /* --- Perte d'Exploitation --- */
            If (index(lbcapi&k., "PERTE EXPLOITATION (MARGE BRUTE)") > 0 
                or index(lbcapi&k., "PERTE D EXPLOITATION") > 0 
                or index(lbcapi&k., "PERTE D'EXPLOITATION") > 0 
                or index(lbcapi&k., "CAPITAL PERTES EXPLOITATION") > 0 
                or index(lbcapi&k., "CAPITAUX TOTAUX P.E.") > 0 
                or index(lbcapi&k., "PERTE EXPLOITATION/AUT. DOM. MAT") > 0  /* Ajout 2020.02.18 AAV */
                or index(lbcapi&k., "PERTES D'EXPLOITATION") > 0 
                or index(lbcapi&k., "PERTES EXPLOITATION") > 0) 
            Then 
                If mtcapi&k.i > PERTE_EXP_100_IND Then PERTE_EXP_100_IND = mtcapi&k.i;
            
            /* --- LCI/SMP Perte d'Exploitation --- */
            If index(lbcapi&k., "LCI/SMP PERTE EXPLOITATION") > 0 
            Then 
                SMP_PE_100_IND = mtcapi&k.i;
            
            /* --- SMP Risque Direct --- */
            If index(lbcapi&k., "SINIS MAX POSSIBLE RISQUE DIRECT") > 0 
            Then 
                SMP_RD_100_IND = mtcapi&k.i;
        %end;
    run;

    /* ========================================================================
     * STEP 3: CAPITAL CALCULATIONS WITHOUT INDEXATION
     * ======================================================================== */
    
    data _01_PTF_CONST_SMP_LCI&vision.;
        set _00_PTF_CONST_SMP_LCI&vision.;
        
        %Do k = 1 %To 14;
            %indexation_v2(date=&FINMOIS.,ind=&k.);
            
            /* Calcul de la perte d'exploitation, du risque direct i.e. de la valeur assurée, 
               du SMP et de la LCI (définition du gareat) */
            /* AAV 05.07.2020 on utilise les MTCAPI&k.I (indicés) */
            /* Cf Gareat Entreprises & Garages IRD IMS.SAS Steffi Boussemart */
            
            /* --- LCI Global du Contrat --- */
            If (index(lbcapi&k.,"LCI GLOBAL DU CONTRAT") > 0 
                or index(lbcapi&k.,"CAPITAL REFERENCE OU LCI") > 0 
                or index(lbcapi&k.,"LCI IRD (DOM. DIRECTS+P.EXPLOIT)") > 0 
                or index(lbcapi&k.,"LIMITE CONTRACTUELLE INDEMNITE") > 0) 
            Then 
                If mtcapi&k. > LCI_100 Then LCI_100 = mtcapi&k.;
            
            /* --- SMP Global du Contrat --- */
            If (index(lbcapi&k.,"SMP GLOBAL DU CONTRAT") > 0 
                or index(lbcapi&k.,"SMP RETENU") > 0 
                or index(lbcapi&k.,"SINISTRE MAXIMUM POSSIBLE") > 0) 
            Then 
                If mtcapi&k. > SMP_100 Then SMP_100 = mtcapi&k.;
            
            /* --- RISQUE DIRECT --- */
            If ((index(lbcapi&k.,"RISQUE DIRECT") > 0 and lbcapi&k. not in ("SINIS MAX POSSIBLE RISQUE DIRECT")) 
                or index(lbcapi&k.,"CAPITAUX DOMMAGES DIR") > 0) 
            Then 
                If mtcapi&k. > RISQUE_DIRECT_100 Then RISQUE_DIRECT_100 = mtcapi&k.;
            
            /* --- Perte d'Exploitation --- */
            If (index(lbcapi&k., "PERTE EXPLOITATION (MARGE BRUTE)") > 0 
                or index(lbcapi&k., "PERTE D EXPLOITATION") > 0 
                or index(lbcapi&k., "PERTE D'EXPLOITATION") > 0 
                or index(lbcapi&k., "CAPITAL PERTES EXPLOITATION") > 0 
                or index(lbcapi&k., "CAPITAUX TOTAUX P.E.") > 0 
                or index(lbcapi&k., "PERTE EXPLOITATION/AUT. DOM. MAT") > 0  /* Ajout 2020.02.18 AAV */
                or index(lbcapi&k., "PERTES D'EXPLOITATION") > 0 
                or index(lbcapi&k., "PERTES EXPLOITATION") > 0) 
            Then 
                If mtcapi&k. > PERTE_EXP_100 Then PERTE_EXP_100 = mtcapi&k.;
            
            /* --- LCI/SMP Perte d'Exploitation --- */
            If index(lbcapi&k., "LCI/SMP PERTE EXPLOITATION") > 0 
            Then 
                SMP_PE_100 = mtcapi&k.;
            
            /* --- SMP Risque Direct --- */
            If index(lbcapi&k., "SINIS MAX POSSIBLE RISQUE DIRECT") > 0 
            Then 
                SMP_RD_100 = mtcapi&k.;
        %end;
    run;

    /* ========================================================================
     * STEP 4: DATA NORMALIZATION AND RECALCULATION TO 100%
     * ======================================================================== */
    
    proc sql;
        /* Set missing coinsurance percentages to 100% */
        UPDATE _01_PTF_CONST_SMP_LCI&vision.
        SET PRCDCIE = 100
        WHERE PRCDCIE = . or PRCDCIE = 0;
        
        /* Recalculate all capital values to 100% basis (cotisation technique à 100%) */
        /* Cf Steffi Boussemart code du GAREAT */
        UPDATE _01_PTF_CONST_SMP_LCI&vision.
        SET 
            PERTE_EXP_100_IND = (PERTE_EXP_100_IND * 100) / prcdcie,
            RISQUE_DIRECT_100_IND = (RISQUE_DIRECT_100_IND * 100) / prcdcie,
            VALUE_INSURED = (VALUE_INSURED * 100) / prcdcie,
            SMP_100_IND = (SMP_100_IND * 100) / prcdcie,
            LCI_100_IND = (LCI_100_IND * 100) / prcdcie,
            LIMITE_RC_100_PAR_SIN = (LIMITE_RC_100_PAR_SIN * 100) / prcdcie,
            LIMITE_RC_100_PAR_AN = (LIMITE_RC_100_PAR_AN * 100) / prcdcie,
            LIMITE_RC_100 = (LIMITE_RC_100 * 100) / prcdcie,
            SMP_PE_100_IND = (SMP_PE_100_IND * 100) / prcdcie,
            SMP_RD_100_IND = (SMP_RD_100_IND * 100) / prcdcie,
            PERTE_EXP_100 = (PERTE_EXP_100 * 100) / prcdcie,
            RISQUE_DIRECT_100 = (RISQUE_DIRECT_100 * 100) / prcdcie,
            SMP_100 = (SMP_100 * 100) / prcdcie,
            LCI_100 = (LCI_100 * 100) / prcdcie,
            SMP_PE_100 = (SMP_PE_100 * 100) / prcdcie,
            SMP_RD_100 = (SMP_RD_100 * 100) / prcdcie;
        
        /* Apply business rules: 16.11.2020 nouvelle règle pour compléter le SMP */
        UPDATE _01_PTF_CONST_SMP_LCI&vision.
        SET 
            SMP_100_IND = MAX(SMP_100_IND, SUM(SMP_PE_100_IND, SMP_RD_100_IND)),
            SMP_100 = MAX(SMP_100, SUM(SMP_PE_100, SMP_RD_100)),
            LIMITE_RC_100 = MAX(LIMITE_RC_100_PAR_SIN, LIMITE_RC_100_PAR_AN);
    quit;

    /* ========================================================================
     * STEP 5: ENRICH WITH SEGMENTATION DATA
     * ======================================================================== */
    
    proc sql;
        CREATE TABLE AZ_CAPITAUX_&vision. AS
        SELECT 
            t1.*,
            t2.CMARCH,
            t2.CSEG,
            t2.CSSSEG
        FROM _01_PTF_CONST_SMP_LCI&vision. t1
        LEFT JOIN SEG.segmentprdt_&vision. t2 
            ON (t1.CDPOLE = t2.CDPOLE AND t1.CDPROD = t2.CPROD);
    quit;

%mend;
