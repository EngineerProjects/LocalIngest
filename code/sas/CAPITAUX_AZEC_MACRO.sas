/******************************************************************************
 * MACRO: azec_capitaux
 * 
 * Description: Processes AZEC capital data including SMP (Sum Insured Maximum) 
 *              and LCI (Loss Limitation Index) calculations, enriches data with
 *              segmentation and filters for construction market
 *
 * Parameters:
 *   - annee : Year for processing (YYYY format)
 *   - mois  : Month for processing (MM format)
 *
 * Input Tables:
 *   - CAPITXCU.CAPITXCU      : Capital data
 *   - INCENDCU.INCENDCU      : Fire insurance data (Perte d'Exploitation & Risque Direct)
 *   - SEG.segmentprdt_&vision. : Product segmentation reference
 *
 * Output: 
 *   - AZEC_CAPITAUX_&vision. : Processed and segmented AZEC capital data
 ******************************************************************************/

%MACRO azec_capitaux(annee, mois);

    %local a;
    %let a = %nrstr(%mend);

    /* ========================================================================
     * STEP 1: PROCESS SMP (Sum Insured Maximum) and LCI (Loss Coverage Index)
     * ======================================================================== */
    
    data _00_CAPITAUX_AZEC;
        set CAPITXCU.CAPITXCU;
        
        /* --- LCI Processing --- */
        /* LCI Perte d'Exploitation (IP0) */
        if smp_sre = "LCI" and brch_rea = "IP0" then do;
            LCI_PE_100 = capx_100;
            LCI_PE_CIE = capx_cua;
        end;
        
        /* LCI Dommages Directs (ID0) */
        if smp_sre = "LCI" and brch_rea = "ID0" then do;
            LCI_DD_100 = capx_100;
            LCI_DD_CIE = capx_cua;
        end;
        
        /* Initialize missing values to zero */
        if LCI_PE_100 = . then LCI_PE_100 = 0;
        if LCI_PE_CIE = . then LCI_PE_CIE = 0;
        if LCI_DD_100 = . then LCI_DD_100 = 0;
        if LCI_DD_CIE = . then LCI_DD_CIE = 0;
        
        /* LCI Globale (Total) */
        LCI_100 = LCI_PE_100 + LCI_DD_100;
        LCI_CIE = LCI_PE_CIE + LCI_DD_CIE;
        
        /* --- SMP Processing --- */
        /* SMP Perte d'Exploitation (IP0) */
        if smp_sre = "SMP" and brch_rea = "IP0" then do;
            SMP_PE_100 = capx_100;
            SMP_PE_CIE = capx_cua;
        end;
        
        /* SMP Dommages Directs (ID0) */
        if smp_sre = "SMP" and brch_rea = "ID0" then do;
            SMP_DD_100 = capx_100;
            SMP_DD_CIE = capx_cua;
        end;
        
        /* Initialize missing values to zero */
        if SMP_PE_100 = . then SMP_PE_100 = 0;
        if SMP_PE_CIE = . then SMP_PE_CIE = 0;
        if SMP_DD_100 = . then SMP_DD_100 = 0;
        if SMP_DD_CIE = . then SMP_DD_CIE = 0;
        
        /* SMP Global (Total) */
        SMP_100 = SMP_PE_100 + SMP_DD_100;
        SMP_CIE = SMP_PE_CIE + SMP_DD_CIE;
        
        keep POLICE PRODUIT SMP_100 SMP_CIE LCI_100 LCI_CIE;
    run;

    /* ========================================================================
     * STEP 2: AGGREGATE CAPITAL DATA BY POLICY AND PRODUCT
     * ======================================================================== */
    
    PROC SQL;
        CREATE TABLE AZEC_CAPITAUX_&vision. AS
        SELECT 
            POLICE as NOPOL,
            PRODUIT as CDPROD,
            SUM(SMP_100) AS SMP_100_IND,
            SUM(SMP_CIE) AS SMP_CIE,
            SUM(LCI_100) AS LCI_100_IND,
            SUM(LCI_CIE) AS LCI_CIE
        FROM _00_CAPITAUX_AZEC
        GROUP BY POLICE, PRODUIT;
    quit;

    /* ========================================================================
     * STEP 3: CALCULATE PERTE D'EXPLOITATION & RISQUE DIRECT
     * ======================================================================== */
    
    proc sql;
        CREATE TABLE PE_RD_VI AS
        SELECT 
            POLICE,
            PRODUIT,
            sum(MT_BASPE) AS PERTE_EXP_100_IND,
            sum(MT_BASDI) AS RISQUE_DIRECT_100_IND,
            sum(MT_BASDI + MT_BASPE) AS VALUE_INSURED_100_IND
        FROM INCENDCU.INCENDCU
        GROUP BY POLICE, PRODUIT;
    quit;

    /* ========================================================================
     * STEP 4: ENRICH WITH SEGMENTATION AND PERTE EXPLOITATION DATA
     * ======================================================================== */
    
    proc sql;
        CREATE TABLE AZEC_CAPITAUX_&vision. AS
        SELECT 
            t1.*,
            t2.CMARCH,
            t2.CSEG,
            t2.CSSSEG,
            t3.PERTE_EXP_100_IND,
            t3.RISQUE_DIRECT_100_IND,
            t3.VALUE_INSURED_100_IND
        FROM AZEC_CAPITAUX_&vision. t1
        LEFT JOIN SEG.segmentprdt_&vision. t2 
            ON (t1.CDPROD = t2.CPROD)
        LEFT JOIN PE_RD_VI t3 
            on t1.nopol = t3.police 
            and t1.cdprod = t3.produit;
    quit;

    /* ========================================================================
     * STEP 5: FILTER FOR CONSTRUCTION MARKET (CMARCH = "6")
     * ======================================================================== */
    
    proc sql;
        CREATE TABLE AZEC_CAPITAUX_&vision. AS
        SELECT * 
        FROM AZEC_CAPITAUX_&vision.
        WHERE CMARCH = "6";
    quit;

%MEND;
