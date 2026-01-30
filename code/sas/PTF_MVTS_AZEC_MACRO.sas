/******************************************************************************
 * MACRO: azec_mvt_ptf
 * 
 * Description: Processes AZEC portfolio and movements data for construction market
 *              Extracts data from AZEC legacy system (POLIC_CU)
 *              Calculates AFN/RES/PTF indicators with specific AZEC business rules
 *              Enriches with capital data (SMP/LCI), NAF codes, and segmentation
 *
 * Parameters:
 *   - vision : Vision to process (YYYYMM format)
 *
 * Input Tables:
 *   - POLIC_CU.POLIC_CU            : AZEC main policy data
 *   - CAPITXCU.CAPITXCU            : AZEC capital data (SMP/LCI)
 *   - INCENDCU.INCENDCU            : Fire insurance data (PE/RD/NAF)
 *   - RCENTCU.RCENTCU              : RC enterprise data
 *   - RISTECCU.RISTECCU            : Professional risk data
 *   - MULPROCU.MULPROCU            : Multi-risk data (CA)
 *   - CONSTRCU.CONSTRCU            : Construction site data
 *   - PT_GEST.PTGST_*              : Management point reference
 *
 * Output:
 *   - AZEC_PTF&vision. : AZEC portfolio/movements data with indicators
 *
 * Logic:
 *   - Excludes migrated contracts (GESTSIT='MIGRAZ')
 *   - Special AFN/RES/PTF calculation rules for AZEC products
 *   - Calculates SMP/LCI from capital data
 *   - Handles tacit renewal and temporary contracts
 ******************************************************************************/

%MACRO azec_mvt_ptf(vision);

    /* ========================================================================
     * DATE PARAMETERS EXTRACTION
     * ======================================================================== */
    
    %let annee = %sysfunc(compress(%substr(&vision., 1, 4)));
    %let mois = %sysfunc(compress(%substr(&vision., 5, 2)));
    %put &annee &mois;
    
    /* Products with specific AFN/RES rules */
    %let produit = ('A00' 'A01' 'AA1' 'AB1' 'AG1' 'AG8' 'AR1' 'ING' 'PG1' 'AA6' 'AG6' 'AR2' 'AU1' 'AA4' 'A03' 'AA3' 'AG3' 'AM3' 'MAI' 'MA0' 'MA1' 'MA2' 'MA3' 'MA4' 'MA5' 'MT0' 'MR0' 'AAC' 'GAV' 'GMC' 'MB1' 'MB2' 'MB3' 'MED' 'MH0' 'MP0' 'MP1' 'MP2' 'MP3' 'MP4' 'MPG' 'MPP' 'MI0' 'MI1' 'MI2' 'MI3' 'PI2');
    
    %local a;
    %let a = %nrstr(%mend);

    /* ========================================================================
     * STEP 1: EXTRACT BASE POLICY DATA WITH DTECHANN CALCULATION
     * ======================================================================== */
    
    PROC SQL;
        /* Calcul des Affaires Nouvelles et des Résiliations */
        CREATE TABLE _0_POLIC_CU2 AS
        SELECT 
            POLICE, DATFIN, DATRESIL, INTERMED, POINGEST, CODECOAS, DATAFN, ETATPOL, 
            DUREE, FINPOL, DATRESIL, DATTERME, DATEXPIR, PRODUIT, EFFETPOL, PRIME, PARTBRUT, 
            CPCUA, NOMCLI, MOTIFRES, ORIGRES, TYPCONTR, RMPLCANT, GESTSIT,
            
            /* Modification 2020.02.12 - Use current year for termination date */
            mdy(echeanmm, echeanjj, &annee.) AS DTECHANN,
            partbrut / 100 AS PARTCIE,
            
            /* Initialize indicators */
            0 AS NBAFN,
            0 AS NBRES,
            0 AS NBPTF,
            0 AS NBAFN_ANTICIPE,
            0 AS NBRES_ANTICIPE,
            (mdy(12, 31, &annee.) - "&dtdebn"d + 1) AS nbj_tot_ytd,
            0 AS nbj_susp_ytd,
            0 AS expo_ytd,
            0 AS expo_gli,
            . AS DT_DEB_EXPO format=ddmmyy10.,
            . AS DT_FIN_EXPO format=ddmmyy10.,
            (CASE WHEN DUREE not in ('00', '01', '', ' ') THEN 1 ELSE 0 END) AS TOP_LTA,
            (CASE WHEN INDREGUL='O' THEN 1 ELSE 0 END) AS TOP_REVISABLE,
            "" AS CRITERE_REVISION,
            "" as CDGREV
        FROM POLIC_CU.POLIC_CU
        WHERE intermed Not in ("24050" "40490")
            And POLICE Not In ("012684940")  /* Codes courtiers fictifs et police fictive */
            AND not(duree in ('00') and produit not in ('DO0','TRC','CTR','CNR'))  /* On exclut les temporaires sauf DO0 */
            AND datfin ne effetpol  /* Contrats sans effet */
            AND (Gestsit not in("MIGRAZ") or (GESTSIT = 'MIGRAZ' and ETATPOL = 'R'));  /* SGE 21/07/2020: Contrats Non Migrés vers IMS */

    /* ========================================================================
     * STEP 2: HANDLE MIGRATED CONTRACTS
     * ======================================================================== */
    
    /* Migration AZEC */
    /* Ajout DR 15/09/20 : Top_ptf car des contrats AZEC migrés ont ETATPOL = R pour une résil future */
    /* 202007 correspond à la date de migration où il faut gérer les cnts topés etatpol = 'R' au lieu de 'X' */
    %If &vision. > 202009 %Then %Do;
        CREATE TABLE POLIC_CU2 AS
        SELECT DISTINCT 
            T1.*, 
            (CASE WHEN t2.NOPOL_AZEC IS missing THEN 1 ELSE 0 END) AS NBPTF_NON_MIGRES_AZEC
        FROM _0_POLIC_CU2 t1
        LEFT JOIN Mig_azec.ref_mig_azec_vs_ims t2 ON (T1.POLICE = t2.NOPOL_AZEC);
    %End;
    %Else %Do;
        CREATE TABLE POLIC_CU2 AS
        SELECT T1.*, 1 AS NBPTF_NON_MIGRES_AZEC
        FROM _0_POLIC_CU2 t1;
    %End;

    /* ========================================================================
     * STEP 3: DATA QUALITY ADJUSTMENTS
     * ======================================================================== */
    
    /* Cf code FG pour expo AZEC : aitihvi/Freq/EXPO_AZEC_TOUTES_LOB */
    UPDATE POLIC_CU2 
    SET datexpir = datfin 
    WHERE (datfin > datexpir and ETATPOL in ("X","R"));
    
    UPDATE POLIC_CU2 
    SET NBAFN_ANTICIPE = 1 
    WHERE (effetpol) > (&finmois.);
    
    UPDATE POLIC_CU2 
    SET NBRES_ANTICIPE = 1 
    WHERE (datfin) > (&finmois.);

    /* Contrats à tacite reconduction en cours non quittancés depuis plus d'un an. 
       On les considère comme résiliés à la date du prochain terme */
    UPDATE POLIC_CU2 
    SET ETATPOL = 'R', DATFIN = DATTERME, DATRESIL = DATTERME 
    WHERE DUREE = '01' 
          And FINPOL=. 
          And DATTERME <> . 
          And DATTERME < Mdy(&mois., 01, &annee. - 1);

    /* Détermination date de fin pour temporaires */
    UPDATE POLIC_CU2 
    SET ETATPOL = 'R', DATFIN=FINPOL, DATRESIL = FINPOL 
    WHERE FINPOL <> . And DATFIN = .;

    /* ========================================================================
     * STEP 4: CALCULATE AFN/RES/PTF INDICATORS
     * ======================================================================== */
    
    /* Création d'un compteur des afn pour certains produits, variable nbafn */
    UPDATE POLIC_CU2 
    SET NBAFN = 1 
    WHERE (ETATPOL = "R" 
           and PRODUIT not in ("CNR", "DO0") 
           and NBPTF_NON_MIGRES_AZEC = 1 
           and ((PRODUIT in &produit. And month(datafn) <= &mois. and year(datafn) = &annee.) 
                or (PRODUIT Not In &produit. 
                    And (((mdy(01,01,&annee.) <= effetpol <="&dtfinmn"d) and (datafn <="&dtfinmn"d)) 
                         Or ((effetpol < mdy(01,01,&annee.)) and (mdy(01,01,&annee.)<= datafn <="&dtfinmn"d))))));

    /* Création d'un compteur des res, variable nbres */
    UPDATE POLIC_CU2 
    SET NBRES = 1 
    WHERE (ETATPOL = "R" 
           and PRODUIT not in ("CNR", "DO0") 
           and NBPTF_NON_MIGRES_AZEC = 1 
           and ((PRODUIT in &produit. And month(datresil) <= &mois. and year(datresil) = &annee.) 
                Or (PRODUIT Not In &produit. 
                    And (((mdy(01,01,&annee.) <= datfin <="&dtfinmn"d) and (datresil <="&dtfinmn"d)) 
                         Or ((datfin <="&dtfinmn"d) and (mdy(01,01,&annee.) <= datresil <="&dtfinmn"d))))));

    /* Le nombre de contrats en ptf : nbptf */
    UPDATE POLIC_CU2 
    SET NBPTF = 1 
    WHERE ((NBPTF_NON_MIGRES_AZEC = 1) 
           and (effetpol<="&dtfinmn"d and datafn <= "&dtfinmn"d) 
           and (datfin=. or datfin>"&dtfinmn"d or datresil > "&dtfinmn"d) 
           and (ETATPOL = 'E' or (ETATPOL = 'R' and DATFIN >= "&dtfinmn"d)) 
           and PRODUIT NOT IN ('DO0','TRC','CTR','CNR'));

    /* Contrats à tacite reconduction en cours non quittancés depuis plus d'un an on les considère comme résiliés à la date du prochain terme */
    UPDATE POLIC_CU2 
    SET ETATPOL = "R", DATFIN = DATTERME, DATRESIL = DATTERME 
    WHERE DUREE = '01' AND FINPOL = . AND DATTERME NE . AND DATTERME LT MDY (&mois., 01, &annee. -1);

    /* Détermination date de fin pour temporaires */
    UPDATE POLIC_CU2 
    SET DATFIN = FINPOL, ETATPOL = 'R', DATRESIL = FINPOL 
    WHERE FINPOL NE . AND DATFIN = .;

    /* ========================================================================
     * STEP 5: CALCULATE EXPOSURE INDICATORS
     * ======================================================================== */
    
    /* Cas des contrats avec une période de suspension : calcul du nbre de jours suspendus durant la période traitée */
    UPDATE POLIC_CU2 
    SET nbj_susp_ytd = (CASE 
        WHEN ("&dtdebn"d <= datresil <="&dtfinmn"d or "&dtdebn"d <= datfin <="&dtfinmn"d) 
            THEN min(datfin,"&dtfinmn"d,datexpir) - max("&dtdebn"d - 1,datresil - 1) 
        WHEN (0 <= datresil <= "&dtdebn"d and datfin >= "&dtfinmn"d) 
            THEN ("&dtfinmn"d - "&dtdebn"d + 1) 
        ELSE 0 
    END);

    /* Calcul de l'exposition */
    UPDATE POLIC_CU2 
    SET expo_ytd = (MAX(0,(MIN(datfin, "&dtfinmn"d) - MAX(EFFETPOL,"&dtdebn"d)) + 1)) / nbj_tot_ytd, 
        expo_gli = (MAX(0,(MIN(datfin, "&dtfinmn"d) - MAX(EFFETPOL, "&dtfinmn1"d + 1)) + 1)) / ("&dtfinmn"d - "&dtfinmn1"d), 
        DT_DEB_EXPO = MAX(EFFETPOL,"&dtdebn"d), 
        DT_FIN_EXPO = MIN(datfin, "&dtfinmn"d);

    /* ========================================================================
     * STEP 6: CALCULATE PREMIUMS AND ENRICH WITH SEGMENTATION
     * ======================================================================== */
    
    /* Ajout des primes ptf des AFN, RES, et PTF -> pour pouvoir calculer les primes moyennes */
    CREATE TABLE PTF_AZEC AS
    SELECT 
        t1.POLICE, t1.DATFIN, t1.DATRESIL, t1.INTERMED, t1.POINGEST, t1.CODECOAS, t1.DATAFN, 
        t1.ETATPOL, t1.DUREE, t1.FINPOL, t1.DATTERME, t1.PRODUIT, t1.EFFETPOL, t1.PRIME, 
        t1.PARTBRUT, t1.CPCUA, t1.NBAFN, t1.NBRES, t1.NBPTF, t1.expo_ytd, t1.expo_gli, t1.NOMCLI, 
        t1.DTECHANN, t1.PARTCIE, t1.MOTIFRES, t1.ORIGRES, t1.TOP_LTA, t2.SEGMENT, t1.RMPLCANT,
        t1.NBAFN_ANTICIPE, t1.NBRES_ANTICIPE,
        (t1.prime * t1.partbrut / 100 + t1.cpcua) AS PRIMECUA,
        t1.DT_DEB_EXPO,
        t1.DT_FIN_EXPO,
        t1.TOP_REVISABLE,
        t1.CRITERE_REVISION,
        t1.CDGREV,
        (CASE WHEN t2.CSSSEG ne "5" THEN (t1.nbafn=1)*(t1.prime*t1.partbrut/100 + t1.cpcua) ELSE 0 END) AS PRIMES_AFN,
        (CASE WHEN t2.CSSSEG ne "5" THEN (t1.nbres=1)*(t1.prime*t1.partbrut/100 + t1.cpcua) ELSE 0 END) AS PRIMES_RES,
        ((t1.nbptf=1)*(t1.prime*t1.partbrut/100 + t1.cpcua)) AS PRIMES_PTF,
        /* Ajout 2020.02.18 */
        (t1.prime*t1.partbrut/100 + t1.cpcua) AS PRIMETO,
        /* Cf Mail Steffi B. 31.07.2019 */
        (CASE WHEN t1.PARTBRUT = 0 THEN t1.PRIME ELSE (t1.PRIME + (t1.CPCUA/t1.PARTCIE)) END) AS Cotis_100,
        /* Cf pgm JC : plus importants contrats AZEC */
        (CASE 
            WHEN t1.codecoas = '0' THEN 'SANS COASSURANCE'
            WHEN t1.codecoas = 'A' THEN 'APERITION'
            /* Modification du 2020.02.14 cf Delphine Petit et AAV mail AAV 2020.02.14 */
            WHEN t1.codecoas = 'C' THEN 'COASS. ACCEPTEE'
            /* Modification du 2020.02.14 cf Delphine Petit et AAV mail AAV 2020.02.14 */
            WHEN t1.TYPCONTR = 'A' And t1.codecoas = 'R' THEN 'REASS. ACCEPTEE'
         END) AS COASS,
        /* Retraitement cdnatp / règle transmise par JF */
        (CASE 
            WHEN (DUREE="00" and CDPROD in ("CNR" "CTR" "DO0")) then "C"
            WHEN (DUREE="00" and CDPROD in ("TRC")) then "T"
            WHEN (DUREE in("01" "02" "03")) then "R"
            else ""
         end) as CDNATP,
        /* Ajout du 2020.02.14 AAV */
        (CASE WHEN t1.codecoas = '0' THEN 0 ELSE 1 END) AS TOP_COASS,
        t1.TYPCONTR AS TYPE_AFFAIRE,
        &vision. AS VISION,
        &annee. AS EXEVUE,
        &mois. AS MOISVUE,
        t2.CMARCH,
        t2.CSEG,
        t2.CSSSEG,
        t2.LMARCH,
        t2.LSEG,
        t2.LSSSEG
    FROM POLIC_CU2 t1
    LEFT JOIN REF.TABLE_SEGMENTATION_AZEC_MML t2 ON (t1.PRODUIT = t2.PRODUIT)
    WHERE t2.CMARCH in ("6") and t1.GESTSIT <> 'MIGRAZ';

    /* On ne récupère pas les contrats construction migrés dans IMS et l'intégralité des mouvements des produits migrés */
    UPDATE PTF_AZEC 
    SET expo_ytd = 0 
    WHERE PRIMES_PTF <= 0;

    /* ========================================================================
     * STEP 7: ENRICH WITH NAF CODES
     * ======================================================================== */
    
    /* Rajouter sitecu et garder uniquement les polices uniques */
    CREATE TABLE _01_CODE_NAF AS
    SELECT DISTINCT 
        (COALESCEC(t1.POLICE,t2.POLICE,t3.POLICE,t4.POLICE)) AS POLICE, 
        (COALESCEC(t1.COD_NAF,t2.cod_naf,t3.COD_NAF,t4.COD_NAF)) AS CODE_NAF
    FROM INCENDCU.INCENDCU t1
    FULL JOIN MPACU.MPACU t2 ON (t1.POLICE = t2.POLICE)
    FULL JOIN RCENTCU.RCENTCU t3 ON (t1.POLICE = t3.POLICE)
    FULL JOIN RISTECCU.RISTECCU t4 ON (t1.POLICE = t4.POLICE);

    CREATE TABLE _02_CODE_NAF AS
    (SELECT t1.POLICE, t1.CODE_NAF 
     FROM _01_CODE_NAF t1 
     GROUP BY t1.POLICE HAVING t1.CODE_NAF = min(t1.CODE_NAF));

    CREATE TABLE CODE_TRE AS
    (SELECT t1.POLICE, t1.COD_TRE 
     FROM INCENDCU.INCENDCU t1 
     GROUP BY t1.POLICE HAVING t1.COD_TRE = min(t1.COD_TRE));

    CREATE TABLE CODE_NAF AS
    SELECT DISTINCT 
        (COALESCEC(t1.POLICE,t2.POLICE)) AS POLICE, 
        t2.CODE_NAF, 
        t1.COD_TRE AS CODE_TRE
    FROM CODE_TRE t1
    FULL JOIN _02_CODE_NAF t2 ON (t1.POLICE = t2.POLICE);

    /* Ajout du code NAF et TRE au PTF_AZEC */
    CREATE TABLE PTF_AZEC2_&vision. AS
    SELECT * FROM PTF_AZEC t1
    LEFT JOIN CODE_NAF t2 ON (t1.POLICE = t2.POLICE);

    /* ========================================================================
     * STEP 8: ADD FORMULAS (NICHES)
     * ======================================================================== */
    
    /* Ajout des formules pour les niches */
    CREATE TABLE FORMULE AS
    (SELECT DISTINCT POLICE, FORMULE, FORMULE2, FORMULE3, FORMULE4 FROM RCENTCU.RCENTCU)
    OUTER UNION CORR
    (SELECT DISTINCT POLICE, FORMULE, FORMULE2, FORMULE3, FORMULE4 FROM RISTECCU.RISTECCU);

    CREATE TABLE FORMULE2 AS
    SELECT DISTINCT POLICE, FORMULE, FORMULE2, FORMULE3, FORMULE4 
    FROM FORMULE 
    GROUP BY police HAVING count(police) = 1;

    CREATE TABLE _00_AZEC_PTF&vision. AS
    SELECT 
        t1.CMARCH, t1.CPCUA, t1.CSEG, t1.CSSSEG, t1.DATAFN, t1.DATFIN, t1.DATRESIL, t1.DATTERME, 
        t1.DTECHANN, t1.MOTIFRES, t1.ORIGRES, t1.DUREE, t1.EFFETPOL, t1.ETATPOL, t1.FINPOL, 
        t1.INTERMED, t1.CODECOAS, t1.COASS, t1.TOP_COASS, t1.TYPE_AFFAIRE, t1.LMARCH, t1.LSEG, 
        t1.LSSSEG, t1.NBAFN, t1.NBPTF, t1.NBRES, t1.PARTBRUT, t1.POINGEST, t1.PRIME, t1.PRIMECUA, 
        t1.PRIMES_AFN, t1.PRIMES_PTF, t1.PRIMES_RES, t1.Cotis_100, t1.PRIMETO, t1.PRODUIT, t1.VISION, 
        t1.EXEVUE, t1.MOISVUE, t1. RMPLCANT, t1.POLICE AS POLICE, 
        t2.FORMULE AS FORMULE, t2.FORMULE2 AS FORMULE2, t2.FORMULE3 AS FORMULE3, t2.FORMULE4 AS FORMULE4, 
        t1.expo_ytd, t1.expo_gli, t1.CODE_NAF, t1.CODE_TRE, t1.NOMCLI, t1.PARTCIE, t1.TOP_LTA, 
        t1.SEGMENT, t1.NBAFN_ANTICIPE, t1.NBRES_ANTICIPE, t1.DT_DEB_EXPO, t1.DT_FIN_EXPO,
        t1.TOP_REVISABLE, t1.CRITERE_REVISION, t1.CDGREV, t1.CDNATP
    FROM PTF_AZEC2_&vision. t1
    LEFT JOIN FORMULE2 t2 ON (t1.POLICE = t2.POLICE);

    /* ========================================================================
     * STEP 9: ADD TURNOVER (CA) DATA
     * ======================================================================== */
    
    /* Jointure pour récupérer le chiffre d'affaire */
    CREATE TABLE police_ca AS
    SELECT POLICE, (SUM(CHIFFAFF)) AS MTCA 
    FROM mulprocu.mulprocu 
    GROUP BY POLICE;

    CREATE TABLE _01_AZEC_PTF&vision. AS
    SELECT 
        t1.CMARCH, t1.CSEG, t1.CSSSEG, t1.LMARCH, t1.LSEG, t1.LSSSEG, t1.DATAFN, t1.DATFIN, 
        t1.DATRESIL, t1.MOTIFRES, t1.ORIGRES, t1.DATTERME, t1.DTECHANN format = ddmmyy6., 
        t1.DUREE, t1.EFFETPOL, t1.ETATPOL, t1.FINPOL, t1.INTERMED, t1.CODECOAS, t1.COASS, 
        t1.TOP_COASS, t1.TYPE_AFFAIRE, t1.NBAFN, t1.NBPTF, t1.NBRES, t1.POINGEST, t1.PRIME, 
        t1.Cotis_100, t1.PRIMETO, t1.PRIMECUA, t1.PRIMES_AFN, t1.PRIMES_PTF, t1.PRIMES_RES, 
        t1.PRODUIT, t1.VISION, t1.EXEVUE, t1.MOISVUE, t1.POLICE, t1.FORMULE, t1.FORMULE2, 
        t1.FORMULE3, t1.FORMULE4, t1.RMPLCANT, t1.expo_ytd, t1.expo_gli, t1.DT_DEB_EXPO, 
        t1.DT_FIN_EXPO, t1.CODE_NAF, t1.CODE_TRE, t1.NOMCLI, t2.MTCA, t1.PARTCIE, t1.CPCUA, 
        t1.PARTBRUT, t1.TOP_LTA, t1.SEGMENT, t1.NBAFN_ANTICIPE, t1.NBRES_ANTICIPE,
        t1.TOP_REVISABLE, t1.CRITERE_REVISION, t1.CDGREV, t1.CDNATP
    FROM _00_AZEC_PTF&vision. t1
    LEFT JOIN police_ca t2 ON (t1.POLICE = t2.POLICE);

    /* ========================================================================
     * STEP 10: ADD CAPITAL DATA (PE, RD, VI)
     * ======================================================================== */
    
    /* Perte d'exploitation + Risque direct = valeur assurée du bien */
    CREATE TABLE PE_RD_VI AS
    SELECT 
        POLICE, PRODUIT, 
        sum(MT_BASPE) as PERTE_EXP, 
        sum(MT_BASDI) as RISQUE_DIRECT, 
        sum(MT_BASDI + MT_BASPE) as VALUE_INSURED
    FROM INCENDCU.INCENDCU 
    GROUP BY POLICE, PRODUIT;
    QUIT;

    data _01_AZEC_PTF&vision.;
        set _01_AZEC_PTF&vision.;
        if EXPO_YTD = 0 then do; 
            DT_DEB_EXPO = .; 
            DT_FIN_EXPO = .; 
        end;
    run;

    /* ========================================================================
     * STEP 11: ADD SMP AND LCI DATA
     * ======================================================================== */
    
    /* Ajout du SMP et de la LCI 2020.02.14 AAV */
    data _00_CAPITAUX_AZEC;
        set CAPITXCU.CAPITXCU;
        if smp_sre = "LCI" and brch_rea = "IP0" then do; LCI_PE_100 = capx_100; LCI_PE_CIE = capx_cua; end;
        if smp_sre = "LCI" and brch_rea = "ID0" then do; LCI_DD_100 = capx_100; LCI_DD_CIE = capx_cua; end;
        if LCI_PE_100=. then LCI_PE_100=0;
        if LCI_PE_CIE=. then LCI_PE_CIE=0;
        if LCI_DD_100=. then LCI_DD_100=0;
        if LCI_DD_CIE=. then LCI_DD_CIE=0;
        /* LCI Globale */
        LCI_100 = LCI_PE_100 + LCI_DD_100;
        LCI_CIE = LCI_PE_CIE + LCI_DD_CIE;

        if smp_sre = "SMP" and brch_rea = "IP0" then do; SMP_PE_100 = capx_100; SMP_PE_CIE = capx_cua; end;
        if smp_sre = "SMP" and brch_rea = "ID0" then do; SMP_DD_100 = capx_100; SMP_DD_CIE = capx_cua; end;
        if SMP_PE_100=. then SMP_PE_100=0;
        if SMP_PE_CIE=. then SMP_PE_CIE=0;
        if SMP_DD_100=. then SMP_DD_100=0;
        if SMP_DD_CIE=. then SMP_DD_CIE=0;
        /* SMP Global */
        SMP_100 = SMP_PE_100 + SMP_DD_100;
        SMP_CIE = SMP_PE_CIE + SMP_DD_CIE;
        keep POLICE PRODUIT SMP_100 SMP_CIE LCI_100 LCI_CIE;
    run;

    PROC SQL;
        CREATE TABLE _01_CAPITAUX_AZEC AS
        SELECT 
            POLICE, PRODUIT, 
            SUM(SMP_100) AS SMP_100, 
            SUM(SMP_CIE) AS SMP_CIE, 
            SUM(LCI_100) AS LCI_100, 
            SUM(LCI_CIE) AS LCI_CIE
        FROM _00_CAPITAUX_AZEC 
        GROUP BY POLICE, PRODUIT;

    /* ========================================================================
     * STEP 12: FINAL ASSEMBLY - ADD ALL ENRICHMENT DATA
     * ======================================================================== */
    
    /* Ajout du libellé Code_NAF et valeur assurée */
    CREATE TABLE AZEC_PTF&vision. AS
    SELECT 
        t1.CMARCH, t1.CSEG, t1.CSSSEG, t1.LMARCH, t1.LSEG, t1.LSSSEG, t1.DATAFN, t1.DATFIN, 
        t1.DATRESIL, t1.MOTIFRES, t1.ORIGRES, t1.DATTERME, t1.DTECHANN format = ddmmyy6., t1.DUREE, 
        t1.EFFETPOL, t1.ETATPOL, t1.FINPOL, t1.INTERMED, t1.CODECOAS, t1.COASS, t1.TOP_COASS, 
        t1.TYPE_AFFAIRE, t1.NBAFN, t1.NBPTF, t1.NBRES, t1.POINGEST, t1.PRIME, t1.Cotis_100, t1.PRIMETO, 
        t1.PRIMECUA, t1.PRIMES_AFN, t1.PRIMES_PTF, t1.PRIMES_RES, t1.PRODUIT, t1.VISION, t1.EXEVUE, 
        t1.MOISVUE, t1.POLICE, t1.FORMULE, t1.FORMULE2, t1.FORMULE3, t1.FORMULE4, t1.expo_ytd, t1.expo_gli,
        t1.CODE_NAF, t1.CODE_TRE, t1.NOMCLI, t1.MTCA, t1.PARTCIE, t1.CPCUA, t1.PARTBRUT, t1.DT_DEB_EXPO,
        t1.DT_FIN_EXPO, t1.TOP_LTA, t2.PERTE_EXP, t2.RISQUE_DIRECT, t2.VALUE_INSURED, t4.SMP_100, 
        t4.LCI_100, t1.SEGMENT, t1.RMPLCANT, t1.NBAFN_ANTICIPE, t1.NBRES_ANTICIPE, t1.TOP_REVISABLE,
        t1.CRITERE_REVISION, t1.CDGREV, t1.CDNATP
    FROM _01_AZEC_PTF&vision. t1
    LEFT JOIN PE_RD_VI t2 ON (t1.POLICE = t2.POLICE AND t1.PRODUIT = t2.PRODUIT)
    LEFT JOIN _01_CAPITAUX_AZEC t4 ON (t1.POLICE = t4.POLICE AND t1.PRODUIT = t4.PRODUIT);
    QUIT;

    /* ========================================================================
     * STEP 13: APPLY BUSINESS RULE ADJUSTMENTS
     * =================================================================== ===== */
    
    /* SG: Ajustement des NBRES AZEC */
    DATA AZEC_PTF&vision.;
        SET AZEC_PTF&vision.;
        
        /* Exclude DO0, TRC, CTR, CNR from PTF and RES counts */
        IF PRODUIT IN ('DO0','TRC','CTR', 'CNR') then do;
            NBPTF = 0;
            NBRES = 0;
        end;
        
        /* Exclude replacements from RES */
        IF NBRES = 1 AND RMPLCANT NOT IN ('') AND MOTIFRES in ('RP') THEN NBRES = 0;
        
        /* Exclude specific termination reasons from RES */
        IF NBRES = 1 AND MOTIFRES in ('SE','SA') THEN NBRES = 0;
        
        /* Exclude CSSSEG='5' from AFN */
        IF NBAFN = 1 and CSSSEG = "5" THEN NBAFN = 0;
    RUN;

    %derniere_version(PT_GEST, ptgst_, &vision., TABLE_PT_GEST);

    /* ========================================================================
     * STEP 14: FINAL ENRICHMENT - SEGMENT AND CONSTRUCTION DATA
     * ======================================================================== */
    
    /* Ajout des Segment et type_produit_2 */
    proc sql;
        create table AZEC_PTF&vision. as
        select distinct 
            a.*, 
            b.segment as segment2, 
            b.type_produit as type_produit_2,
            c.DATOUVCH, c.LDESTLOC, c.MNT_GLOB, c.DATRECEP, c.DEST_LOC, c.DATFINCH, c.LQUALITE,
            d.UPPER_MID
        from AZEC_PTF&vision. as a
        left join CONSTRCU_AZEC as b on a.produit = b.cdprod and a.police = b.police
        left join CONSTRCU.CONSTRCU as c on a.produit = c.produit and a.police = c.police
        left join TABLE_PT_GEST as d ON a.POINGEST = d.PTGST
        order by police;
    quit;
%MEND;
