/******************************************************************************
 * MACRO: az_mvt_ptf
 * 
 * Description: Processes AZ portfolio and movements data for construction market
 *              Extracts data from PTF16 (Agent) and PTF36 (Courtage)
 *              Calculates capital amounts, AFN/RES indicators, exposure indicators
 *              Enriches with segmentation and product type data
 *
 * Parameters:
 *   - annee : Year for processing (YYYY format)
 *   - mois  : Month for processing (MM format)
 *
 * Input Tables:
 *   - PTF16.IPF / PTF36.IPF           : Portfolio data (Agent/Courtage)
 *   - PTF16a.IPFM99 / PTF36a.IPFM99  : Specific CA data for product 01099
 *   - SEGMprdt.PRDPFA1 / PRDPFA3     : Segmentation reference data
 *   - PRDCAP.PRDCAP                   : Product catalog
 *   - PT_GEST.PTGST_*                 : Management point reference
 *
 * Output:
 *   - MVT_CONST_PTF&vision. : Portfolio/movements data with indicators
 *
 * Logic:
 *   - Filters construction market (CMARCH=6, CSEG=2)
 *   - Calculates AFN, RES, PTF indicators
 *   - Processes capital fields (LCI, SMP, PE, RD)
 *   - Calculates exposure (YTD and GLI)
 *   - Enriches with segmentation
 ******************************************************************************/

%MACRO az_mvt_ptf(annee, mois);
    %local a;
    %let a = %nrstr(%mend);

    /* ========================================================================
     * FILTER DEFINITIONS - CONSTRUCTION MARKET
     * ======================================================================== */
    
    /* Création des indicateurs Mouvements et PTF Construction */
    %let filtres_ptf = cdri not in ("X") 
        and cdsitp not in ("4","5") 
        and noint not in ('H90061','482001','489090','102030','H90036','H90059','H99045','H99059',
                          '5B2000','446000','5R0001','446118','4F1004','4A1400','4A1500','4A1600',
                          '4A1700','4A1800','4A1900','482001','489090','4F1004','4L1010') 
        and cdprod not in ('01073') 
        and cdnatp in('R','O','T','C') 
        and cmarch= "6" 
        and csegt="2";

    /* ========================================================================
     * VARIABLE SELECTION
     * ======================================================================== */
    
    %let VAR_PTF = 
        cdprod, nopol, NOCLT, nmclt, posacta AS posacta_ri, rueacta AS rueacta_ri, cediacta AS cediacta_ri, 
        NMACTA, noint, dtcrepol, cdnatp, txcede, ptgst, dteffan, dttraan, dtresilp, dttraar, 
        cdtypli1, cdtypli2, cdtypli3, dttypli1, dttypli2, dttypli3, cmarch, cdsitp, mtprprto, prcdcie, 
        csegt AS CSEG, cssegt AS CSSSEG, cdri, CDCIEORI, CDSITMGR, cdgecent, Cdmotres, cdpolrvi, 
        nopolli1, CDCASRES, DTECHANN, CDTPCOA AS CDCOAS, PRCDCIE AS PRCIE,
        (CASE WHEN txcede = . THEN 0 ELSE txcede END) AS tx,
        
        /* Coassurance indicators - 2020.02.11 cf mail Alain Cadinot */
        (CASE WHEN cdpolqpl = '1' THEN 1 ELSE 0 END) AS TOP_COASS,
        (CASE 
            WHEN cdpolqpl = '1' AND CDTPCOA in ('3', '6') THEN 'APERITION'
            WHEN cdpolqpl = '1' AND CDTPCOA in ('4', '5') THEN 'COASS. ACCEPTEE'
            WHEN cdpolqpl = '1' AND CDTPCOA in ('8') THEN 'ACCEPTATION INTERNATIONALE'
            WHEN cdpolqpl = '1' AND CDTPCOA not in ('3' '4' '5' '6' '8') THEN 'AUTRES'
            WHEN cdpolqpl <> '1' THEN 'SANS COASSURANCE'
         END) AS COASS,
        (CASE 
            WHEN cdpolqpl <> '1' THEN 1                 /* SANS COASSURANCE */
            WHEN cdpolqpl = '1' THEN (PRCDCIE / 100)    /* AVEC COASSURANCE */
         END) AS PARTCIE,
        
        /* Revision indicators - 26.06.20 */
        (CASE WHEN CDPOLRVI= "1" THEN 1 ELSE 0 END) AS TOP_REVISABLE,
        (CASE 
            /* JFT 13/12/2021 : Nouveaux libellés de critère de révision */
            WHEN CDGREV ="20" THEN "20_SANS_PROV_MANUEL "
            WHEN CDGREV ="21" THEN "21_PROV_FIXE_MANUEL "
            WHEN CDGREV ="22" THEN "22_PROV_VAR_MANUEL "
            WHEN CDGREV ="23" THEN "23_SANS_REVISION "
            WHEN CDGREV ="24" THEN "24_REV_FIN_CHANTIER "
            WHEN CDGREV ="30" THEN "30_SANS_PROV_MANUEL "
            WHEN CDGREV ="31" THEN "31_PROV_FIXE_MANUEL "
            WHEN CDGREV ="32" THEN "32_PROV_VAR_MANUEL "
            WHEN CDGREV ="40" THEN "40_SANS_PROV_AUTOMAT"
            WHEN CDGREV ="41" THEN "41_PROV_FIXE_AUTOMAT"
            WHEN CDGREV ="42" THEN "42_PROV_VAR_AUTOMAT "
            WHEN CDGREV ="43" THEN "43_SANS_REV_AUTOMAT "
            WHEN CDGREV ="80" THEN "80_REVISION_SPECIALE"
            WHEN CDGREV ="90" THEN "90_SANS_PROV_AUTOMAT"
            WHEN CDGREV ="91" THEN "91_PROV_FIXE_AUTOMAT"
            WHEN CDGREV ="92" THEN "92_PROV_VAR_AUTOMAT "
         END) AS CRITERE_REVISION,
        CDGREV,
        
        /* Capital and risk fields */
        CDFRACT, QUARISQ, NMRISQ, NMSRISQ, RESRISQ, RUERISQ, LIDIRISQ, POSRISQ, VILRISQ, CDREG, 
        MTCAPI1,MTCAPI2,MTCAPI3,MTCAPI4,MTCAPI5,MTCAPI6,MTCAPI7,MTCAPI8,MTCAPI9,MTCAPI10,
        MTCAPI11,MTCAPI12,MTCAPI13,MTCAPI14, 
        LBCAPI1,LBCAPI2,LBCAPI3,LBCAPI4,LBCAPI5,LBCAPI6,LBCAPI7,LBCAPI8,LBCAPI9,LBCAPI10,
        LBCAPI11,LBCAPI12,LBCAPI13,LBCAPI14,
        
        /* Initialized indicators */
        0 AS primeto, 0 AS Primes_PTF, 0 AS Primes_AFN, 0 AS Primes_RES, 0 AS Primes_RPT, 0 AS Primes_RPC, 
        0 AS NBPTF, 0 AS NBAFN, 0 AS NBRES, 0 AS NBRPT, 0 AS NBRPC, 0 AS TOP_TEMP, 0 AS expo_ytd, 0 AS expo_gli, 
        0 AS Cotis_100, 0 as MTCA_, 0 AS TOP_LTA, 0 AS PERTE_EXP, 0 AS RISQUE_DIRECT, 0 AS VALUE_INSURED, 
        0 AS SMP_100, 0 AS LCI_100, 0 AS TOP_AOP, "AZ " AS DIRCOM,
        CTDEFTRA, CTPRVTRV, DTOUCHAN, DTRCPPR, DTRECTRX, DTRCPRE, LBNATTRV, DSTCSC, LBQLTSOU,
        0 AS NBAFN_ANTICIPE, 0 AS NBRES_ANTICIPE, 
        0 AS DT_DEB_EXPO format= ddmmyy10., 
        0 AS DT_FIN_EXPO format= ddmmyy10.,
        ACTPRIN, MTSMPR;

    /* ========================================================================
     * STEP 1: EXTRACT PORTFOLIO DATA (AGENT + COURTAGE)
     * ======================================================================== */
    
    /* SG : 20/05/2020 : SELECTION DU MARCHE CONSTRUCTION => LIBRAIRIE PTF16 (AGENCE), PTF36(COURTAGE) */
    PROC SQL;
        CREATE TABLE MVT_CONST_PTF&vision. AS
        (
            /* Agent Portfolio (CDPOLE = 1) */
            SELECT 
                &VAR_PTF., 
                "1" AS CDPOLE, 
                &vision. AS VISION, 
                &annee. AS EXEVUE, 
                &mois. AS MOISVUE, 
                CDNAF, CDTRE, CDACTPRO, FNCMACA AS MTCA, MTCAF, "" AS CTIMB, "" as NoSyn, 
                . AS CTDUREE, tydris1, OPAPOFFR
            FROM PTF16.IPF
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
                &mois. AS MOISVUE, 
                CDNAF, CDTRE, CDACTPRO, FNCMACA AS MTCA, MTCAF, "" AS CTIMB, "" as NoSyn, 
                . AS CTDUREE, tydris1, OPAPOFFR
            FROM PTF36.IPF
            WHERE &filtres_ptf.
        );

        /* ====================================================================
         * STEP 2: EXTRACT CA DATA FOR PRODUCT 01099
         * ==================================================================== */
        
        /* Extraction des montants de chiffres d'affaires pour le produit 01099 */
        CREATE TABLE mvt_ptfa&vision. AS
        (
            SELECT 
                "1" AS CDPOLE, &vision. AS VISION, CDPROD, NOPOL, NOINT, 
                MTCA, MTCAENP, MTCASST, MTCAVNT
            FROM PTF16a.IPFM99
            WHERE CDPROD = '01099'
        )
        OUTER UNION CORR
        (
            SELECT 
                "3" AS CDPOLE, &vision. AS VISION, CDPROD, NOPOL, NOINT, 
                MTCA, MTCAENP, MTCASST, MTCAVNT
            FROM PTF36a.IPFM99
            WHERE CDPROD = '01099'
        );

        /* Join CA data with main portfolio */
        CREATE TABLE MVT_CONST_PTF&vision. AS
        (
            select a.*, b.MTCAENP, b.MTCASST, b.MTCAVNT
            from MVT_CONST_PTF&vision. as a
            left join mvt_ptfa&vision. as b 
                on (a.CDPOLE = b.CDPOLE and a.CDPROD = b.CDPROD 
                    and a.NOPOL = b.NOPOL and a.NOINT = b.NOINT)
        );

        /* Mise à jour des montants de chiffres d'affaires pour le produit 01099 */
        UPDATE MVT_CONST_PTF&vision. 
        set MTCA = MTCAENP + MTCASST + MTCAVNT 
        WHERE CDPROD = '01099';

        /* ====================================================================
         * STEP 3: PROCESS CAPITAL FIELDS (14 FIELDS)
         * ==================================================================== */
        
        /* Calcul de la perte d'exploitation, du risque direct i.e. de la valeur assurée, 
           du SMP et de la LCI (définition du gareat) */
        %Do k=1 %To 14;
            /* Cf Gareat Entreprises & Garages IRD IMS.sas Steffi Boussemart */

            /* LCI Global du Contrat */
            UPDATE MVT_CONST_PTF&vision. 
            SET LCI_100 = mtcapi&k. 
            WHERE (index(lbcapi&k.,"LCI GLOBAL DU CONTRAT") > 0 
                   or index(lbcapi&k.,"CAPITAL REFERENCE OU LCI") > 0 
                   or index(lbcapi&k.,"LCI IRD (DOM. DIRECTS+P.EXPLOIT)") > 0 
                   or index(lbcapi&k.,"LIMITE CONTRACTUELLE INDEMNITE") > 0);

            /* SMP Global du Contrat */
            UPDATE MVT_CONST_PTF&vision. 
            SET SMP_100 = mtcapi&k. 
            WHERE (index(lbcapi&k.,"SMP GLOBAL DU CONTRAT") > 0 
                   or index(lbcapi&k.,"SMP RETENU") > 0 
                   or index(lbcapi&k.,"SINISTRE MAXIMUM POSSIBLE") > 0);

            /* RISQUE DIRECT */
            UPDATE MVT_CONST_PTF&vision. 
            SET RISQUE_DIRECT = mtcapi&k. 
            WHERE ((index(lbcapi&k.,"RISQUE DIRECT") > 0 
                    and lbcapi&k. not in ("SINIS MAX POSSIBLE RISQUE DIRECT")) 
                   or index(lbcapi&k.,"CAPITAUX DOMMAGES DIR") > 0);

            /* Perte d'Exploitation */
            UPDATE MVT_CONST_PTF&vision. 
            SET PERTE_EXP = mtcapi&k. 
            WHERE index(lbcapi&k., "PERTE EXPLOITATION (MARGE BRUTE)") > 0 
                   or index(lbcapi&k., "PERTE D EXPLOITATION") > 0 
                   or index(lbcapi&k., "PERTE D'EXPLOITATION") > 0 
                   or index(lbcapi&k., "CAPITAL PERTES EXPLOITATION") > 0 
                   or index(lbcapi&k., "CAPITAUX TOTAUX P.E.") > 0 
                   or index(lbcapi&k., "PERTE EXPLOITATION/AUT. DOM. MAT") > 0  /* Ajout 2020.02.18 AAV */
                   or index(lbcapi&k., "PERTES D'EXPLOITATION") > 0 
                   or index(lbcapi&k., "PERTES EXPLOITATION") > 0;
        %End;

        /* ====================================================================
         * STEP 4: CALCULATE PREMIUM AND BUSINESS INDICATORS
         * ==================================================================== */
        
        /* Primes portefeuille */
        UPDATE MVT_CONST_PTF&vision. 
        SET primeto = mtprprto * (1 - tx / 100);
        
        UPDATE MVT_CONST_PTF&vision. 
        SET TOP_LTA = 1 
        WHERE ((CTDUREE > 1) and (tydris1 in ("QAW" "QBJ" "QBK" "QBB" "QBM")));

        /* ====================================================================
         * STEP 5: CALCULATE PORTFOLIO/MOVEMENTS INDICATORS
         * ==================================================================== */
        
        /* MVT CONST */
        /* Nombre de contrats en PTF */
        UPDATE MVT_CONST_PTF&vision. 
        SET NBPTF = 1, Primes_PTF = primeto 
        WHERE (CSSSEG ne "5" 
               and CDNATP in ('R','O') 
               and ((cdsitp in('1') and dtcrepol <= "&DTFIN."d) 
                    or (cdsitp = '3' and dtresilp > "&DTFIN."d)));

        /* Les affaires nouvelles (AFN) */
        UPDATE MVT_CONST_PTF&vision. 
        SET NBAFN = 1, Primes_AFN = primeto 
        WHERE ((("&DTDEB_AN"d <= dteffan <= "&DTFIN"d) and ("&DTDEB_AN"d <= dttraan <= "&DTFIN"d)) 
               or (("&DTDEB_AN"d <= dtcrepol <= "&DTFIN"d)) 
               or (year(dteffan) < year("&DTFIN"d) and ("&DTDEB_AN"d <= dttraan <= "&DTFIN"d)));

        /* Les résiliations (RES) - SG: 24/05/2020, on écarte les Chantiers du dénombrement CDNATP= 'C' */
        UPDATE MVT_CONST_PTF&vision. 
        SET NBRES = 1, Primes_RES = primeto 
        WHERE CDNATP ne 'C' 
              and ((cdsitp='3' and ("&DTDEB_AN"d <= dtresilp <= "&DTFIN"d)) 
                   or (cdsitp='3' and ("&DTDEB_AN"d <= dttraar <= "&DTFIN"d) and dtresilp <= "&DTFIN"d));

        /* Les remplacés (RPC) */
        UPDATE MVT_CONST_PTF&vision. 
        SET NBRES = 0, Primes_RES = 0, NBRPC = 1, Primes_RPC = primeto 
        WHERE NBRES = 1 
              And ((cdtypli1 in('RP') and year(dttypli1)=year("&DTFIN"d)) 
                   or (cdtypli2 in('RP') and year(dttypli2)=year("&DTFIN"d)) 
                   or (cdtypli3 in('RP') and year(dttypli3)=year("&DTFIN"d)));

        /* Les remplaçants (RPT) */
        UPDATE MVT_CONST_PTF&vision. 
        SET NBAFN = 0, Primes_AFN = 0, NBRPT = 1, Primes_RPT = primeto 
        WHERE NBAFN = 1 
              And ((cdtypli1 in('RE') and year(dttypli1)=year("&DTFIN"d)) 
                   or (cdtypli2 in('RE') and year(dttypli2)=year("&DTFIN"d)) 
                   or (cdtypli3 in('RE') and year(dttypli3)=year("&DTFIN"d)));

        /* Création d'un top temporaire */
        UPDATE MVT_CONST_PTF&vision. 
        SET TOP_TEMP = 1 
        WHERE cdnatp = "T";

        /* ====================================================================
         * STEP 6: CALCULATE EXPOSURE INDICATORS
         * ==================================================================== */
        
        /* Expo RIS à la maille contrat */
        UPDATE MVT_CONST_PTF&vision. 
        SET expo_ytd = (MAX(0,(MIN(dtresilp, "&DTFIN"d) - MAX(dtcrepol, "&DTDEB_AN"d)) + 1)) / ((mdy(12,31,&annee.) - "&DTDEB_AN"d) + 1), 
            expo_gli = (MAX(0,(MIN(dtresilp, "&DTFIN"d) - MAX(dtcrepol, "&dtfinmn1"d+1)) + 1)) / ("&DTFIN"d - "&dtfinmn1"d), 
            DT_DEB_EXPO = MAX(dtcrepol, "&DTDEB_AN"d), 
            DT_FIN_EXPO = MIN(dtresilp, "&DTFIN"d)
        /* Selection de ce portefeuille pour le calcul des freq (expo) dans le code de FG. cf ptf_freq_az_v3 */
        WHERE (cdnatp in ("R","O") 
               and ((cdsitp = '1' and dtcrepol <= "&dtfinmn"d) 
                    or (cdsitp='3' and dtresilp > "&dtfinmn"d))) 
              or (cdsitp in ('1' '3') 
                  and ((dtcrepol <= "&dtfinmn"d and (dtresilp=. or "&dtfinmn1"d <= dtresilp < "&dtfinmn"d)) 
                       or (dtcrepol <= "&dtfinmn"d and dtresilp > "&dtfinmn"d) 
                       or ("&dtfinmn1"d < dtcrepol <= "&dtfinmn"d and "&dtfinmn1"d <= dtresilp)) 
                  and cdnatp not in ('F'));

        /* ====================================================================
         * STEP 7: CALCULATE COTISATION 100% AND CA
         * ==================================================================== */
        
        UPDATE MVT_CONST_PTF&vision. 
        SET PRCDCIE = 100 
        WHERE PRCDCIE = . or PRCDCIE = 0;

        /* Cotisation technique à 100% cf Steffi Boussemart code du GAREAT */
        UPDATE MVT_CONST_PTF&vision. 
        SET Cotis_100 = mtprprto;
        
        UPDATE MVT_CONST_PTF&vision. 
        SET Cotis_100 = (mtprprto * 100) / prcdcie 
        WHERE (TOP_COASS = 1 AND CDCOAS in ('4', '5')); /* i.e. COASS ACCEPTEE */

        /* Centralisation du CA dans 1 colonne */
        UPDATE MVT_CONST_PTF&vision. SET MTCA = 0 WHERE MTCA = .;
        UPDATE MVT_CONST_PTF&vision. SET MTCAF = 0 WHERE MTCAF = .;
        UPDATE MVT_CONST_PTF&vision. SET MTCA_ = MTCAF + MTCA;

        /* ====================================================================
         * STEP 8: BUSINESS RULE APPLICATIONS
         * ==================================================================== */
        
        /* Ajout 2020.02.11 TOP_AOP cf mail d'Alain Voss du 06.02.2020 */
        UPDATE MVT_CONST_PTF&vision. 
        SET TOP_AOP = 1 
        WHERE OPAPOFFR = 'O';

        /* SG 21/07/2020 Iop migration AZEC */
        /*UPDATE MVT_CONST_PTF&vision. SET DIRCOM = "AZEC" WHERE (CDCIEORI = '63' AND CDSITMGR = 'Z' AND substr(NOPOL,1,1)='Z');*/

        /* AFN/RES anticipés */
        UPDATE MVT_CONST_PTF&vision. 
        SET NBAFN_ANTICIPE = 1 
        WHERE ((dteffan > &finmois.) OR (dtcrepol > &finmois.)) 
              AND (NOT (cdtypli1 in('RE') or cdtypli2 in('RE') or cdtypli3 in('RE')));
        
        UPDATE MVT_CONST_PTF&vision. 
        SET NBRES_ANTICIPE = 1 
        WHERE (dtresilp > &FINMOIS.) 
              AND (NOT (cdtypli1 in('RP') or cdtypli2 in('RP') or cdtypli3 in('RP') or cdmotres='R' or cdcASres='2R'));
    QUIT;

    /* ========================================================================
     * STEP 9: DATA CLEANUP
     * ======================================================================== */
    
    data MVT_CONST_PTF&vision.;
        set MVT_CONST_PTF&vision.;
        if EXPO_YTD = 0 then do; 
            DT_DEB_EXPO = .; 
            DT_FIN_EXPO = .; 
        end;
        If NMCLT in(" ") then NMCLT=NMACTA; 
        else NMCLT=NMCLT;
    run;

    /* ========================================================================
     * STEP 10: PREPARE SEGMENTATION REFERENCE DATA
     * ======================================================================== */
    
    /* Agent segmentation */
    DATA Segment1;
        ATTRIB lmarch2 FORMAT = $46.;
        ATTRIB lseg2 FORMAT = $54.;
        ATTRIB lssseg2 FORMAT = $60.;
        SET SEGMprdt.PRDPFA1 (KEEP = cmarch lmarch cseg lseg cssseg lssseg CPROD lprod);
        WHERE cmarch IN ('6');
        lmarch2 = (cmarch!!"_"!!lmarch);
        lseg2 = (cseg!!"_"!!lseg);
        lssseg2 = (cssseg!!"_"!!lssseg);
        reseau = '1';
        KEEP CPROD lprod cseg lseg2 cssseg lssseg2 cmarch lmarch2 reseau;
    RUN;

    PROC SORT DATA = Segment1 NODUPKEY;
        BY CPROD;
    RUN;

    /* Courtage segmentation */
    DATA Segment3;
        ATTRIB lmarch2 FORMAT = $46.;
        ATTRIB lseg2 FORMAT = $54.;
        ATTRIB lssseg2 FORMAT = $60.;
        SET SEGMprdt.PRDPFA3 (KEEP = cmarch lmarch cseg lseg cssseg lssseg CPROD lprod);
        WHERE cmarch IN ('6');
        lmarch2 = (cmarch!!"_"!!lmarch);
        lseg2 = (cseg!!"_"!!lseg);
        lssseg2 = (cssseg!!"_"!!lssseg);
        reseau = '3';
        KEEP CPROD lprod cseg lseg2 cssseg lssseg2 cmarch lmarch2 reseau;
    RUN;

    PROC SORT DATA = Segment3 NODUPKEY;
        BY CPROD;
    RUN;

    /* Product catalog */
    DATA Cproduit;
        SET AACPrtf.Cproduit;
    RUN;

    PROC SORT DATA = Cproduit NODUPKEY;
        BY CPROD;
    RUN;

    /* Combine segmentations */
    DATA Segment;
        SET Segment1 Segment3;
    RUN;

    PROC SORT DATA = Segment;
        BY CPROD;
    RUN;

    /* Enrich with product type */
    DATA Segment;
        MERGE Segment (IN = a) Cproduit (IN = b KEEP = CPROD Type_Produit_2 segment Segment_3);
        BY CPROD;
        IF a;
    RUN;

    PROC SORT DATA = Segment;
        BY CPROD;
    RUN;

    /* Product label enrichment */
    DATA PRDCAPa (KEEP = CPROD lprod);
        ATTRIB CPROD FORMAT = $5.;
        ATTRIB lprod FORMAT = $34.;
        SET PRDCAP.PRDCAP (KEEP = cdprod lbtprod);
        CPROD = cdprod;
        lprod = lbtprod;
    RUN;

    PROC SORT DATA = PRDCAPa NODUPKEY;
        BY CPROD;
    RUN;

    DATA Segment1;
        MERGE Segment (IN = a DROP = lprod) PRDCAPa (IN = b);
        BY CPROD;
        IF a and b;
    RUN;

    DATA Segment2;
        MERGE Segment (IN = a) PRDCAPa (IN = b);
        BY CPROD;
        IF a and not b;
    RUN;

    DATA Segment;
        SET Segment1 Segment2;
    RUN;

    PROC SORT DATA = Segment NODUPKEY;
        BY reseau CPROD;
    RUN;

    /* ========================================================================
     * STEP 11: GET MANAGEMENT POINT REFERENCE
     * ======================================================================== */
    
    %if &vision. <=201112 %then %do;
        data TABLE_PT_GEST;
            set PT_GEST.PTGST_201201;
        run;
    %end;
    %else %do;
        %derniere_version(PT_GEST, ptgst_, &vision., TABLE_PT_GEST);
    %end;

    /* ========================================================================
     * STEP 12: FINAL ENRICHMENT - SEGMENTATION AND UPPER_MID
     * ======================================================================== */
    
    /* Ajout des données segment, type_produit_2 et Upper_Mid */
    proc sql;
        create table MVT_CONST_PTF&vision. as
        select 
            a.*, 
            b.segment as segment2, 
            b.type_produit_2, 
            c.Upper_MID
        from MVT_CONST_PTF&vision. a
        left join segment b on a.cdprod = b.cprod and a.CDPOLE = b.reseau
        left join TABLE_PT_GEST c on a.PTGST = c.PTGST
        order by nopol, cdsitp;
    quit;

    proc sort data=MVT_CONST_PTF&vision. nodupkey;
        by nopol;
    run;
%MEND;
