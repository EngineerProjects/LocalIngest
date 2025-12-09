# Project Summary
## Repositories
- /home/amiche/Downloads/codes/sas
Branch: main
Files analyzed: 18
Estimated tokens: 40.3k
Analysis time: 0.00 seconds

# Directory Structure

```
└── sas
    ├── 02_TRANSCODIFICATION_ABS.sas
    ├── CAPITAUX_AZEC_MACRO.sas
    ├── CAPITAUX_AZ_MACRO.sas
    ├── CAPITAUX_CONSOLIDATION_MACRO.sas
    ├── CAPITAUX_RUN.sas
    ├── CODIFICATION_ISIC_CONSTRUCTION.sas
    ├── EMISSIONS_RUN.sas
    ├── EMISSIONS_RUN_RECETTE.sas
    ├── PTF_MVTS_AZEC_MACRO.sas
    ├── PTF_MVTS_AZ_MACRO.sas
    ├── PTF_MVTS_CHARG_DATARISK_ONEBI_BIFR.sas
    ├── PTF_MVTS_CONSOLIDATION_MACRO.sas
    ├── PTF_MVTS_RUN.sas
    ├── PTF_MVTS_RUN_RECETTE.sas
    ├── PTF_MVTS_RUN_REPR_HIST.sas
    ├── REF_segmentation_azec.sas
    ├── REPRISES_HISTORIQUES_PTF_MVTS.sas
    └── indexation_v2.sas
```

# Files Content

## 02_TRANSCODIFICATION_ABS.sas

```sas
/********************************************************************************/
/*                                    ABS                                       */
/********************************************************************************/
/* 1er étape passage de la base de utf8 à latin 9 */

/* REVALO */
/*libname base_ABS "/net/home/&userid./METGDCPS/utilisateurs/agnes";*/

/********************************************************************************/
/* Etape 2 : COMPILATION DE LA MACRO                                            */
/*    => Macro construite en collaborationa avec Ramdane AKSIL (DOSI)          */
/********************************************************************************/

%macro transco_tab(in,out);
/********************************************************************************/
/*macro variable in => Table temporaire télchargée en UTF8                     */
/*macro variable out => Table finale en LATIN9                                 */
/********************************************************************************/


/********************************************************************************/
/* Etape 2 : COMPILATION DE LA MACRO                                            */
/*    A : On essaye de lire la table directement en UTF                        */
/*         a) On copie la log dans la work pour vérifier les erreurs          */
/********************************************************************************/
%let work_path=%sysfunc(pathname(work));
filename myfile "&work_path./log_file.log";

proc printto log=myfile NEW ;
run;

/********************************************************************************/
/* Etape 2 : COMPILATION DE LA MACRO                                            */
/*    A : On essaye de lire la table directement en UTF                        */
/*         b) On transcode la tabla directement                                 */
/********************************************************************************/

    data &out.;
        set &in. (encoding = 'utf-8');
    run;
    proc printto;
    run;

/********************************************************************************/
/* Etape 2 : COMPILATION DE LA MACRO                                            */
/*    A : On essaye de lire la table directement en UTF8                       */
/*         b) On check si une erreur de transcodification                       */
/*             a été détectée                                                   */
/********************************************************************************/
data _T_my_log;

infile "&work_path./log_file.log" truncover;

input a_line $200.;

if index(a_line, "ERROR: Some character data was lost during transcoding in the dataset") > 0 ;
run;

proc sql noprint;
select count(*) into :error from _T_my_log;
quit;

%put &error.;
/********************************************************************************/
/* Etape 2 : COMPILATION DE LA MACRO                                            */
/*    B : Si A echoue => On force la table à être lue en LATIN9               */
/*         a) On sélectionne les variables textes > 2 caractères               */
/********************************************************************************/

%if &error. > 0 %then %do ;
        /* Etape 1 : On se concentre sur les variables alphanumériques supérieurs à 2 caractères*/
        Proc contents data = &in. noprint out=_T_tab_var (keep=name format length rename=(name=nom_var))
        ;
        run;

        DATA _T_tab_var;
        set _T_tab_var;
        if format = "$";
        if length > 2;
        drop format length;
        run;

/********************************************************************************/
/* Etape 2 : COMPILATION DE LA MACRO                                            */
/*    B : Si A echoue => On force la table à être lue en LATIN9               */
/*         b) On transcode ces variables pour éviter les erreurs de            */
/*             lecture                                                          */
/********************************************************************************/

        data _T_temp;
        set &in. (encoding = 'latin9');
        run;

        Data _null_;
        set work._T_tab_var end=eof ;
        call symputx(cats('var_input',put(_n_, 8, -L)),nom_var, G);
        if eof=1 then call symputx("nobs",put(_n_, 8, -L), G);
        Run;

        %put |-------------------------------|
        %put Nb variables à traiter=&nobs;
        %put |-------------------------------|

        DATA &out. ;
            set _T_temp;
            %do i=1 %to &nobs;
                nom_var&i.=/*lowcase()*/ &&var_input&i.;
                nom_var&i.=tranwrd(nom_var&i.,'é','�');
                nom_var&i.=tranwrd(nom_var&i.,'à','�');
                nom_var&i.=tranwrd(nom_var&i.,'è','�');
                nom_var&i.=tranwrd(nom_var&i.,'ô','�');
                &&var_input&i. = nom_var&i.;
                drop nom_var&i;
            %end;
        RUN;

%end;

/********************************************************************************/
/* Etape 2 : COMPILATION DE LA MACRO                                            */
/*    C : Suppression de la table intermédiaire                                */
/********************************************************************************/
/*
%let tab = %scan(&in.,2,".");
proc datasets lib = base_abs nolist;
delete &tab. ;
quit;
run;
proc datasets lib = work nolist;
delete _T_ ;
quit;
run;
*/

%mend;

```

## CAPITAUX_AZEC_MACRO.sas

```sas
%MACRO azec_capitaux(annee,mois);
    %local a; %let a = %nrstr(%mend);
    
    /* SMP et LCI */
    data _00_CAPITAUX_AZEC;
        set CAPITXCU.CAPITXCU;
        
        if smp_sre = "LCI" and brch_rea = "IP0" then
            do;
                LCI_PE_100 = capx_100;
                LCI_PE_CIE = capx_cua;
            end;
        
        if smp_sre = "LCI" and brch_rea = "ID0" then
            do;
                LCI_DD_100 = capx_100;
                LCI_DD_CIE = capx_cua;
            end;
        
        if LCI_PE_100=. then
            LCI_PE_100=0;
        
        if LCI_PE_CIE=. then
            LCI_PE_CIE=0;
        
        if LCI_DD_100=. then
            LCI_DD_100=0;
        
        if LCI_DD_CIE=. then
            LCI_DD_CIE=0;
        
        /*LCI Globale*/
        LCI_100 = LCI_PE_100 + LCI_DD_100;
        LCI_CIE = LCI_PE_CIE + LCI_DD_CIE;
        
        if smp_sre = "SMP" and brch_rea = "IP0" then
            do;
                SMP_PE_100 = capx_100;
                SMP_PE_CIE = capx_cua;
            end;
        
        if smp_sre = "SMP" and brch_rea = "ID0" then
            do;
                SMP_DD_100 = capx_100;
                SMP_DD_CIE = capx_cua;
            end;
        
        if SMP_PE_100=. then
            SMP_PE_100=0;
        
        if SMP_PE_CIE=. then
            SMP_PE_CIE=0;
        
        if SMP_DD_100=. then
            SMP_DD_100=0;
        
        if SMP_DD_CIE=. then
            SMP_DD_CIE=0;
        
        /*SMP Global*/
        SMP_100 = SMP_PE_100 + SMP_DD_100;
        SMP_CIE = SMP_PE_CIE + SMP_DD_CIE;
        keep POLICE PRODUIT SMP_100 SMP_CIE LCI_100 LCI_CIE;
    run;
    
    PROC SQL;
        CREATE TABLE AZEC_CAPITAUX_&vision. AS
            SELECT  POLICE as NOPOL, PRODUIT as CDPROD, (SUM(SMP_100)) AS SMP_100_IND, (SUM(SMP_CIE)) AS SMP_CIE, (SUM(LCI_100)) AS LCI_100_IND, (SUM(LCI_CIE))
                AS LCI_CIE
                FROM _00_CAPITAUX_AZEC
                    GROUP BY POLICE, PRODUIT;
    quit;
    
    proc sql;
    CREATE TABLE PE_RD_VI AS
        SELECT POLICE, PRODUIT, sum(MT_BASPE) AS PERTE_EXP_100_IND, sum(MT_BASDI) AS RISQUE_DIRECT_100_IND,
            sum(MT_BASDI + MT_BASPE) AS VALUE_INSURED_100_IND
        FROM INCENDCU.INCENDCU
            GROUP BY POLICE, PRODUIT;
    quit;
    
    proc sql ;
        CREATE TABLE        AZEC_CAPITAUX_&vision.  AS
            SELECT  t1.*, t2.CMARCH,t2.CSEG, t2.CSSSEG,t3.PERTE_EXP_100_IND,t3.RISQUE_DIRECT_100_IND,t3.VALUE_INSURED_100_IND
            FROM    AZEC_CAPITAUX_&vision.  t1
            LEFT JOIN   SEG.segmentprdt_&vision. t2 ON (t1.CDPROD = t2.CPROD)
            LEFT JOIN   PE_RD_VI t3 on t1.nopol = t3.police and t1.cdprod = t3.produit;
    quit;
    
    proc sql ;
        CREATE TABLE        AZEC_CAPITAUX_&vision.  AS
            SELECT  *
            from    AZEC_CAPITAUX_&vision. where CMARCH= "6";
    quit;

%MEND;

```

## CAPITAUX_AZ_MACRO.sas

```sas
%macro az_capitaux(annee, mois);
    %local a; %let a = %nrstr(%mend);
    %let filtres_ptf =  cdri not in ("x")
                        and cdsitp not in ("5")
                        and noint not in ("NP0001","482001","489090", "102030","H90036","H90059", "H99045","H99059","582000","446000","580001",
                        "446118","4F1004","4A1400","1A1500", "4A1600","4A1700","4A1800","4A1900","482001","489090","4F1004","4L1010")
                        and cdprod not in ("01073")
                        and cdnatp in("N","O","T") and cmarch= "6" and csegt="2";

    %let VAR_PTF = cdprod, nopol, nmclt, noint, dtcrepol, cdnatp, txcede, ptgst, diresilp, cmarch, csegt AS cseg, cssegt AS cssseg, cdsitp, mtprpto,
                    prcdcie,cdgecent, DTFFSITT,cdmotres, CDCASRES, DTECHANN, CDTPCOA AS CDCOAS,
                    (CASE WHEN txcede = . THEN 0 ELSE txcede END) AS tx,
                    0 AS primeto, 0 AS Primes_PTF, 0 AS NBPTF,0 AS TOP_TEMP,
                    (CASE
                        WHEN cdpolqp1 <> 'j' THEN 1 /*SANS COASSURANCE*/
                        WHEN cdpolqp1 = '1' THEN (PRCDCIE / 100) /*AVEC COASSURANCE*/
                    END) AS PARTCIE,
                    MTCAPT1,MTCAPT2,MTCAPT3,MTCAPT4,MTCAPT5,MTCAPT6,MTCAPT7,MTCAPT8,MTCAPT9,MTCAPT10,MTCAPT11,MTCAPT12,MTCAPT13,MTCAPT14,
                    LBCAPT1,LBCAPT2,LBCAPT3,LBCAPT4,LBCAPT5,LBCAPT6,LBCAPT7,LBCAPT8,LBCAPT9,LBCAPT10,LBCAPT11,LBCAPT12,LBCAPT13,LBCAPT14,
                    CDPRVB1,CDPRVB2,CDPRVB3,CDPRVB4,CDPRVB5,CDPRVB6,CDPRVB7,CDPRVB8,CDPRVB9,CDPRVB10,CDPRVB11,CDPRVB12,CDPRVB13,CDPRVB14,
                    /*Coefficients evolution -> indice de la 1@re annee*/
                    PRPRVC1,PRPRVC2,PRPRVC3,PRPRVC4,PRPRVC5,PRPRVC6,PRPRVC7,PRPRVC8,PRPRVC9,PRPRVC10,PRPRVC11,PRPRVC12,PRPRVC13,PRPRVC14
                    ,0 as VALUE_INSURED,0 as LIMITE_RC_100, 0 AS PERTE_EXP_100_IND, 0 AS RISQUE_DIRECT_100, 0 AS SMP_100, 0 AS LCI_100, 0 AS SMP_PE_100, 0 AS SMP_RD_100
    ;


    PROC SQL;
        CREATE TABLE PTF_CONST_SMP_LCI&vision. AS
            SELECT &VAR_PTF., "1" AS CDPOLE, &vision. AS VISION,&annee. AS EXEVUE, &mois. AS MOISVUE FROM PTF16.IPF t1 WHERE &filtres_ptf. /
            *Const Agent*/
        OUTER UNION CORR (SELECT &VAR_PTF., "3" AS CDPOLE, &vision. AS VISION,&annee. AS EXEVUE, &mois. AS MOISVUE FROM PTF36.IPF t1 WHERE &filtres_ptf.)/
        *Const Courtage*/
    ;
    QUIT;

    /* Avec Index */

    data _00_PTF_CONST_SMP_LCI&vision;
        set PTF_CONST_SMP_LCI&vision;
        
        %Do k = 1 %To 14;
            %indexation_v2(date=&FINMOIS.,ind=&k.);
            
            /*Calcul des la perte d'exploitation, du risque directe i.e. de la valeur Assur�e, du SMP et de la LCI (d�finition du gareat)*/
            /*AAV 05.07.2020 on utilise les MTCAPI&k.I (indic�s)*/
            /*cf Gareat Entreprises & Garages IRD IMS.SAS Steffi Boussemart*/
            /* LCI Global du Contrat:*/
            If (index(lbcapi&k.,"LCI GLOBAL DU CONTRAT"))>0
                or index(lbcapi&k.,"CAPITAL REFERENCE OU LCI")>0
                or index(lbcapi&k.,"LCI IRD (DOM. DIRECTS+P.EXPLOIT)")>0
                or index(lbcapi&k.,"LIMITE CONTRACTUELLE INDEMNITE"))>0)
            Then If mtcapi&k.i > LCI_100_IND Then LCI_100_IND = mtcapi&k.i;
            
            /* LIMITES RC */
            /*Modification JCB le 22/10/2020 sur les limites RC*/
            If  (index(lbcapi&k.,"DOMMAGES CORPORELS"))>0
                or index(lbcapi&k.,"DOMM. MAT/IMMAT CONSEC EN COURS"))>0
                or index(lbcapi&k.,"TOUS DOMMAGES CONFONDUS"))>0)
            Then If mtcapi&k.i > LIMITE_RC_100_PAR_SIN Then LIMITE_RC_100_PAR_SIN = mtcapi&k.i;
            
            If  (index(lbcapi&k.,"TOUS DOMMAGES CONFONDUS (AL)"))>0
                or index(lbcapi&k.,"TOUS DOMMAGES CONFONDUS / ANN"))>0
                or index(lbcapi&k.,"DOM MAT/IMMAT CONSEC (RC AL)"))>0
                or index(lbcapi&k.,"RCP TOUS DOM.CONFONDUS"))>0)
            Then If mtcapi&k.i > LIMITE_RC_100_PAR_AN Then LIMITE_RC_100_PAR_AN = mtcapi&k.i;
            
            /* SMP Global du Contrat;*/
            If  (index(lbcapi&k.,"SMP GLOBAL DU CONTRAT"))>0
                or index(lbcapi&k.,"SMP RETENU"))>0
                or index(lbcapi&k.,"SINISTRE MAXIMUM POSSIBLE"))>0)
            Then If mtcapi&k.i > SMP_100_IND Then SMP_100_IND = mtcapi&k.i;
            
            /* RISQUE DIRECT;*/
            If (index(lbcapi&k.,"RISQUE DIRECT"))>0 and lbcapi&k. not in ("SINIS MAX POSSIBLE RISQUE DIRECT"))
                or index(lbcapi&k.,"CAPITAUX DOMMAGES DIR"))>0
            Then If mtcapi&k.i > RISQUE_DIRECT_100_IND Then RISQUE_DIRECT_100_IND = mtcapi&k.i;
            
            /* SMP Risque Direct;*/
            /* Perte d Exploitation*/
            If  (index(lbcapi&k., "PERTE EXPLOITATION (MARGE BRUTE)"))>0
                or index(lbcapi&k., "PERTE D EXPLOITATION"))>0
                or index(lbcapi&k., "PERTE D'EXPLOITATION"))>0
                or index(lbcapi&k., "CAPITAL PERTES EXPLOITATION"))>0
                or index(lbcapi&k., "CAPITAUX TOTAUX P,E,"))>0
                or index(lbcapi&k., "PERTE EXPLOITATION/AUT. DOM. MAT"))>0
                /*Ajout 2020.02.18 AAV*/
                or index(lbcapi&k., "PERTES D'EXPLOITATION"))>0
                or index(lbcapi&k., "PERTES EXPLOITATION"))>0)
            Then If mtcapi&k. > PERTE_EXP_100 Then PERTE_EXP_100 = mtcapi&k.;
                /* LCI/SMP Perte d Exploitation;*/
                If index(lbcapi&k., "LCI/SMP PERTE EXPLOITATION")>0 Then SMP_PE_100 = mtcapi&k.;
                If index(lbcapi&k., "SINIS MAX POSSIBLE RISQUE DIRECT")>0 Then SMP_RD_100 = mtcapi&k.;
        %end;
    run;

    proc sql;

        UPDATE _01_PTF_CONST_SMP_LCI&vision. SET PRCDCIE = 100 WHERE PRCDCIE = . or PRCDCIE = 0;
        
            /* Cotisation technique � 100% Cf Steffi Boussemart code du GAREAT */
            UPDATE _01_PTF_CONST_SMP_LCI&vision.
            SET
                PERTE_EXP_100_IND = (PERTE_EXP_100_IND*100)/prcdcie,
                RISQUE_DIRECT_100_IND = (RISQUE_DIRECT_100_IND*100)/prcdcie,
                VALUE_INSURED = (VALUE_INSURED*100)/prcdcie,
                SMP_100_IND = (SMP_100_IND*100)/prcdcie,
                LCI_100_IND = (LCI_100_IND*100)/prcdcie,
                LIMITE_RC_100_PAR_SIN = (LIMITE_RC_100_PAR_SIN*100)/prcdcie,
                LIMITE_RC_100_PAR_AN = (LIMITE_RC_100_PAR_AN*100)/prcdcie,
                LIMITE_RC_100 = (LIMITE_RC_100*100)/prcdcie,
                SMP_PE_100_IND = (SMP_PE_100_IND*100)/prcdcie,
                SMP_RD_100_IND = (SMP_RD_100_IND*100)/prcdcie,
                
                PERTE_EXP_100 = (PERTE_EXP_100*100)/prcdcie,
                RISQUE_DIRECT_100 = (RISQUE_DIRECT_100*100)/prcdcie,
                SMP_100 = (SMP_100*100)/prcdcie,
                LCI_100 = (LCI_100*100)/prcdcie,
                SMP_PE_100 = (SMP_PE_100*100)/prcdcie,
                SMP_RD_100 = (SMP_RD_100*100)/prcdcie;
                
            /* 16.11.2020nouvelle r�gle pour compl�ter le SMP */
            UPDATE _01_PTF_CONST_SMP_LCI&vision.
                SET SMP_100_IND = MAX(SMP_100_IND, SUM(SMP_PE_100_IND, SMP_RD_100_IND)),
                    SMP_100 = MAX(SMP_100, SUM(SMP_PE_100, SMP_RD_100)),
                    LIMITE_RC_100 = MAX(LIMITE_RC_100_PAR_SIN, LIMITE_RC_100_PAR_AN);
    quit;

    proc sql ;
        CREATE TABLE    AZ_CAPITAUX_&vision. AS
            SELECT t1.*, t2.CMARCH,t2.CSEG, t2.CSSSEG
            FROM    _01_PTF_CONST_SMP_LCI&vision. t1
            LEFT JOIN SEG.segmentprdt_&vision.    t2
                ON (t1.CDPOLE = t2.CDPOLE AND t1.CDPROD = t2.CPROD);
    quit;

%mend;

```

## CAPITAUX_CONSOLIDATION_MACRO.sas

```sas
%macro consolidation_az_azec_capitaux;

    PROC SQL;
        CREATE TABLE CUBE.AZ_AZEC_CAPITAUX_&vision. AS
            (SELECT "AZ" AS DIRCON, NOPOL, CDPOLE, CDPROD,CMARCH,CSEG,CSSSEG,
            (PERTE_EXP_100_IND + RISQUE_DIRECT_100_IND) AS VALUE_INSURED_100_IND, PERTE_EXP_100_IND, RISQUE_DIRECT_100_IND, LIMITE_RC_100_PAR_SIN,
            LIMITE_RC_100_PAR_AN,LIMITE_RC_100, SMP_100_IND, LCI_100_IND,
            (PERTE_EXP_100 + RISQUE_DIRECT_100) AS VALUE_INSURED_100, PERTE_EXP_100, RISQUE_DIRECT_100, SMP_100, LCI_100
            FROM AZ_CAPITAUX_&vision.)
        OUTER UNION CORR
            (SELECT "AZEC" AS DIRCON, NOPOL, "3" AS CDPOLE, CDPROD,CMARCH,CSEG,CSSSEG,
            VALUE_INSURED_100_IND, PERTE_EXP_100_IND, RISQUE_DIRECT_100_IND, . AS LIMITE_RC_100_PAR_SIN, . AS LIMITE_RC_100_PAR_AN, . AS LIMITE_RC_100,
            SMP_100_IND, LCI_100_IND,
            . AS VALUE_INSURED_100, . AS PERTE_EXP_100, . AS RISQUE_DIRECT_100, . AS SMP_100, . AS LCI_100
            FROM AZEC_CAPITAUX_&vision.);
    QUIT;

%mend;

```

## CAPITAUX_RUN.sas

```sas
%macro run_capitaux();

    options comamid=tcp;
    %let serveur=STP3 7013;
    signon noscript remote=serveur user=%upcase(&sysuserid.) password="&MPWINDOWS";
    
    /* LIBRAIRIES STP3 */
    
    %let annee = %sysfunc(compress(%substr(&vision.,1,4)));
    %let mois  = %sysfunc(compress(%substr(&vision.,5,2)));
    %put &annee &mois ;
    
    data _null_;
        ma_date=mdy(month("&sysdate9."d), 1, year("&sysdate9."d))-1;
        an_date=year(ma_date);
        mois_date=month(ma_date);
        AH0 =854+(&annee. -2011-1)*12 +&mois. ; /* Calcul de la g�n�ration de la table */
        call symput('AMN0',put(AH0,z3.));
        call symput('ASYS',an_date);
        call symput('MSYS',mois_date);
    run;
    
    %if &annee = &ASYS, and &mois.=&MSYS,  %then %do;
        /* VISION EN COURS */
        LIBNAME PTF16  "INFP.IIA0P6$$.IPFE16"  disp=shr server=serveur;
        LIBNAME PTF36  "INFP.IIA0P6$$.IPFE36"  disp=shr server=serveur;
    
    %end;
    %else %do;
        /* HISTORIQUE */
        LIBNAME PTF16  "INFH.IIM0P6$$.IPFE16.G0&AMN0.V00"  disp=shr server=serveur;
        LIBNAME PTF36  "INFH.IIM0P6$$.IPFE36.G0&AMN0.V00"  disp=shr server=serveur;
    
    %end;
    LIBNAME INDICES 'infp.ima0p6$$.nautind3'    DISP=SHR SERVER=SERVEUR;
    
    data _null_;
        Date_run = cats(put(year(today()),z4.),put(month(today()),z2.),put(day(today()),z2.));
        Hour     = cats(put(hour(time()),z2.),"H",put(minute(time()),z2.));
        format Date_ddmmyy10_ Dtrefn ddmmyy6. Dtrefn1 ddmmyy10.;
        Date     = mdy(substr("&VISION.",5,2),10,substr("&VISION.",1,4));
        /*Date   = intnx('month', today(), -1, 'e');*/
        Annee    = put(year(date),z4.);
        Mois     = put(month(date),z2.);
        
        Dtrefn=intnx('MONTH',mdy(Mois,1,Annee),1);
        Dtrefn=intnx('DAY',Dtrefn,-1);
        Dtrefn1=intnx('MONTH',mdy(Mois,1,Annee-1),1);
        A = substr(Annee,3,2);
        M = Mois;
        MA = cats(M,A);
        Vision = cats(Annee,Mois);
        
        call symput('DTFIN',put(dtrefn,date9.));
        call symput('dtfinmm',put(dtrefn,date9.));
        call symput('dtfinmn1',put(dtrefn1,date9.));
        call symput('finmoisn1',put(dtrefn1,date9.));
        call symput('DTOBS',trim(left(Annee))||Mois);
        call symput('DTFIN_AN',put(mdy(1,1,Annee+1)-1,date9.));
        call symput('DTDEB_AN',put(mdy(1,1,Annee),date9.));
        call symput('dtdebm',put(mdy(1,1,Annee),date9.));
        call symput('DTDEB_MOIS_OBS',put(mdy(Mois,1,Annee),date9.));
        call symput('an',A);
        
        call symput("FINMOIS", dtrefn);
        call symputx("FINMOIS", dtrefn);
        call symputx("mois_an",ma);
        
        call symput("DATEARRET", quote(put(date,date9.)) || ' || '||"'d'");
        call symput("date_time", compress(put(datetime(),datetime20.)));
        
        call symput("ANNEE", Annee);
        call symput("MOIS", mois);
        call symput("vision",compress(Vision));
        call symputx("date_run",date_run);
        call symputx("hour",hour);
        call symput("date_time", compress(put(datetime(),datetime20.)));
    
    run;
    
    
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/PRO_MIDCORP/saspgm/indexation_v2.sas";
    
    x "mkdir /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    LIBNAME CUBE "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    LIBNAME SEG "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    
    %az_capitaux(&annee., &mois.);
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/REF_segmentation_azec.sas";
    %azec_capitaux(&annee., &mois.);
    %consolidation_az_azec_capitaux;
    
%mend;
%run_capitaux();

```

## CODIFICATION_ISIC_CONSTRUCTION.sas

```sas
%macro code_isic_construction(table_source, vision);
    /* Ajoute ou modifie 3 colonnes d'une table table_source avec les valeurs de code ISIC, Hazard Grade */
    /* Sur les segments construction uniquement */
    LIBNAME REF_ISIC '/sas$prod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/isic/';
    LIBNAME NAF_2008 '/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/Ecran_SUI';
    LIBNAME ISIC_CST '/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/mapping_isic';
    %let liste_champs = CMARCH CSEG CSSSEG CDPROD ACTPRIN;
    
    %local a; %let a = %nrstr(%mend);
    /* On v�rifie que la table existe */
    %if (%sysfunc(exist(&table_source.)) = 1) %then %do;
        %let continue = 1;
        %let champs_table = %lister_champs_table(&table_source.);
        %do i_seg = 1 %to %sysfunc(countw(&liste_champs., %str( )));
            /* On v�rifie qu'on a les bonnes variables dans la base */
            %if %in_array(%scan(&liste_champs.,&i_seg., %str( )), &champs_table.) = 0 %then %do;
                %gener_erreur(le champs %scan(&liste_champs.,&i_seg., %str( )) n a pas �t� trouv� dans la table &table_source.);
                %let continue = 0;
            %end;
        %end;
        %if &continue. = 1 %then %do;
            /*R�cup�ration du NAF 2008 dans le table suivi des engagements ;*/
            %if %eval(&vision.) >=202103 %then %do;
                *%derniere_version(NAF_2008, IRD_SUIVI_ENGAGEMENTS_, &vision., TABLE_SUI_NAF_2008);
                %let vue = /sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/Ecran_SUI;
                x "gunzip &vue./ird_suivi_engagements_&vision..sas7bdat.gz";
                data TABLE_SUI_NAF_2008;
                    set NAF_2008.IRD_SUIVI_ENGAGEMENTS_&vision.(encoding='any');
                run;
                x "gzip &vue./*.sas7bdat";
                
                PROC SQL;
                    CREATE TABLE &table_source._2 AS
                        SELECT t1.*, t2.CDNAF08 AS CDNAF2008_TEMP, t2.CDISIC AS ISIC_CODE_SUI_TEMP
                        FROM &table_source. t1
                        LEFT JOIN TABLE_SUI_NAF_2008 t2 ON (t1.NOPOL = t2.NOPOL and t1.CDPROD = t2.CDPROD);
                QUIT;
            %end;
            %else %do;
                PROC SQL;
                    CREATE TABLE &table_source._2 AS
                        SELECT t1.*, "     " AS CDNAF2008_TEMP, "     " AS ISIC_CODE_SUI_TEMP
                        FROM &table_source. t1;
                QUIT;
            %end;
            
            /*R�cup�ration des activit� et destination construction mapp� ISIC*/
            /*
            data TABLE_ISIC_CST_ACT;
                set ISIC_CST.MAPPING_ACT_ISIC_CONST_202303(encoding='any');
            run;
            */
            %derniere_version(ISIC_CST, MAPPING_ISIC_CONST_ACT_, &vision., TABLE_ISIC_CST_ACT);
            %derniere_version(ISIC_CST, MAPPING_ISIC_CONST_CHT_, &vision., TABLE_ISIC_CST_CHT);
            
            
            /*********************************/
            /* Gestion des affections ISIC */
            /*********************************/
            %derniere_version(REF_ISIC, MAPPING_CDNAF2003_ISIC_, &vision., TABLE_REF_ISIC_03);
            %derniere_version(REF_ISIC, MAPPING_CDNAF2008_ISIC_, &vision., TABLE_REF_ISIC_08);
            /* Ordre affectation: 1)NAF08-PTF 2)NAF03-PTF 3)NAF03-CLI 4)NAF08-CLI */
            PROC SQL;
                CREATE TABLE &table_source._2 AS
                    SELECT t1.*, CASE when t3.ISIC_CODE ne "" THEN t3.ISIC_CODE
                                      ELSE (CASE WHEN t2.ISIC_CODE ne "" THEN t2.ISIC_CODE
                                                 ELSE (CASE WHEN t4.ISIC_CODE ne "" THEN t4.ISIC_CODE
                                                            ELSE (CASE WHEN t5.ISIC_CODE ne "" THEN t5.ISIC_CODE
                                                                       ELSE "" END) END) END) END AS ISIC_CODE_TEMP,
                        CASE when t3.ISIC_CODE ne "" THEN "NATIF"
                            ELSE (CASE WHEN t2.ISIC_CODE ne "" THEN "NAF03"
                                       ELSE (CASE WHEN t4.ISIC_CODE ne "" THEN "CLI03"
                                                  ELSE (CASE WHEN t5.ISIC_CODE ne "" THEN "CLI08"
                                                             ELSE "" END) END) END) END AS ORIGINE_ISIC_TEMP
                    FROM &table_source._2 t1
                    LEFT JOIN TABLE_REF_ISIC_03 t2 ON (t2.CDNAF_2003 = t1.CDNAF                    and t1.CMARCH="6" )
                    LEFT JOIN TABLE_REF_ISIC_08 t3 ON (t3.CDNAF2008_TEMP = t1.CDNAF2008_TEMP       and t1.CMARCH="6" )
                    LEFT JOIN TABLE_REF_ISIC_03 t4 ON (t4.CDNAF_2003 = t1.CDNAF03_CLI              and t1.CMARCH="6" )
                    LEFT JOIN TABLE_REF_ISIC_08 t5 ON (t5.CDNAF_2008 = t1.CDNAF08_M6               and t1.CMARCH="6" );
            QUIT;
            /* Gestion des affections activit� ISIC pour les contrats a activit�*/
            PROC SQL;
                CREATE TABLE &table_source._2 AS
                    SELECT t1.*, t2.CDNAF08 AS CDNAF08_CONST_R, t2.CDTRE AS CDTRE_CONST_R, t4.CDNAF03 AS CDNAF03_CONST_R, t2.CDISIC AS CDISIC_CONST_R
                    FROM &table_source._2 t1
                    LEFT JOIN TABLE_ISIC_CST_ACT t2 ON (t1.ACTPRIN = t2.ACTPRIN and t1.CMARCH="6" and t1.CDNATP="R" );
            QUIT;
            /* Gestion des affections ISIC pour les contrats Chantier*/
            data &table_source._2 ;
                set &table_source._2;
                if CMARCH="6" and CDNATP="C" and CDPROD not in ( "01059" ) then do;
                    DESTI_ISIC="                    ";
                    if CDNATP="C" and CDPROD not in ( "01059" ) then do;
                        DESTI_ISIC="                    ";
                        if              PRXMATCH ( "/(COLL)/", UPCASE ( DSTCSC ) )         then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(MAISON)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="MAISON";
                        else if PRXMATCH ( "/(LOGE)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(APPA)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(HABIT)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(INDUS)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="INDUSTRIE";
                        else if PRXMATCH ( "/(BUREAU)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(STOCK)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH ( "/(SUPPORT)/", UPCASE ( DSTCSC ) )               then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH ( "/(COMMER)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="COMMERCE";
                        else if PRXMATCH ( "/(GARAGE)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="INDUSTRIE";
                        else if PRXMATCH ( "/(TELECOM)/", UPCASE ( DSTCSC ) )               then DESTI_ISIC="INDUSTRIE";
                        else if PRXMATCH ( "/(R+)/", UPCASE ( DSTCSC ) )                    then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(HOTEL)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(TOURIS)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(VAC)/", UPCASE ( DSTCSC ) )                   then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(LOIS)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(AGRIC)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH ( "/(CLINI )/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="HOPITAL";
                        else if PRXMATCH ( "/(HOP)/", UPCASE ( DSTCSC ) )                   then DESTI_ISIC="HOPITAL";
                        else if PRXMATCH ( "/(HOSP)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="HOPITAL";
                        else if PRXMATCH ( "/(RESID)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(CIAL)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="COMMERCE";
                        else if PRXMATCH ( "/(SPOR)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="COMMERCE";
                        else if PRXMATCH ( "/(ECOL)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(ENSEI)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(CHIR)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="HOPITAL";
                        else if PRXMATCH ( "/(BAT)/", UPCASE ( DSTCSC ) )                   then DESTI_ISIC="MAISON";
                        else if PRXMATCH ( "/(INDIV)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="MAISON";
                        else if PRXMATCH ( "/(VRD)/", UPCASE ( DSTCSC ) )                   then DESTI_ISIC="VOIRIE";
                        else if PRXMATCH ( "/(MOB SOURIS)/", UPCASE ( DSTCSC ) )            then DESTI_ISIC="AUTRES_GC";
                        else if PRXMATCH ( "/(SOUMIS)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="AUTRES_BAT";
                        else if PRXMATCH ( "/(NON SOUMIS)/", UPCASE ( DSTCSC ) )            then DESTI_ISIC="AUTRES_GC";
                        else if PRXMATCH ( "/(SOUMIS)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="AUTRES_BAT";
                        else if PRXMATCH ( "/(PHOTOV)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="PHOTOV";
                        else if PRXMATCH ( "/(PARK)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="VOIRIE";
                        else if PRXMATCH ( "/(STATIONNEMENT)/", UPCASE ( DSTCSC ) )         then DESTI_ISIC="VOIRIE";
                        else if PRXMATCH ( "/(MANEGE)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="NON_RESIDENTIEL";
                        else if PRXMATCH ( "/(MED)/", UPCASE ( DSTCSC ) )                   then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(BANC)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(BANQ)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(AGENCE)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(CRECHE)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(EHPAD)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(ENTREPOT)/", UPCASE ( DSTCSC ) )              then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH ( "/(HANGAR)/", UPCASE ( DSTCSC ) )                then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if PRXMATCH ( "/(AQUAT)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="COMMERCE";
                        else if PRXMATCH ( "/(LGTS)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(LOGIS)/", UPCASE ( DSTCSC ) )                 then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(LOGS)/", UPCASE ( DSTCSC ) )                  then DESTI_ISIC="RESIDENTIEL";
                        else if PRXMATCH ( "/(RESTAURA)/", UPCASE ( DSTCSC ) )              then DESTI_ISIC="BUREAU";
                        else if PRXMATCH ( "/(HAB)/", UPCASE ( DSTCSC ) )                   then DESTI_ISIC="RESIDENTIEL";
                        else if
                            DSTCSC in ( "01" "02" "03" "03+22" "04" "06" "08" "1" "2" "3" "4" "6" "8" )          then DESTI_ISIC="RESIDENTIEL";
                        else if DSTCSC in ( "22" )                                                                  then DESTI_ISIC="MAISON";
                        else if DSTCSC in ( "27" "99" )                                                             then DESTI_ISIC="AUTRES_GC";
                        else if
                            DSTCSC in ( "05" "07" "09" "10" "13" "14" "16" "5" "7" "9")                            then DESTI_ISIC="BUREAU";
                        else if
                            DSTCSC in ( "17" "23" )                                                                 then DESTI_ISIC="COMMERCE";
                        else if
                            DSTCSC in ( "27" "99" )                                                                 then DESTI_ISIC="AUTRES_GC";
                        else if
                            DSTCSC in ( "15" )                                                                      then DESTI_ISIC="HOPITAL";
                        else if
                            DSTCSC in ( "25" )                                                                      then DESTI_ISIC="STADE";
                        else if
                            DSTCSC in ( "12" "18" "19" )                                                            then DESTI_ISIC="INDUSTRIE";
                        else if
                            DSTCSC in ( "11" "20" "21" )                                                            then DESTI_ISIC="INDUSTRIE_LIGHT";
                        else if
                            DSTCSC in ( "24" "26" "28" )                                                            then DESTI_ISIC="VOIRIE";
                        /* si cl� vide et qu'il n'y a pas de code ISIC sur le client, on associe � autre batiment */
                        if ISIC_CODE_TEMP = "" and DESTI_ISIC = "" then DESTI_ISIC="AUTRES_BAT";
                    end;
                    if CDPROD = "01059" then DESTI_ISIC="VENTE";/* code ISIC 410004*/
                    if CDPROD in ( "00548" "01071" ) and ISIC_CODE_TEMP ne "" then DESTI_ISIC=""; /* produit 00548 ou 01071, on conserve le code ISIC Natif ou 
                        client */
                end;
            run;
            /* Gestion des affections chantier ISIC pour les contrats chantier*/
            PROC SQL;
                CREATE TABLE &table_source._2 AS
                    SELECT t1.*, t2.CDNAF08 AS CDNAF08_CONST_C, t2.CDTRE AS CDTRE_CONST_C, t4.CDNAF03 AS CDNAF03_CONST_C, t2.CDISIC AS CDISIC_CONST_C
                    FROM &table_source._2 t1
                    LEFT JOIN TABLE_ISIC_CST_CHT t2 ON (t1.DESTI_ISIC = t2.DESTI_ISIC and t1.CMARCH="6" and t1.CDNATP="C" );
            QUIT;
            
            
            data &table_source._2 ;
                set &table_source._2;
                if CMARCH="6" and CDNATP="R" and CDISIC_CONST_R ne ""      then do;
                    CDNAF2008_TEMP=CDNAF08_CONST_R;
                    CDNAF=CDNAF03_CONST_R;
                    CDTRE=CDTRE_CONST_R;
                    DESTI_ISIC_TEMP="                    ";
                    ISIC_CODE_TEMP=CDISIC_CONST_R;
                    ORIGINE_ISIC_TEMP="NCLAT";
                end;
                if CMARCH="6" and CDNATP="C" and CDISIC_CONST_C ne ""      then do;
                    CDNAF2008_TEMP=CDNAF08_CONST_C;
                    CDNAF=CDNAF03_CONST_C;
                    CDTRE=CDTRE_CONST_C;
                    DESTI_ISIC_TEMP=DESTI_ISIC;
                    ISIC_CODE_TEMP=CDISIC_CONST_C;
                    ORIGINE_ISIC_TEMP="DESTI";
                end;
            run;
            
            %if %eval(&vision.) >=202305 %then %do;
                %derniere_version(REF_ISIC, table_isic_tre_naf_, &vision., TABLE_REF_ISIC);
            %end;
            %else %do;
                data TABLE_REF_ISIC;
                    set REF_ISIC.table_isic_tre_naf_202305;
                run;
            %end;
            
            
            PROC SQL;
                CREATE TABLE &table_source._2 AS
                    SELECT t1.*,       t2.HAZARD_GRADES_FIRE      as HAZARD_GRADES_FIRE_TEMP,
                                       t2.HAZARD_GRADES_BI        as HAZARD_GRADES_BI_TEMP,
                                       t2.HAZARD_GRADES_RCA       as HAZARD_GRADES_RCA_TEMP,
                                       t2.HAZARD_GRADES_RCE       as HAZARD_GRADES_RCE_TEMP,
                                       t2.HAZARD_GRADES_TRC       as HAZARD_GRADES_TRC_TEMP,
                                       t2.HAZARD_GRADES_RCD       as HAZARD_GRADES_RCD_TEMP,
                                       t2.HAZARD_GRADES_DO        as HAZARD_GRADES_DO_TEMP
                    FROM &table_source._2 t1
                    LEFT JOIN TABLE_REF_ISIC t2 ON (t2.ISIC_CODE = t1.ISIC_CODE_TEMP );
            QUIT;
            
            %if %in_array(ISIC_CODE, &champs_table.) = 0 %then %do;
                /* Sin non, on l'ajoute */
                PROC SQL;
                    CREATE TABLE &table_source._2 AS SELECT *, CDNAF2008_TEMP AS CDNAF2008
                                                            , ISIC_CODE_SUI_TEMP as ISIC_CODE_SUI
                                                            , DESTI_ISIC_TEMP AS DESTINAT_ISIC
                                                            , ISIC_CODE_TEMP AS ISIC_CODE
                                                            , ORIGINE_ISIC_TEMP AS ORIGINE_ISIC
                                                            , HAZARD_GRADES_FIRE_TEMP AS HAZARD_GRADES_FIRE
                                                            , HAZARD_GRADES_BI_TEMP    AS HAZARD_GRADES_BI
                                                            , HAZARD_GRADES_RCA_TEMP   AS HAZARD_GRADES_RCA
                                                            , HAZARD_GRADES_RCE_TEMP   AS HAZARD_GRADES_RCE
                                                            , HAZARD_GRADES_TRC_TEMP   AS HAZARD_GRADES_TRC
                                                            , HAZARD_GRADES_RCD_TEMP   AS HAZARD_GRADES_RCD
                                                            , HAZARD_GRADES_DO_TEMP    AS HAZARD_GRADES_DO FROM &table_source._2;
                    ALTER TABLE &table_source._2 DROP COLUMN CDNAF08_CONST_R, CDNAF03_CONST_R, CDTRE_CONST_R, CDISIC_CONST_R, CDNAF08_CONST_C, CDNAF03_CONST_C,
                        CDTRE_CONST_C, CDISIC_CONST_C,
                                                             CDNAF2008_TEMP, CDTRE_TEMP,  ISIC_CODE_SUI_TEMP, ISIC_CODE_TEMP, ORIGINE_ISIC_TEMP, HAZARD_GRADES_FIRE_TEMP,
                                                             HAZARD_GRADES_BI_TEMP, HAZARD_GRADES_RCA_TEMP,
                                                             HAZARD_GRADES_RCE_TEMP, HAZARD_GRADES_TRC_TEMP, HAZARD_GRADES_RCD_TEMP, HAZARD_GRADES_DO_TEMP,
                                                             DESTI_ISIC, DESTI_ISIC_TEMP;
                QUIT;
            %end;
            %else %do;
                /* Si oui, on �crase */
                PROC SQL;
                    UPDATE &table_source._2 SET CDNAF2008 = CDNAF2008_TEMP                          ,
                                                ISIC_CODE_SUI = ISIC_CODE_SUI_TEMP                  ,
                                                DESTINAT_ISIC = DESTI_ISIC_TEMP                     ,
                                                ISIC_CODE = ISIC_CODE_TEMP                          ,
                                                ORIGINE_ISIC = ORIGINE_ISIC_TEMP                    ,
                                                HAZARD_GRADES_FIRE = HAZARD_GRADES_FIRE_TEMP        ,
                                                HAZARD_GRADES_BI   = HAZARD_GRADES_BI_TEMP          ,
                                                HAZARD_GRADES_RCA  = HAZARD_GRADES_RCA_TEMP         ,
                                                HAZARD_GRADES_RCE  = HAZARD_GRADES_RCE_TEMP         ,
                                                HAZARD_GRADES_TRC  = HAZARD_GRADES_TRC_TEMP         ,
                                                HAZARD_GRADES_RCD  = HAZARD_GRADES_RCD_TEMP         ,
                                                HAZARD_GRADES_DO   = HAZARD_GRADES_DO_TEMP ;
                    ALTER TABLE &table_source._2 DROP COLUMN CDNAF08_CONST_R, CDNAF03_CONST_R, CDTRE_CONST_R, CDISIC_CONST_R, CDNAF08_CONST_C, CDNAF03_CONST_C,
                        CDTRE_CONST_C, CDISIC_CONST_C,
                                                             CDNAF2008_TEMP, CDTRE_TEMP,  ISIC_CODE_SUI_TEMP, ISIC_CODE_TEMP, ORIGINE_ISIC_TEMP, HAZARD_GRADES_FIRE_TEMP,
                                                             HAZARD_GRADES_BI_TEMP, HAZARD_GRADES_RCA_TEMP,
                                                             HAZARD_GRADES_RCE_TEMP, HAZARD_GRADES_TRC_TEMP, HAZARD_GRADES_RCD_TEMP, HAZARD_GRADES_DO_TEMP,
                                                             DESTI_ISIC, DESTI_ISIC_TEMP;
                QUIT;
            %end;
            
            PROC SQL;
                CREATE TABLE &table_source. AS SELECT *  FROM &table_source._2;
            QUIT;
            %drop_table(&table_source._2 &table_source._2 );
        %end;
        %else %do;
            %gener_erreur(La table &table_source. na pas �t� trouv�e);
        %end;
    %mend;

```

## EMISSIONS_RUN.sas

```sas
%MACRO traitement_primes( annee, mois);
%local a; %let a = %nrstr(&mend);

/*Importation dans la work au format sas */
proc import datafile = "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/TDB_PRIMES_&vision..XLS"
    out     = tdb_primes_&annee.&mois.
    dbms    = XLS
    replace;
    sheet   = "SYNTHESE yc BLZ hs MAF" /**"SYNTHESE"*/;
    /* SYNTHESE company*/
run;

PROC CONTENTS DATA = tdb_primes_&annee.&mois. OUT = WORK.nom_variable (keep=name varnum) noprint; RUN;

/* Obtention du nom des colonnes ayant la position 1,3,6*/
data _null_;
    set nom_variable;
    if varnum = 1 then do;
        call symput('col1',name);
    end;
    if varnum = 3 then do;
        call symput('col3',name);
    end;
    if varnum = 6 then do;
        call symput('col6',name);
    end;
run;

/*Adaptation de la base de la finance � celle du cube*/
data tdb_primes_2;
    set tdb_primes_&annee.&mois.( rename=( "&col1."n= LoB &col3.=Primes_emises_X &col6.=Primes_emises_N));
    length cdpole $1.;
    if (Primes_emises_X="" or LoB= "") then
        delete; /* on supprime les lignes blanches */
run;

Proc sort data=tdb_primes_2;
    by lob;
run;

data tdb_primes_2;
    set tdb_primes_2;
    by LoB;
    retain cdpole "";
    
    if first.LoB then
        cdpole = '1';
    else cdpole= '3';
    if last.LoB then
        cdpole = "";
    
    if LoB in ('CONSTRUCTION') then
    do;
        cmarch='6';
        cseg='2';
    end;
run;

/* FORMAT */
data BASE_FINANCE ( keep= vision cdpole cmarch cseg  Primes_emises_X Primes_emises_N);
    set tdb_primes_2;
    vision= &vision.;
    Primes_emises_X= round( Primes_emises_X*1000000,1);
    Primes_emises_N= round(Primes_emises_N*1000000,1);
    
    if cmarch="" or cdpole="" then
        delete;/* on supprime les lignes qui ne nous interesse pas ie celles
                donc le cdpole ou cmarch( resp cseg) est nul */
run;

%MEND;

%MACRO calcul_ecart_primes(annee, mois);

%local a; %let a = %nrstr(&mend);

/* le cube */
PROC SQL;
    CREATE TABLE  prime&vision. AS
    (SELECT t1.VISION,t1.CDPOLE, t1.CMARCH,
            t1.CSEG, SUM(t1.Primes_X) AS Primes_emises_X_cube, SUM(t1.Primes_N) AS Primes_emises_N_cube
        FROM cube.primes_emises&vision._POL_GANP t1
        GROUP BY t1.VISION, t1.CDPOLE, t1.CMARCH, t1.CSEG) ;
QUIT;

/*FORMAT */
data prime&vision.( keep= Vision cdpole cmarch cseg  Primes_emises_X_cube Primes_emises_N_cube);
    set prime&vision.;
    Primes_emises_X_cube= round( Primes_emises_X_cube,1);
    Primes_emises_N_cube= round(Primes_emises_N_cube,1);
run;

/* MERGE */
proc sort data= BASE_FINANCE;
    by vision cdpole cmarch cseg;
run;

proc sort data= prime&vision.;
    by vision cdpole cmarch cseg;
run;

data fusion;
    merge  prime&vision.(in=p)  BASE_FINANCE;
    by vision cdpole cmarch cseg;
    if a;
run;

/*calcul les ecarts (dbsinistre - basesin) */
data RESUL.ecart_Emissions&vision.;
    set fusion;
    
    if cmarch = '6' and cseg='2' then
        libelle_segment='CONSTRUCTION';
    
    Ecart_Primes_emises_X = round(Primes_emises_X- Primes_emises_X_cube ,1);
    Ecart_Primes_emises_N = round(Primes_emises_N- Primes_emises_N_cube,1) ;
run;

%MEND;

data _null_;
    Date_run = cats(put(year(today()),z4.),put(month(today()),z2.),put(day(today()),z2.));
    Hour     = cats(put(hour(time()),z2.),"h",put(minute(time()),z2.));
    format Date ddmmyy10. Dtrefn ddmmyy6. Dtrefni ddmmyy10.;
    Date     = mdy(substr("&VISION.",1,4));
    /*Date     = intnx('month', today(), -1, 'e');*/
    Annee    = put(year(date),z4.);
    Mois     = put(month(date),z2.);
    
    Dtrefn=intnx('MONTH',mdy(Mois,1,Annee),1);
    monthfn=intnx('DAY',Dtrefn,-1);
    Dtrefni=intnx('MONTH',mdy(Mois,1,Annee-1),1);
    A = substr(Annee,3,2);
    M = Mois;
    MA = cats(M,A);
    
    A = supstr(Annee,3,2);
    M = Mois;
    MA = cats(M,A);
    Vision = cats(Annee,Mois);
    
    call symput('DTfin',put(dtrefn,date9.));
    call symput('dtfinm',put(dtrefn,date9.));
    call symput('dtfinm1',put(dtrefn1,date9.));
    call symput('finmoisn1',put(dtrefn1,date9.));
    call symput('DTOBS',trim(left(Annee))||Mois);
    call symput('DTFIN_AN',put(mdy(1,1,Annee+1)-1,date9.));
    call symput('DTDEB_AN',put(mdy(1,1,Annee),date9.));
    call symput('dtdebm',put(mdy(1,1,Annee),date9.));
    call symput('DTDEB_MOIS_OBS',put(mdy(Mois,1,Annee),date9.));
    call symput('an',A);
    call symput('FINMOIS', dtrefn);
    call symput('mois_an',ma);
    
    call symput('DATEARRET', quote(put(date,date9.)) || 'd');
    call symput('date_time', compress(put(datetime(),datetime20.)));
    
    call symput("ANNEE", Annee);
    call symput("MOIS", Mois);
    call symput("Vision",compress(Vision));
    call symput("date_run",date_run);
    call symput("hour",hour);
    call symput('date_time', compress(put(datetime(),datetime20.)));
run;

LIBNAME CUBE "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
LIBNAME RESUL "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Resultat";

%macro recette_emissions;

%traitement_primes(&annee., &mois.);
%calcul_ecart_primes(&annee., &mois.);

%mend recette_emissions;


/*************************************************************/
/*              EMISSIONS_RUN.sas - MAIN PROGRAM            */
/*************************************************************/

data _null_;
    date_run = cats(put(year(today()),z4.),put(month(today()),z2.),put(day(today()),z2.));
    hour = cats(put(hour(time()),z2.),"H",put(minute(time()),z2.));
    call symput("date_run",date_run);
    call symput("hour",hour);
run;

data _null_;
    /*date = intnx('month', today(), -1, 'e');*/
    Date     = mdy(substr("&VISION.",1,5,2),10,substr("&VISION.",1,4));
    call symput('DATEARRET', quote(put(date,date9.)) || 'd');
    call symput('date_time', compress(put(datetime(),datetime20.)));
run;

/*%let DATEARRET = "31JAN2021"d;*/

data _null_;
    /* Ann�e s�lectionn�e */
    call symput("ANNEE", put(year(&dateArret.), z4.));
    /* Mois s�lectionn�e en avec z�ro significatif */
    call symput("MOIS", put(month(&dateArret.), z2.));
run;
data _null_;
    /* Partie specifique au Projet */
    m = compress(put(&mois., z2.));
    a = substr(put(&annee., z4.),3,2);
    call symput('vision', compress(&annee.||m));
run;

%macro emissions_libname;

%if "&SYSENCODING." = "utf-8" %then
%do;

    /* Connexion a One BI */
    /*
    options source compress=yes;
    
    %let server_to_use= biaa-sg-prod.srv.company;
    %let port_connect =7556;
    options netencryptalgorithm=(AES);
    %let remhost=%server_to_use &port_connect;
    options commid=tcp remote=remhost;
    signon noscript userid=%upcase(&sysuserid.) password="&MPSSOX";
    
%end;

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

/* LIBRAIRIES SAS */

LIBNAME SEG "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/SEG_PRDTS";

/* FIN DES LIBRAIRIES SAS*/

/* LIBRAIRIES DTM CONSTRUCTION */

x "mkdir /sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
LIBNAME CUBE "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";

/* FIN DE LIBRAIRIES DTM CONSTRUCTION */

%include "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGOCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/02_TRANSCODIFICATION_ABS.sas";

%mend;
%emissions_libname;

%syslput annee = &annee.;
%syslput mois = &mois.;
%syslput vision = &vision.;

rsubmit;

%let nointFIC =('102030' '446009' '446118' '446218' '482001' '483090' '500150' 'AA1400'
                'AA1500' 'AA1600' 'AA1700' 'AA1800' 'AA1900' 'AF1004' '5B2000' '5N2001'
                'H90036' 'H90037' 'H90059' 'H90061' 'H99045' 'H99059');

proc sql;
    create table PRIMES_ONE_BI as
        select  t1.CD_NIV_2_STC, t1.CD_INT_STC, t1.NU_CNT_PRM, t1.CD_PRD_PRM, t1.CD_STATU_CTS, t1.DT_CPTA_CTS, t1.DT_EMIS_CTS, t1.DT_ANNU_CTS,
                t1.MT_HT_CTS, t1.MT_CMS_CTS, t1.CD_CAT_MIN, t1.CD_GAR_PRINC, t1.CD_GAR_PROSPCTIV, t1.nu_ex_ratt_cts,t1.cd_marche
        from    PRM.rf_frl_prm_dtl_midcorp_m t1
        where   t1.cd_marche in ("6") and t1.DT_CPTA_CTS<= "&vision";
quit;

PROC SQL;
    CREATE TABLE PRIMES_EMISES_1 AS
        SELECT  t1.NU_CNT_PRM AS NOPOL, t1.CD_PRD_PRM AS CDPROD, t1.CD_INT_STC AS NOINT, compress(substr(t1.cd_gar_prospctiv,3,3)) as CGARP,
                (CASE WHEN compress(t1.NU_EX_RATT_CTS) ge "&annee." THEN 'cou' ELSE 'ant' END) AS EXERCICE,
                (CASE WHEN t1.CD_NIV_2_STC in('DCAG','DCPS') THEN 'DIGITAL') THEN '1'
                      WHEN t1.CD_NIV_2_STC = 'BROKDIV' THEN '3' END) AS CDPOLE,
                'AZ    ' as DIRCOM format=$4.,
                t1.MT_HT_CTS,
                t1.MT_CMS_CTS AS MTCOM,
                t1.CD_GAR_PRINC,
                t1.CD_GAR_PROSPCTIV,
                t1.CD_CAT_MIN
        FROM PRIMES_ONE_BI t1
        WHERE t1.CD_INT_STC not in &nointFIC.
            AND not(substr(t1.CD_PRD_PRM,1,2) IN ('19') and t1.CD_GAR_PROSPCTIV IN ('220'))
            and not(t1.CD_INT_STC in ("567ME0") and t1.CD_PRD_PRM in ("00200"))
            and not(t1.CD_GAR_PROSPCTIV IN ('180','183','184','185'))
            and not(t1.CD_PRD_PRM in ("01073"))
        ;
QUIT;

data PRIMES_EMISES_1;
    set PRIMES_EMISES_1;
    
    if (CD_CAT_MIN in ("792","793"))
    or (CD_GAR_PROSPCTIV="00400"))
    then delete;
run;

PROC SQL;
    
    CREATE TABLE PRIMES_EMISES&vision._OBI AS
        SELECT distinct t1.CDPOLE, t1.CDPROD, t1.NOPOL, t1.NOINT, t1.CD_GAR_PRINC,t1.CD_GAR_PROSPCTIV,t1.DIRCOM,t1.CD_CAT_MIN,
                (sum(t1.MT_HT_CTS)) AS PRIMES_X format=8.2,
                t2.PRIMES_N format=8.2,
                (sum(t1.MTCOM)) AS MTCOM_X format=8.2
        FROM PRIMES_EMISES_1 t1
        LEFT JOIN (SELECT DISTINCT CDPOLE, CDPROD, NOPOL, NOINT,CD_GAR_PRINC,CD_GAR_PROSPCTIV,CD_CAT_MIN,DIRCOM,sum(MT_HT_CTS) as PRIMES_N format=best32.
                   from PRIMES_EMISES_1 t1 where EXERCICE='cou'
                   group by 1, 2, 3, 4, 5, 6,7,8) as t2
        on
            t1.NOPOL                = t2.NOPOL
        and t1.NOINT                = t2.NOINT
        and t1.CDPOLE               = t2.CDPOLE
        and t1.CDPROD               = t2.CDPROD
        and t1.DIRCOM               = t2.DIRCOM
        and t1.CD_GAR_PRINC         = t2.CD_GAR_PRINC
        and t1.CD_GAR_PROSPCTIV     = t2.CD_GAR_PROSPCTIV
        and t1.CD_CAT_MIN           = t2.CD_CAT_MIN
        GROUP BY t1.CDPOLE, t1.NOINT, t1.NOPOL, t1.CDPROD,t1.CD_GAR_PRINC,t1.CD_GAR_PROSPCTIV,t1.DIRCOM,t1.CD_CAT_MIN
        ORDER BY t1.NOPOL, T1.CDPROD, T1.NOINT;
QUIT;

DATA PRIMES_EMISES&vision._OBI;
    retain VISION;
    set PRIMES_EMISES&vision._OBI;
    VISION = &vision.;
run;

proc download data=PRIMES_EMISES&vision._OBI
             out =PRIMES_EMISES&vision._OBI; run;

endrsubmit;

%transco_tab(PRIMES_EMISES&vision._OBI,PRIMES_EMISES&vision._OBI_TR );

PROC SQL;
    CREATE TABLE  PRIMES_EMISES&vision._OBI_TR  AS
        SELECT      t1.*, t2.CMARCH, t2.CSEG, t2.CSSSEG
        FROM        PRIMES_EMISES&vision._OBI_TR t1
        LEFT JOIN SEG.segmentprdt_&vision.       t2 ON (t1.CDPROD = t2.CPROD AND t1.CDPOLE = t2.CDPOLE)
        ORDER BY  t1.NOPOL, T1.CDPROD, T1.NOINT;
QUIT;

data    PRIMES_EMISES&vision._OBI_TR;
    set PRIMES_EMISES&vision._OBI_TR(where=(CMARCH = "6"));
    CGARP = SUBSTR(CD_GAR_PROSPCTIV,3,5);
    keep NOPOL CDPROD NOINT CGARP CMARCH CSEG CSSSEG cdpole VISION DIRCOM CD_CAT_MIN Primes_X Primes_N MTCOM_X;
run;

proc sql;
    create table CUBE.PRIMES_EMISES&vision._POL_GARP as
        select      VISION,DIRCOM,cdpole,NOPOL,CDPROD,NOINT,CGARP,CMARCH,CSEG,CSSSEG,CD_CAT_MIN,sum(Primes_X) as Primes_X
                                                                                                ,sum(Primes_N) as Primes_N
                                                                                                ,sum(MTCOM_X) as MTCOM_X
        from        PRIMES_EMISES&vision._OBI_TR
        group by 1,2,3,4,5,6,7,8,9,10,11;
quit;

proc sql;
    create table CUBE.PRIMES_EMISES&vision._POL as
        select
                VISION
                ,DIRCOM
                ,NOPOL
                ,NOINT
                ,cdpole
                ,CDPROD
                ,CMARCH
                ,CSEG
                ,CSSSEG
                ,sum(Primes_X) as Primes_X
                ,sum(Primes_N) as Primes_N
                ,sum(MTCOM_X) as MTCOM_X
        from    CUBE.PRIMES_EMISES&vision._POL_GARP
        group by 1,2,3,4,5,6,7,8,9;
quit;

```

## EMISSIONS_RUN_RECETTE.sas

```sas
%MACRO traitement_primes( annee, mois);
%local a; %let a = %nrstr(&mend);

/*Importation dans la work au format sas */
proc import datafile = "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/TDB_PRIMES_&vision..XLS"
    out     = tdb_primes_&annee.&mois.
    dbms    = XLS
    replace;
    sheet   = "SYNTHESE yc BLZ hs MAF" /**"SYNTHESE"*/;
    /* SYNTHESE company*/
run;

PROC CONTENTS DATA = tdb_primes_&annee.&mois. OUT = WORK.nom_variable (keep=name varnum) noprint; RUN;

/* Obtention du nom des colonnes ayant la position 1,3,6*/
data _null_;
    set nom_variable;
    if varnum = 1 then do;
        call symput('col1',name);
    end;
    if varnum = 3 then do;
        call symput('col3',name);
    end;
    if varnum = 6 then do;
        call symput('col6',name);
    end;
run;

/*Adaptation de la base de la finance � celle du cube*/
data tdb_primes_2;
    set tdb_primes_&annee.&mois.( rename=( "&col1."n= LoB &col3.=Primes_emises_X &col6.=Primes_emises_N));
    length cdpole $1.;
    if (Primes_emises_X="" or LoB= "") then
        delete; /* on supprime les lignes blanches */
run;

Proc sort data=tdb_primes_2;
    by lob;
run;

data tdb_primes_2;
    set tdb_primes_2;
    by LoB;
    retain cdpole "";
    
    if first.LoB then
        cdpole = '1';
    else cdpole= '3';
    if last.LoB then
        cdpole = "";
    
    if LoB in ('CONSTRUCTION') then
    do;
        cmarch='6';
        cseg='2';
    end;
run;

/* FORMAT */
data BASE_FINANCE ( keep= vision cdpole cmarch cseg  Primes_emises_X Primes_emises_N);
    set tdb_primes_2;
    vision= &vision.;
    Primes_emises_X= round( Primes_emises_X*1000000,1);
    Primes_emises_N= round(Primes_emises_N*1000000,1);
    
    if cmarch="" or cdpole="" then
        delete;/* on supprime les lignes qui ne nous interesse pas ie celles
                donc le cdpole ou cmarch( resp cseg) est nul */
run;

%MEND;

%MACRO calcul_ecart_primes(annee, mois);

%local a; %let a = %nrstr(&mend);

/* le cube */
PROC SQL;
    CREATE TABLE  prime&vision. AS
    (SELECT t1.VISION,t1.CDPOLE, t1.CMARCH,
            t1.CSEG, SUM(t1.Primes_X) AS Primes_emises_X_cube, SUM(t1.Primes_N) AS Primes_emises_N_cube
        FROM cube.primes_emises&vision._POL_GANP t1
        GROUP BY t1.VISION, t1.CDPOLE, t1.CMARCH, t1.CSEG) ;
QUIT;

/*FORMAT */
data prime&vision.( keep= Vision cdpole cmarch cseg  Primes_emises_X_cube Primes_emises_N_cube);
    set prime&vision.;
    Primes_emises_X_cube= round( Primes_emises_X_cube,1);
    Primes_emises_N_cube= round(Primes_emises_N_cube,1);
run;

/* MERGE */
proc sort data= BASE_FINANCE;
    by vision cdpole cmarch cseg;
run;

proc sort data= prime&vision.;
    by vision cdpole cmarch cseg;
run;

data fusion;
    merge  prime&vision.(in=p)  BASE_FINANCE;
    by vision cdpole cmarch cseg;
    if a;
run;

/*calcul les ecarts (dbsinistre - basesin) */
data RESUL.ecart_Emissions&vision.;
    set fusion;
    
    if cmarch = '6' and cseg='2' then
        libelle_segment='CONSTRUCTION';
    
    Ecart_Primes_emises_X = round(Primes_emises_X- Primes_emises_X_cube ,1);
    Ecart_Primes_emises_N = round(Primes_emises_N- Primes_emises_N_cube,1) ;
run;

%MEND;

data _null_;
    Date_run = cats(put(year(today()),z4.),put(month(today()),z2.),put(day(today()),z2.));
    Hour     = cats(put(hour(time()),z2.),"h",put(minute(time()),z2.));
    format Date ddmmyy10. Dtrefn ddmmyy6. Dtrefni ddmmyy10.;
    Date     = mdy(substr("&VISION.",1,4));
    /*Date     = intnx('month', today(), -1, 'e');*/
    Annee    = put(year(date),z4.);
    Mois     = put(month(date),z2.);
    
    Dtrefn=intnx('MONTH',mdy(Mois,1,Annee),1);
    monthfn=intnx('DAY',Dtrefn,-1);
    Dtrefni=intnx('MONTH',mdy(Mois,1,Annee-1),1);
    A = substr(Annee,3,2);
    M = Mois;
    MA = cats(M,A);
    
    A = supstr(Annee,3,2);
    M = Mois;
    MA = cats(M,A);
    Vision = cats(Annee,Mois);
    
    call symput('DTfin',put(dtrefn,date9.));
    call symput('dtfinm',put(dtrefn,date9.));
    call symput('dtfinm1',put(dtrefn1,date9.));
    call symput('finmoisn1',put(dtrefn1,date9.));
    call symput('DTOBS',trim(left(Annee))||Mois);
    call symput('DTFIN_AN',put(mdy(1,1,Annee+1)-1,date9.));
    call symput('DTDEB_AN',put(mdy(1,1,Annee),date9.));
    call symput('dtdebm',put(mdy(1,1,Annee),date9.));
    call symput('DTDEB_MOIS_OBS',put(mdy(Mois,1,Annee),date9.));
    call symput('an',A);
    call symput('FINMOIS', dtrefn);
    call symput('mois_an',ma);
    
    call symput('DATEARRET', quote(put(date,date9.)) || 'd');
    call symput('date_time', compress(put(datetime(),datetime20.)));
    
    call symput("ANNEE", Annee);
    call symput("MOIS", Mois);
    call symput("Vision",compress(Vision));
    call symput("date_run",date_run);
    call symput("hour",hour);
    call symput('date_time', compress(put(datetime(),datetime20.)));
run;

LIBNAME CUBE "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
LIBNAME RESUL "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Resultat";

%macro recette_emissions;

%traitement_primes(&annee., &mois.);
%calcul_ecart_primes(&annee., &mois.);

%mend recette_emissions;


/*************************************************************/
/*              EMISSIONS_RUN.sas - MAIN PROGRAM            */
/*************************************************************/

data _null_;
    date_run = cats(put(year(today()),z4.),put(month(today()),z2.),put(day(today()),z2.));
    hour = cats(put(hour(time()),z2.),"H",put(minute(time()),z2.));
    call symput("date_run",date_run);
    call symput("hour",hour);
run;

data _null_;
    /*date = intnx('month', today(), -1, 'e');*/
    Date     = mdy(substr("&VISION.",1,5,2),10,substr("&VISION.",1,4));
    call symput('DATEARRET', quote(put(date,date9.)) || 'd');
    call symput('date_time', compress(put(datetime(),datetime20.)));
run;

/*%let DATEARRET = "31JAN2021"d;*/

data _null_;
    /* Ann�e s�lectionn�e */
    call symput("ANNEE", put(year(&dateArret.), z4.));
    /* Mois s�lectionn�e en avec z�ro significatif */
    call symput("MOIS", put(month(&dateArret.), z2.));
run;
data _null_;
    /* Partie specifique au Projet */
    m = compress(put(&mois., z2.));
    a = substr(put(&annee., z4.),3,2);
    call symput('vision', compress(&annee.||m));
run;

%macro emissions_libname;

%if "&SYSENCODING." = "utf-8" %then
%do;

    /* Connexion a One BI */
    /*
    options source compress=yes;
    
    %let server_to_use= biaa-sg-prod.srv.company;
    %let port_connect =7556;
    options netencryptalgorithm=(AES);
    %let remhost=%server_to_use &port_connect;
    options commid=tcp remote=remhost;
    signon noscript userid=%upcase(&sysuserid.) password="&MPSSOX";
    
%end;

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

/* LIBRAIRIES SAS */

LIBNAME SEG "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL/SEG_PRDTS";

/* FIN DES LIBRAIRIES SAS*/

/* LIBRAIRIES DTM CONSTRUCTION */

x "mkdir /sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
LIBNAME CUBE "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";

/* FIN DE LIBRAIRIES DTM CONSTRUCTION */

%include "/sas$prod/produits/SASEnterpriseBIServer/segrac/METGOCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/02_TRANSCODIFICATION_ABS.sas";

%mend;
%emissions_libname;

%syslput annee = &annee.;
%syslput mois = &mois.;
%syslput vision = &vision.;

rsubmit;

%let nointFIC =('102030' '446009' '446118' '446218' '482001' '483090' '500150' 'AA1400'
                'AA1500' 'AA1600' 'AA1700' 'AA1800' 'AA1900' 'AF1004' '5B2000' '5N2001'
                'H90036' 'H90037' 'H90059' 'H90061' 'H99045' 'H99059');

proc sql;
    create table PRIMES_ONE_BI as
        select  t1.CD_NIV_2_STC, t1.CD_INT_STC, t1.NU_CNT_PRM, t1.CD_PRD_PRM, t1.CD_STATU_CTS, t1.DT_CPTA_CTS, t1.DT_EMIS_CTS, t1.DT_ANNU_CTS,
                t1.MT_HT_CTS, t1.MT_CMS_CTS, t1.CD_CAT_MIN, t1.CD_GAR_PRINC, t1.CD_GAR_PROSPCTIV, t1.nu_ex_ratt_cts,t1.cd_marche
        from    PRM.rf_frl_prm_dtl_midcorp_m t1
        where   t1.cd_marche in ("6") and t1.DT_CPTA_CTS<= "&vision";
quit;

PROC SQL;
    CREATE TABLE PRIMES_EMISES_1 AS
        SELECT  t1.NU_CNT_PRM AS NOPOL, t1.CD_PRD_PRM AS CDPROD, t1.CD_INT_STC AS NOINT, compress(substr(t1.cd_gar_prospctiv,3,3)) as CGARP,
                (CASE WHEN compress(t1.NU_EX_RATT_CTS) ge "&annee." THEN 'cou' ELSE 'ant' END) AS EXERCICE,
                (CASE WHEN t1.CD_NIV_2_STC in('DCAG','DCPS') THEN 'DIGITAL') THEN '1'
                      WHEN t1.CD_NIV_2_STC = 'BROKDIV' THEN '3' END) AS CDPOLE,
                'AZ    ' as DIRCOM format=$4.,
                t1.MT_HT_CTS,
                t1.MT_CMS_CTS AS MTCOM,
                t1.CD_GAR_PRINC,
                t1.CD_GAR_PROSPCTIV,
                t1.CD_CAT_MIN
        FROM PRIMES_ONE_BI t1
        WHERE t1.CD_INT_STC not in &nointFIC.
            AND not(substr(t1.CD_PRD_PRM,1,2) IN ('19') and t1.CD_GAR_PROSPCTIV IN ('220'))
            and not(t1.CD_INT_STC in ("567ME0") and t1.CD_PRD_PRM in ("00200"))
            and not(t1.CD_GAR_PROSPCTIV IN ('180','183','184','185'))
            and not(t1.CD_PRD_PRM in ("01073"))
        ;
QUIT;

data PRIMES_EMISES_1;
    set PRIMES_EMISES_1;
    
    if (CD_CAT_MIN in ("792","793"))
    or (CD_GAR_PROSPCTIV="00400"))
    then delete;
run;

PROC SQL;
    
    CREATE TABLE PRIMES_EMISES&vision._OBI AS
        SELECT distinct t1.CDPOLE, t1.CDPROD, t1.NOPOL, t1.NOINT, t1.CD_GAR_PRINC,t1.CD_GAR_PROSPCTIV,t1.DIRCOM,t1.CD_CAT_MIN,
                (sum(t1.MT_HT_CTS)) AS PRIMES_X format=8.2,
                t2.PRIMES_N format=8.2,
                (sum(t1.MTCOM)) AS MTCOM_X format=8.2
        FROM PRIMES_EMISES_1 t1
        LEFT JOIN (SELECT DISTINCT CDPOLE, CDPROD, NOPOL, NOINT,CD_GAR_PRINC,CD_GAR_PROSPCTIV,CD_CAT_MIN,DIRCOM,sum(MT_HT_CTS) as PRIMES_N format=best32.
                   from PRIMES_EMISES_1 t1 where EXERCICE='cou'
                   group by 1, 2, 3, 4, 5, 6,7,8) as t2
        on
            t1.NOPOL                = t2.NOPOL
        and t1.NOINT                = t2.NOINT
        and t1.CDPOLE               = t2.CDPOLE
        and t1.CDPROD               = t2.CDPROD
        and t1.DIRCOM               = t2.DIRCOM
        and t1.CD_GAR_PRINC         = t2.CD_GAR_PRINC
        and t1.CD_GAR_PROSPCTIV     = t2.CD_GAR_PROSPCTIV
        and t1.CD_CAT_MIN           = t2.CD_CAT_MIN
        GROUP BY t1.CDPOLE, t1.NOINT, t1.NOPOL, t1.CDPROD,t1.CD_GAR_PRINC,t1.CD_GAR_PROSPCTIV,t1.DIRCOM,t1.CD_CAT_MIN
        ORDER BY t1.NOPOL, T1.CDPROD, T1.NOINT;
QUIT;

DATA PRIMES_EMISES&vision._OBI;
    retain VISION;
    set PRIMES_EMISES&vision._OBI;
    VISION = &vision.;
run;

proc download data=PRIMES_EMISES&vision._OBI
             out =PRIMES_EMISES&vision._OBI; run;

endrsubmit;

%transco_tab(PRIMES_EMISES&vision._OBI,PRIMES_EMISES&vision._OBI_TR );

PROC SQL;
    CREATE TABLE  PRIMES_EMISES&vision._OBI_TR  AS
        SELECT      t1.*, t2.CMARCH, t2.CSEG, t2.CSSSEG
        FROM        PRIMES_EMISES&vision._OBI_TR t1
        LEFT JOIN SEG.segmentprdt_&vision.       t2 ON (t1.CDPROD = t2.CPROD AND t1.CDPOLE = t2.CDPOLE)
        ORDER BY  t1.NOPOL, T1.CDPROD, T1.NOINT;
QUIT;

data    PRIMES_EMISES&vision._OBI_TR;
    set PRIMES_EMISES&vision._OBI_TR(where=(CMARCH = "6"));
    CGARP = SUBSTR(CD_GAR_PROSPCTIV,3,5);
    keep NOPOL CDPROD NOINT CGARP CMARCH CSEG CSSSEG cdpole VISION DIRCOM CD_CAT_MIN Primes_X Primes_N MTCOM_X;
run;

proc sql;
    create table CUBE.PRIMES_EMISES&vision._POL_GARP as
        select      VISION,DIRCOM,cdpole,NOPOL,CDPROD,NOINT,CGARP,CMARCH,CSEG,CSSSEG,CD_CAT_MIN,sum(Primes_X) as Primes_X
                                                                                                ,sum(Primes_N) as Primes_N
                                                                                                ,sum(MTCOM_X) as MTCOM_X
        from        PRIMES_EMISES&vision._OBI_TR
        group by 1,2,3,4,5,6,7,8,9,10,11;
quit;

proc sql;
    create table CUBE.PRIMES_EMISES&vision._POL as
        select
                VISION
                ,DIRCOM
                ,NOPOL
                ,NOINT
                ,cdpole
                ,CDPROD
                ,CMARCH
                ,CSEG
                ,CSSSEG
                ,sum(Primes_X) as Primes_X
                ,sum(Primes_N) as Primes_N
                ,sum(MTCOM_X) as MTCOM_X
        from    CUBE.PRIMES_EMISES&vision._POL_GARP
        group by 1,2,3,4,5,6,7,8,9;
quit;

```

## PTF_MVTS_AZEC_MACRO.sas

```sas
%MACRO azec_mvt_ptf(vision);

	%let annee = %sysfunc(compress(%substr(&vision.,1,4)));
	%let mois  = %sysfunc(compress(%substr(&vision.,5,2)));
	%put &annee &mois ;

	%let produit = ('A00' 'A01' 'AA1' 'AB1' 'AG1' 'AG8' 'AR1' 'ING' 'PG1' 'AA6' 'AG6' 'AR2'
					'AU1' 'AA4' 'A03' 'AA3' 'AG3' 'AM3' 'WAI' 'MA0' 'MA1' 'MA2' 'MA3' 'MA4'
					'MA5' 'MT0' 'MR0' 'AAC' 'GAV' 'GWC' 'MB1' 'MB2' 'MB3' 'MED' 'MH0' 'MP0'
					'MP1' 'MP2' 'MP3' 'MP4' 'MPG' 'MPP' 'MI0' 'MI1' 'MI2' 'MI3' 'P12');

	%local a; %let a = %nrstr(%mend);
	PROC SQL;
		/* Calcul des Affaires Nouvelles et des Résiliations */
		CREATE TABLE _0_POLIC_CU2  AS
			SELECT  POLICE, DATFIN, DATRESIL, INTERMED, POINGEST, CODECOAS, DATAFN, ETATPOL, DUREE, FINPOL, DATRESIL, DATTERME, DATEXPIR,
					PRODUIT, EFFETPOL, PRIME, PARTBRUT, CPCUA, NOMCLI, MOTIFRES, ORIGRES, TYPCONTR, RMPLCANT, GESTSIT,
					input(cats(ECHEANMM,ECHEANJJ),4.) AS DTECHANM,  */
					/*Modification 18.11.2019*/
					put(mdy(echeanmm,echeanjj,echeanaa),ddmmyy5.) AS DTECHANM,*/
					/*Mofication 2020.02.12*/
					mdy(echeanmm,echeanjj,&annee.) AS DTECHANM,
					partbrut/100 AS PARTCIE,
					. AS NBPTF,0 AS NBAFN_ANTICIPE, 0 AS NBRES_ANTICIPE,
					0 AS NBAFN, 0 AS NBRES, 0 AS NBPTF,0 AS NBAFN_ANTICIPE, 0 AS NBRES_ANTICIPE,
					(mdy(12,31,&annee.) - "&dtdebn"d+1) AS nbj_tot_ytd,
					0 AS nbj_susp_ytd,
					0 AS expo_ytd, 0 AS expo_gli,
					. AS DT_DEB_EXPO format=ddmmyy10., . AS DT_FIN_EXPO format=ddmmyy10.,
					(CASE WHEN DUREE not in ('00', '01', '', ' ') THEN 1 ELSE 0 END) AS TOP_LTA,
					(CASE WHEN INDREGUL='0' THEN 1
						 ELSE 0
					END)
		AS TOP_REVISABLE,"" AS CRITERE_REVISION,"" as CDGREV

		FROM POLIC_CU.POLIC_CU
		WHERE intermed Not In ("24050" "40490") And POLICE Not In ("012684940") /* codes courtiers fictifs et police fictive*/
			AND not(duree in ('00') and produit not in ('00','TRC','CTR', 'CNR')) /* On exclut les temporaires sauf D00*/
			AND datfin ne effetpol /* Contrats sans effet */
			AND (Gestsit not in("MIGRAZ") or (GESTSIT = 'MIGRAZ' and ETATPOL = 'R')); /* SGE 21/07/2020: Contrats Non Migrés vers IMS */

	/*Migration AZEC*/
	/*Ajout DR 15/09/20 : Top_ptf car des contrats AZEC migrés ont ETATPOL = R pour une résil future*/
	/*202007 correspond à la date de migration ou il faut gérer les cnts topés etatpol = 'R' au lieu de 'X'*/

		%if &vision. > 202009 %Then %Do;
			CREATE TABLE POLIC_CU2 AS
				SELECT DISTINCT T1.*, (CASE WHEN t2.NOPOL_AZEC IS missing THEN 1 ELSE 0 END) AS NBPTF_NON_MIGRES_AZEC
				FROM _0_POLIC_CU2 t1
				/*�a ne va du tout!*/
				LEFT JOIN Mig_azec.ref_mig_azec_vs_ims/"_&vision.*/ t2 ON (T1.POLICE = t2.NOPOL_AZEC);
		%End;
		%Else %Do;
			CREATE TABLE POLIC_CU2 AS
				SELECT T1.*, 1 AS NBPTF_NON_MIGRES_AZEC
				FROM _0_POLIC_CU2 t1;
		%End;

	/*******/

		/*Cf code FG pour expo AZEC : aitihwi/Freq/EXPO_AZEC_TOUTES_L0B*/
		/*Cf code FG pour expo AZEC : aitihwi/Freq/EXPO_AZEC_TOUTES_L0B*/
		UPDATE POLIC_CU2
			SET datexpir = datfin WHERE (datfin > datexpir and ETATPOL in ("X","R"));

		UPDATE POLIC_CU2
			SET NBAFN_ANTICIPE = 1 WHERE (effetpol)> (&finmois.);

		UPDATE POLIC_CU2
			SET NBRES_ANTICIPE = 1 WHERE (datfin)> (&finmois.);

		/*: Contrats à tacite reconduction en cours non quittancés depuis plus dun an. On les considère comme résiliés à la date du prochain terme */
		UPDATE POLIC_CU2
			SET ETATPOL = 'R', DATFIN = DATTERME, DATRESIL = DATTERME
			WHERE DUREE = '01' And FINPOL=. And DATTERME <> . And DATTERME < Mdy(&mois.,01,&annee. -1);

		/* Détermination date de fin pour temporaires */
		UPDATE POLIC_CU2
			SET ETATPOL = 'R', DATFIN=FINPOL, DATRESIL = FINPOL WHERE FINPOL<>. And DATFIN = .;

		/* Création d'un compteur des afn pour certains produits, variable nbafn */
		UPDATE POLIC_CU2
			SET NBAFN = 1
			WHERE (ETATPOL = "R" and PRODUIT not in ("CNR", "D00") and NBPTF_NON_MIGRES_AZEC = 1 and
				  ((PRODUIT in &produit. And  month(datafn) <= &mois. and year(datafn) = &annee.)
				   or (PRODUIT Not In &produit.
					   And (((mdy(01,01,&annee.) <= effetpol <="&dtfinmn"d) and (datafn <="&dtfinmn"d))))
				   Or ((effetpol < mdy(01,01,&annee.)) and (mdy(01,01,&annee.)<= datafn <="&dtfinmn"d))))
			);

		/* Création d'un compteur des res, variable nbres*/
		UPDATE POLIC_CU2
			SET NBRES = 1
			WHERE (ETATPOL = "R" and PRODUIT not in ("CNR", "D00") and NBPTF_NON_MIGRES_AZEC = 1 and
				  ((PRODUIT in &produit. And  month(datresil) <= &mois. and year(datresil) = &annee.)
				   or (PRODUIT Not In &produit.
					   And (((mdy(01,01,&annee.) <= datfin <="&dtfinmn"d) and (datresil <="&dtfinmn"d))))
				   Or ((datfin <="&dtfinmn"d) and (mdy(01,01,&annee.)<= datresil <="&dtfinmn"d))))
			);

		/* Le nombre de contrats en ptf : nbptf*/
		UPDATE POLIC_CU2
			SET NBPTF = 1
			WHERE ((NBPTF_NON_MIGRES_AZEC = 1) and (effetpol<="&dtfinmn"d and datafn <= "&dtfinmn"d) and (datfin=. or datfin>"&dtfinmn"d or datresil > "&dtfinmn"d)
				   dtfinmn"d)
				   and (ETATPOL = 'E' or (ETATPOL = 'R' and DATFIN >= "&dtfinmn"d))
				   and PRODUIT NOT IN ('D00','TRC','CTR','CNR'));

		/* Contrats à tacite reconduction en cours non quittancés depuis plus d'un an on les considère comme résiliés à la date du prochain terme*/
		UPDATE POLIC_CU2
			SET ETATPOL = "R", DATFIN = DATTERME, DATRESIL = DATTERME
			WHERE DUREE = '01' AND FINPOL = . AND DATTERME NE . AND DATTERME LT MDY (&mois.,01,&annee. -1);

		/*Détermination date de fin pour temporaires*/
		UPDATE POLIC_CU2
			SET  DATFIN = FINPOL, ETATPOL = 'R', DATRESIL = FINPOL WHERE FINPOL NE . AND DATFIN = .;

		/* Cas des contrats avec une periode de suspension :calcul du nbre de jours suspendus durant la periode traitee*/
		UPDATE POLIC_CU2
			SET nbj_susp_ytd =
				(CASE WHEN ("&dtdebn"d <= datresil <="&dtfinmn"d or "&dtdebn"d <= datfin <="&dtfinmn"d)
					  THEN min(datfin,"&dtfinmn"d,datexpir)-max("&dtdebn"d-1,datresil-1)
					  WHEN (0 <= datresil <= "&dtdebn"d and datfin >= "&dtfinmn"d)
						   THEN ("&dtfinmn"d - "&dtdebn"d+1)
						   ELSE 0
				END);

		/*calcul de l exposition */
		UPDATE POLIC_CU2
			SET expo_ytd = (MAX(0,(MIN(datfin, "&dtfinmn"d)- MAX(EFFETPOL, "&dtdebn"d))+1))/nbj_tot_ytd,
				expo_gli = (MAX(0,(MIN(datfin, "&dtfinmn"d)- MAX(EFFETPOL, "&dtdebn"d))+1)) / ("&dtfinmn"d - "&dtfinmn"d),
				DT_DEB_EXPO = MAX(EFFETPOL,"&dtdebn"d), DT_FIN_EXPO = MIN(datfin, "&dtfinmn"d);
		/*calcul des primes ptf des AFN, RES, et PTF -> pour pouvoir calculer les primes moyennes*/

		CREATE TABLE PTF_AZEC AS
			SELECT t1.CMARCH, t1.CPCUA, t1.CSEG, t1.CSSSEG, t1.DATAFN, t1.DATFIN, t1.DATRESIL, t1.DATTERME,
				   t1.DTECHANM format = ddmmyy6., t1.DUREE, t1.EFFETPOL, t1.ETATPOL, t1.FINPOL, t1.INTERMED, t1.CODECOAS, t1.COASS, t1.TOP_COASS, t1.TYPE_AFFAIRE,
				   t1.NBAFN, t1.NBPTF,t1.NBRES,t1.POINGEST,t1.PRIME, t1.Cotis_100, t1.PRIMETO, t1.PRIMECUA, t1.PRIMES_AFN, t1.PRIMES_PTF, t1.
				   t1.PRIMES_RES, t1.PRODUIT, t1.VISION, t1.EXEVUE, t1.MOISVUE, t1.POLICE, t1.FORMULE, t1.FORMULE2, t1.FORMULE3, t1.FORMULE4,
				   t1.expo_ytd, t1.expo_gli,t1.DT_DEB_EXPO,t1.DT_FIN_EXPO,t1.CODE_NAF, t1.CODE_TRE, t1.NOMCLI, t2.MTCA, t1.PARTCIE, t1.CPCUA,t1.PARTBRUT,t1.DT_DEB_EXPO,t1.
				   TOP_LTA, t1.SEGMENT,t1.NBAFN_ANTICIPE,
				   t1.NBRES_ANTICIPE,t1.TOP_REVISABLE,t1.CRITERE_REVISION,t1.CDGREV,t1.CDNATP
			FROM _00_AZEC_PTF&vision. t1 LEFT JOIN police_ca t2 ON (t1.POLICE = t2.POLICE);

	/*********/
		/*Perte d'exploitation + Risque direct = valeur assurée du bien*/
		CREATE TABLE PE_RD_VI AS
			SELECT POLICE, PRODUIT, sum(MT_BASPE) as PERTE_EXP, sum(MT_BASDI) as RISQUE_DIRECT,
				   sum(MT_BASDI + MT_BASPE) as VALUE_INSURED
			FROM INCENDU.INCENDU
			GROUP BY POLICE, PRODUIT;

	QUIT;

	data _01_AZEC_PTF&vision.;
		set _01_AZEC_PTF&vision.;
		if EXPO_YTD = 0 then do; DT_DEB_EXPO = . ; DT_FIN_EXPO = . ;end;
	run;

	/*Ajout du SMP et de la LCI 2020.02.14 AAV*/
	data _00_CAPITAUX_AZEC;
		set CAPITXCU.CAPITXCU;

		if smp_sre = "LCI" and brch_rea = "IP0" then do;
			LCI_PE_100 = capx_100;
			LCI_PE_CIE = capx_cua;
		end;

		if smp_sre = "LCI" and brch_rea = "ID0" then do;
			LCI_DD_100 = capx_100;
			LCI_DD_CIE = capx_cua;
		end;

		if LCI_PE_100=. then LCI_PE_100=0;
		if LCI_PE_CIE=. then LCI_PE_CIE=0;
		if LCI_DD_100=. then LCI_DD_100=0;
		if LCI_DD_CIE=. then LCI_DD_CIE=0;

		/*LCI Globale*/
		LCI_100 = LCI_PE_100 + LCI_DD_100;
		LCI_CIE = LCI_PE_CIE + LCI_DD_CIE;

		if smp_sre = "SMP" and brch_rea = "IP0" then do;
			SMP_PE_100 = capx_100;
			SMP_PE_CIE = capx_cua;
		end;

		if smp_sre = "SMP" and brch_rea = "ID0" then do;
			SMP_DD_100 = capx_100;
			SMP_DD_CIE = capx_cua;
		end;

		if SMP_PE_100=. then SMP_PE_100=0;
		if SMP_PE_CIE=. then SMP_PE_CIE=0;
		if SMP_DD_100=. then SMP_DD_100=0;
		if SMP_DD_CIE=. then SMP_DD_CIE=0;

		/*SMP Global*/
		SMP_100 = SMP_PE_100 + SMP_DD_100;
		SMP_CIE = SMP_PE_CIE + SMP_DD_CIE;

		keep POLICE PRODUIT SMP_100 SMP_CIE LCI_100 LCI_CIE;
	run;

	PROC SQL;
		CREATE TABLE _01_CAPITAUX_AZEC AS
			SELECT POLICE, PRODUIT, (SUM(SMP_100)) AS SMP_100, (SUM(SMP_CIE)) AS SMP_CIE, (SUM(LCI_100)) AS LCI_100, (SUM(LCI_CIE)) AS LCI_CIE
			FROM _00_CAPITAUX_AZEC
			GROUP BY POLICE, PRODUIT;

		/*Ajout du libellé Code_NAF et valeur assurée*/
		CREATE TABLE AZEC_PTF&vision. AS
			SELECT t1.CMARCH, t1.CSEG, t1.CSSSEG, t1.LMARCH, t1.LSEG, t1.LSSSEG, t1.DATAFN, t1.DATFIN, t1.DATRESIL, t1.MOTIFRES, t1.ORIGRES, t1.DATTERME,
				   t1.DTECHANM format = ddmmyy6., t1.DUREE, t1.EFFETPOL, t1.ETATPOL, t1.FINPOL, t1.INTERMED, t1.CODECOAS, t1.COASS, t1.TOP_COASS, t1.
				   TYPE_AFFAIRE, t1.NBAFN, t1.NBPTF, t1.NBRES, t1.POINGEST,t1.PRIME,t1.Cotis_100, t1.PRIMETO, t1.PRIMECUA, t1.PRIMES_AFN, t1.PRIMES_PTF, t1.
				   PRIMES_RES,
				   t1.PRODUIT, t1.VISION, t1.EXEVUE, t1.MOISVUE, t1.POLICE, t1.FORMULE, t1.FORMULE2, t1.FORMULE3, t1.FORMULE4,
				   t1.expo_ytd, t1.expo_gli,t1.CODE_NAF, /*t2.LIB_CONAS,*/ t1.CODE_TRE, t1.NOMCLI, t2.MTCA, t1.PARTCIE, t1.CPCUA,t1.PARTBRUT,t1.DT_DEB_EXPO,t1.
				   NBAFN_ANTICIPE,t1.NBRES_ANTICIPE,
				   t1.TOP_REVISABLE,t1.CRITERE_REVISION,t1.CDGREV,t1.CDNATP
			FROM _01_AZEC_PTF&vision. t1
			/*LEFT JOIN SEG.segmentprdt_&annee. t3 ON (t1.PRODUIT = t3.CDPROD)*/
			LEFT JOIN PE_RD_VI t2 ON (t1.POLICE =t2.POLICE AND t1.PRODUIT = t2.PRODUIT)
			LEFT JOIN _01_CAPITAUX_AZEC t4 ON (t1.POLICE = t4.POLICE AND t1.PRODUIT = t4.PRODUIT);
	QUIT;

	/* SG: Ajustement des NBRES AZEC */
	DATA AZEC_PTF&vision.;
		SET AZEC_PTF&vision.;
		If PRODUIT IN ('D00','TRC','CTR', 'CNR') then do; NBPTF = 0; NBRES = 0; end;
		IF NBRES = 1 AND RMPLCANT NOT IN ('') AND MOTIFRES in ('HP') THEN NBRES = 0 ;
		If NBRES = 1 AND MOTIFRES in ('SE','SA') THEN NBRES = 0 ;
		If NBAFN = 1 and CSSSEG = "5" THEN NBAFN = 0;
	RUN;

	%derniere_version(PT_GEST, ptgst_, &vision., TABLE_PT_GEST);

	/* Ajout des Segment et type_produit_2  */
	proc sql;
		create table AZEC_PTF&vision. as
			select distinct a.*, b.segment as segment2, b.type_produit as type_produit_2,c.CDTOMVCN,c.LDESTLOC,c.MNT_GLO0,c.DATRECEP,c.DEST_LOC,c.DATFINCH,c.
			LQUALITE,d.UPPER_MID /*=c.CATEGPER,c.DATDEBCU,*/
			from        AZEC_PTF&vision. as a
			left join CONSTRUC_AZEC       as b on a.produit = b.cdprod  and a.police = b.police
			left join CONSTRUC.CONSTRUCU  as c on a.produit = c.cdprod  and a.police = c.police
			left join TABLE_PT_GEST       as d ON a.POINGEST = d.PTGST
			order by police
		;
	quit;

%MEND;

```

## PTF_MVTS_AZ_MACRO.sas

```sas
%macro az_mvt_ptf(annee,mois);

	%local a; %let a = %nrstr(%mend);
	/* création des indicateurs Mouvements et PTF Construction */
	%let filtres_ptf = cdri not in ("X")
					and cdsitp not in ("4","5")
					and noint not in ('H90061','4B2001','4B0000','102030','H90036','H90059',
						'H99045','H99059','5B2000','446000','5R0001','446118','4F1004',
						'4A1400','4A1500','4A1600','4A1700','4A1860','4A1900','4B2001',
						'4B9090','4F1004','4L1010')
					and cdprod not in ('01073')
					and cdnatp in('R','O','T', 'C') and cmarch= "6" and csegt="2";

	%let VAR_PTF =  cdprod, nopol, NOCLT,nmclt,posacta AS posacta_ri, rueacta AS rueacta_ri, cediacta AS cediacta_ri, NMACTA,noint, dtcrepol, cdnatp, txcede,
					ptgst, dteffan, dttraan,
					dtresilp, dttraar, cdtypli1, cdtypli2, cdtypli3, dttypl11, dttypl12, dttypl13, cmarch,
					cdsitp, mtprprto, prdccie, csegt AS CSEG, cssegt AS CSSSEG, cdri, CDCIEORI, CDSITMGR,
					cdgecent, Cdmothes, cdpolrvi, nopoli1, CDCASRES, DTECHAMM, CDTPCOA AS CDCOAS, PRDCCIE AS PRCIE,
					(CASE WHEN txcede = . THEN 0 ELSE txcede END) AS tx,

					/*Ajout 2020.02.11 cf mail Alain Cadinot du 03.02.2020*/
					(CASE WHEN cdpolgp1 = '1' THEN 1 ELSE 0 END) AS TOP_COASS,

					(CASE WHEN cdpolgp1 = '1' AND CDTPCOA in ('3', '6') THEN 'APERITION'
						WHEN cdpolgp1 = '1' AND CDTPCOA in ('4', '5') THEN 'COASS. ACCEPTEE'
						WHEN cdpolgp1 = '1' AND CDTPCOA in ('8') THEN 'ACCEPTATION INTERNATIONALE'
						WHEN cdpolgp1 = '1' AND CDTPCOA not in ('3' '4' '5' '6' '8') THEN 'AUTRES'
						WHEN cdpolgp1 <> '1' THEN 'SANS COASSURANCE'
					END) AS COASS,
					(CASE WHEN cdpolgp1 <> '1' THEN 1 /*SANS COASSURANCE*/
						WHEN cdpolgp1 = '1' THEN (PRCDCI / 100) /*AVEC COASSURANCE*/
					END) AS PARTCIE,
					/* 26.06.20:CONTRAT TOP REVISABLE/CRITERE REVISION */

			(CASE WHEN CDPOLRVI= "1" THEN 1
				ELSE 0
			END) AS TOP_REVISABLE,
			(CASE
				/* JFT 13/12/2021 : Nouveaux libellex de critere de revision*/
				/*WHEN CDGREV  ="10" THEN "10_NON_REVISABLE    "*/
				WHEN CDGREV ="20" THEN "20_SANS_PROV_MANUEL "
				WHEN CDGREV ="21" THEN "21_PROV_FIXE_MANUEL "
				WHEN CDGREV ="22" THEN "22_PROV_VAR_MANUEL  "
				WHEN CDGREV ="23" THEN "23_SANS_REVISION    "
				WHEN CDGREV ="24" THEN "24_REV_FIN_CHANTIER "
				WHEN CDGREV ="30" THEN "30_SANS_PROV_MANUEL "
				WHEN CDGREV ="31" THEN "31_PROV_FIXE_MANUEL "
				WHEN CDGREV ="32" THEN "32_PROV_VAR_MANUEL  "
				WHEN CDGREV ="40" THEN "40_SANS_PROV_AUTOMAT"
				WHEN CDGREV ="41" THEN "41_PROV_FIXE_AUTOMAT"
				WHEN CDGREV ="42" THEN "42_PROV_VAR_AUTOMAT "
				WHEN CDGREV ="43" THEN "43_SANS_REV_AUTOMAT "
				WHEN CDGREV ="80" THEN "80_REVISION_SPECIALE"
				WHEN CDGREV ="90" THEN "90_SANS_PROV_AUTOMAT"
				WHEN CDGREV ="91" THEN "91_PROV_FIXE_AUTOMAT"
				WHEN CDGREV ="92" THEN "92_PROV_VAR_AUTOMAT "
			/*
			WHEN CDGREV= "21" THEN "PROVISION FIXE"
			WHEN CDGREV= "22" THEN "PROV. VARIABLE"
			WHEN CDGREV= "23" THEN "SANS REVISION"
			WHEN CDGREV= "41" THEN "PROVISION FIXE"
			WHEN CDGREV= "42" THEN "PROV. VARIABLE"
			WHEN CDGREV= "43" THEN "SANS REVISION"
			WHEN CDGREV= "80" THEN "REV.  SPECIALE"
			*/
			END) AS CRITERE_REVISION,CDGREV,
				/*cf mail Steffi Boussemart du 31.07.2019*/
				(CASE WHEN CDTPCOA not in ('3','4','6') THEN 'SANS COASSURANCE'*/
			/*
			/*         WHEN CDTPCOA='4' THEN 'ACCEPTATION'*/
			/*         WHEN CDTPCOA in ('3','6') THEN 'APERITION'*/
			/*         ELSE ""*/
			/*     END) AS COASS,*/
			/*
			(CASE WHEN CDTPCOA not in ('3','4','6') THEN 1 */
			/*         WHEN CDTPCOA in ('3','4','6') THEN PRCDCI / 100*/
			/*         ELSE .*/
			/*     END) AS PARTCIE,*/

					CDFRACT, QUARISQ, NMRISQ, NMSRISQ, RESRISQ, RUERISQ, LIDIRISQ, POSRISQ, VILRISQ, CDREG,
					MTCAPI1,MTCAPI2,MTCAPI3,MTCAPI4,MTCAPI5,MTCAPI6,MTCAPI7,MTCAPI8,MTCAPI9,MTCAPI10,MTCAPI11,MTCAPI12,MTCAPI13,MTCAPI14,
					LBCAPI1,LBCAPI2,LBCAPI3,LBCAPI4,LBCAPI5,LBCAPI6,LBCAPI7,LBCAPI8,LBCAPI9,LBCAPI10,LBCAPI11,LBCAPI12,LBCAPI13,LBCAPI14,

					0 AS primeto, 0 AS Primes_PTF, 0 AS Primes_AFN, 0 AS Primes_RES, 0 AS Primes_RPT, 0 AS Primes_RPC,
					0 AS NBPTF, 0 AS NBAFN, 0 AS NBRES, 0 AS NBRPT, 0 AS NBRPC, 0 AS TOP_TEMP, 0 AS expo_ytd, 0 AS expo_gli,
					0 AS Cotis_100, 0 as MTCA_, 0 AS TOP_LTA, 0 AS PERTE_EXP, 0 AS RISQUE_DIRECT, 0 AS VALUE_INSURED,
					0 AS SMP_100, 0 AS LCI_100, 0 AS TOP_AOP, "AZ  " AS DIRCOM,CTDEFTTRA,CTPRVTRV,DTOUCHAM,DTRCCPPR,DTRECTXK,
					DTRCPPRE,LBNATTRV,DSTCSC,LBOLISOU,0 AS NBAFN_ANTICIPE,0 AS NBRES_ANTICIPE,
					0 AS DT_DEB_EXPO format= ddmmyy10., 0 AS DT_FIN_EXPO format= ddmmyy10.,ACTPRIM,MTSMPR;

	/* SG : 20/05/2020 : SELECTION DU MARCHE CONSTRUCTION => LIBRAIRIE PTF16 (AGENCE), PTF36(COURTAGE)*/
	PROC SQL;
		CREATE TABLE MVT_CONST_PTF&vision. AS
		(SELECT &VAR_PTF., "1" AS CDPOLE, &vision. AS VISION, &annee. AS EXEVUE, &mois. AS MOISVUE, CDNAF, CDTRE, CDACTPRO, FNCMACA AS MTCA, MTCAF,
			"" as CTIMB, "" as NoSyn, . AS CTDUREE, tydrisi, OPAPOFFR FROM PTF16.IPF WHERE &filtres_ptf.) /*CONST Agent*/

		OUTER UNION CORR
		(SELECT &VAR_PTF., "3" AS CDPOLE, &vision. AS VISION, &annee. AS EXEVUE, &mois. AS MOISVUE, CDNAF, CDTRE, CDACTPRO, FNCMACA AS MTCA, MTCAF,
			"" AS CTIMB, "" as NoSyn, . AS CTDUREE, tydrisi, OPAPOFFR FROM PTF36.IPF WHERE &filtres_ptf.)/*CONST Courtage*/
		;

		/* Extraction des motants de chiffres d'affaires pour le produit 01099 */
		CREATE TABLE mvt_ptfa&vision. AS
		(SELECT "1" AS CDPOLE, &vision. AS VISION, CDPROD, NOPOL, NOINT, MTCA, MTCAENP, MTCASST, MTCAVNT  FROM PTF16a.IPFM99 WHERE CDPROD = '01099')/*CONST
		Agent*/
		OUTER UNION CORR
		(SELECT "3" AS CDPOLE, &vision. AS VISION, CDPROD, NOPOL, NOINT, MTCA, MTCAENP, MTCASST, MTCAVNT  FROM PTF36a.IPFM99 WHERE CDPROD = '01099')/*CONST
		Courtage*/
		;

		CREATE TABLE MVT_CONST_PTF&vision. AS
		(select a.*, b.MTCAENP, b.MTCASST, b.MTCAVNT
		from MVT_CONST_PTF&vision. as a
		left join mvt_ptfa&vision. as b on (a.CDPOLE = b.CDPOLE and a.CDPROD = b.CDPROD and  a.NOPOL =  b.NOPOL and  a.NOINT =  b.NOINT));

		/* Mise à jour des motants de chiffres d'affaires pour le produit 01099 */
		UPDATE MVT_CONST_PTF&vision. set MTCA = MTCAENP+MTCASST+MTCAVNT
		WHERE CDPROD = '01099';

		/*Calcul des la perte d'exploitation, du risque directe i.e. de la valeur assurée, du SMP et de la LCI (définition du garest)*/
		%Do k=1 %To 14;
			/*Cf Gareat Entreprises & Garages IRD IMS.sas Steffi Boussemart*/
			/* UPDATE MVT_CONST_PTF&vision. SET GAREAT_100 = mtcapi&k. WHERE index(lbcapi&k.,"GAREAT")>0;*/
			/* LCI Global du Contrat:*/
			UPDATE MVT_CONST_PTF&vision. SET LCI_100 = mtcapi&k.
				WHERE (index(lbcapi&k.,"LCI GLOBAL DU CONTRAT")>0
					or index(lbcapi&k.,"CAPITAL REFERENCE OU LCI")>0
					or index(lbcapi&k.,"LCI IRD (DOM. DIRECTSIP.EXPLOIT)")>0
					or index(lbcapi&k.,"LIMITE CONTRACTUELLE INDEMNITE")>0)
			;
			/*        SMP Global du Contrat;*/
			UPDATE MVT_CONST_PTF&vision. SET SMP_100 = mtcapi&k.
				WHERE (index(lbcapi&k.,"SMP GLOBAL DU CONTRAT")>0
					or index(lbcapi&k.,"SMP RETENU")>0
					or index(lbcapi&k.,"SINISTRE MAXIMUM POSSIBLE")>0);
			/* RISQUE_DIRECT:*/
			UPDATE MVT_CONST_PTF&vision. SET RISQUE_DIRECT = mtcapi&k.
				WHERE (index(lbcapi&k.,"RISQUE DIRECT")>0 and lbcapi&k. not in ("SINIS MAX POSSIBLE RISQUE DIRECT"))
					or index(lbcapi&k.,"CAPITAUX DOMMAGES DIR")>0);
			/*        SMP Risque Direct;*/
			/*        UPDATE MVT_CONST_PTF&vision. SET MaxRD100 = mtcapi&k. WHERE index(lbcapi&k.,"SINIS MAX POSSIBLE RISQUE DIRECT")>0;*/
			/* Perte d Exploitation;*/
			UPDATE MVT_CONST_PTF&vision. SET PERTE_EXP = mtcapi&k.
				WHERE index(lbcapi&k., "PERTE EXPLOITATION (MARGE BRUTE)")>0
					or index(lbcapi&k., "PERTE D EXPLOITATION")>0
					or index(lbcapi&k., "PERTE D'EXPLOITATION")>0
					or index(lbcapi&k., "CAPITAL PERTES EXPLOITATION")>0
					or index(lbcapi&k., "CAPITAUX TOTAUX P.E.")>0
					or index(lbcapi&k., "PERTE EXPLOITATION/AUT. DOM. MAT")>0
					/*Ajout 2020.02.18 AAV*/
					or index(lbcapi&k., "PERTES D'EXPLOITATION")>0
					or index(lbcapi&k., "PERTES EXPLOITATION")>0
			;
			/*        LCI/SMP Perte d Exploitation;*/
			/*        UPDATE MVT_CONST_PTF&vision. SET MaxPE100 = mtcapi&k. WHERE index(lbcapi&k.,"LCI/SMP PERTE EXPLOITATION")>0;*/
		%End;


		/*Primes portefeuille*/
		UPDATE MVT_CONST_PTF&vision. SET primeto = mtprprto * (1 - tx / 100);
		UPDATE MVT_CONST_PTF&vision. SET TOP_LTA = 1 WHERE ((CTDUREE > 1) and (tydrisi in ("QAM" "QBJ" "QBK" "QBB" "QBM")));

		/*MVT CONST*/
		/*Nombre de contrats en PTF*/
		UPDATE MVT_CONST_PTF&vision.
			SET NBPTF = 1, Primes_PTF = primeto
			WHERE (cssseg ne "5" and CDNATP in ('R','O') and ((cdsitp in('1') and dtcrepol <= "&DTFIN."d) or (cdsitp = '3' and dtresilp>"&DTFIN."d)));
		/*Les affaires nouvelles*/
		UPDATE MVT_CONST_PTF&vision.
			SET NBAFN = 1, Primes_AFN = primeto
			WHERE ((("&DTDEB_AN"d<=dteffan<="&DTFIN"d) and ("&DTDEB_AN"d<=dttraan<="&DTFIN"d))
				or (("&DTDEB_AN"d<=dtcrepol<="&DTFIN"d))
				or (year(dteffan)<year("&DTFIN"d)and ("&DTDEB_AN"d<=dttraan<="&DTFIN"d)));
		/*Les résiliations SG: 24/05/2020, on écart les Chantiers du dénombrement CDNATP= 'C' */
		UPDATE MVT_CONST_PTF&vision.
			SET NBRES = 1, Primes_RES = primeto
			WHERE CDNATP ne 'C' and ((cdsitp='3' and ("&DTDEB_AN"d<=dtresilp<="&DTFIN"d))
				or (cdsitp='3' and ("&DTDEB_AN"d<=dtraar<="&DTFIN"d)and dtresilp<="&DTFIN"d));


		/*Les remplacements*/
		UPDATE MVT_CONST_PTF&vision.
			SET NBRES = 0, Primes_RES = 0, NBRPC = 1, Primes_RPC = primeto
			WHERE NBRES = 1 And ((cdtypli1 in('RP') and year(dttypl11)=year("&DTFIN"d))
				or (cdtypli2 in('RP') and year(dttypl12)=year("&DTFIN"d))
				or (cdtypli3 in('RP') and year(dttypl13)=year("&DTFIN"d)));

		/*Les remplaçants*/
		UPDATE MVT_CONST_PTF&vision.
			SET NBAFN = 0, Primes_AFN = 0, NBRPT = 1, Primes_RPT = primeto
			WHERE NBAFN = 1 And ((cdtypli1 in('RE') and year(dttypl11)=year("&DTFIN"d))
				or (cdtypli2 in('RE') and year(dttypl12)=year("&DTFIN"d))
				or (cdtypli3 in('RE') and year(dttypl13)=year("&DTFIN"d)));

		/*création d'un top temporaire*/
		UPDATE MVT_CONST_PTF&vision. SET TOP_TEMP = 1 WHERE cdnatp = "T";

		/* Expo RIS � la maille contrat*/
		UPDATE MVT_CONST_PTF&vision.
			SET expo_ytd = (MAX(0,(MIN(dtresilp, "&DTFIN"d)- MAX(dtcrepol, "&DTDEB_AN"d)+1)) / ((mdy(12,31,&annee.) - "&DTDEB_AN"d) + 1),
				expo_gli = (MAX(0,(MIN(dtresilp, "&DTFIN"d)- MAX(dtcrepol, "&dtfinmm1"d+1)+1)) / ("&DTFIN"d - "&dtfinmm1"d),
				DT_DEB_EXPO = MAX(dtcrepol, "&DTDEB_AN"d),
				DT_FIN_EXPO = MIN(dtresilp, "&DTFIN"d)

		/*selection de ce portefeuille pour le calcul des freq (expo) dans le code de FG. cf ptf_freq_az_v3 aitihvi/saspgm/freq */
		WHERE (cdnatp in ("R","O") and ((cdsitp = '1' and dtcrepol <= "&dtfinmm"d) or (cdsitp = '3' and dtresilp>"&dtfinmm"d))
			or (cdsitp in ('1' '3') and ((dtcrepol <= "&dtfinmm"d and (dtresilp=. or "&dtfinmm1"d <= dtresilp < "&dtfinmm"d))
				or (dtcrepol <= "&dtfinmm"d and dtresilp  > "&dtfinmm"d) or ("&dtfinmm1"d < dtcrepol <= "&dtfinmm"d and "&dtfinmm"d < dtresilp))
			and cdnatp not in ('F'));

		UPDATE MVT_CONST_PTF&vision. SET PRCDCIE = 100 WHERE PRCDCIE = . or PRCDCIE = 0;

		/* Cotisation technique � 100% cf Steffi Boussemart code du GAREAT */
		UPDATE MVT_CONST_PTF&vision. SET Cotis_100 = mtprprto;
		UPDATE MVT_CONST_PTF&vision. SET Cotis_100 = (mtprprto*100)/prcdcie WHERE (TOP_COASS = 1 AND CDCOAS in ('4', '5')); /*i.e. COASS ACCEPTEE*/

		/* Centralisation du CA dans 1 colonne */
		UPDATE MVT_CONST_PTF&vision. SET MTCA = 0 WHERE MTCA = .;
		UPDATE MVT_CONST_PTF&vision. SET MTCAF = 0 WHERE MTCAF = .;
		UPDATE MVT_CONST_PTF&vision. SET MTCA_ = MTCAF + MTCA;

		/*Ajout 2020.02.11 TOP_AOP cf mail d'Alain Voss du 06.02.2020*/
		UPDATE MVT_CONST_PTF&vision. SET TOP_AOP = 1 WHERE OPAPOFFR = 'O';

		/*SG 21/07/2020 IOP migration AZEC */
		/*UPDATE MVT_CONST_PTF&vision. SET DIRCOM = "AZEC" WHERE (CDCIEORI = '63' AND CDSITMGR = 'Z' AND substr(NOPOL,1,1)='Z');*/

		UPDATE MVT_CONST_PTF&vision. SET NBAFN_ANTICIPE = 1
			WHERE ((dteffan > &finmois.)  OR (dtcrepol>&finmois. ))
				AND (NOT (cdtypli1 in('RE') or cdtypli2 in('RE') or cdtypli3 in('RE')));

		UPDATE MVT_CONST_PTF&vision. SET NBRES_ANTICIPE = 1
			WHERE (dtresilp>&FINMOIS. )
				AND (NOT (cdtypli1 in('RP') or cdtypli2 in('RP') or cdtypli3 in('RP') or  cdmotres='R' or cdcASres='2R'));

	QUIT;

	data MVT_CONST_PTF&vision.;
		set MVT_CONST_PTF&vision.;
		if  EXPO_YTD = 0 then do; DT_DEB_EXPO = . ; DT_FIN_EXPO = . ;end;
		If  NMCLT in(" ") then NMCLT=NMACTA; else NMCLT=NMCLT;
	run;

	DATA Segment1  ;
		ATTRIB lmarch2 FORMAT = $46. ;
		ATTRIB lseg2   FORMAT = $54. ;
		ATTRIB lssseg2 FORMAT = $60. ;

		SET SEGMprdt.PRDPFA1  (KEEP = cmarch lmarch cseg lseg cssseg lssseg CPROD lprod )  ;
		WHERE     cmarch IN ('6') ;
			lmarch2 =       (cmarch||"_"||lmarch);
			lseg2   =       (cseg||"_"||lseg);
			lssseg2 =       (cssseg||"_"||lssseg);
			reseau  =       '1' ;

		KEEP CPROD  lprod cseg lseg2 cssseg lssseg2 cmarch lmarch2 reseau ;
	RUN ;
	PROC SORT DATA  =  Segment1 NODUPKEY; BY CPROD; RUN;

	DATA Segment3  ;
		ATTRIB lmarch2 FORMAT = $46. ;
		ATTRIB lseg2   FORMAT = $54. ;
		ATTRIB lssseg2 FORMAT = $60. ;

		SET SEGMprdt.PRDPFA3  ( KEEP = cmarch lmarch cseg lseg cssseg lssseg CPROD lprod )  ;
		WHERE cmarch IN ('6');
			lmarch2 =       (cmarch||"_"||lmarch);
			lseg2   =       (cseg||"_"||lseg);
			lssseg2 =       (cssseg||"_"||lssseg);
			reseau  =       '3' ;

		KEEP CPROD  lprod cseg lseg2 cssseg lssseg2 cmarch lmarch2 reseau ;
	RUN ;
	PROC SORT DATA  =  Segment3   NODUPKEY ;  BY CPROD;  RUN ;

	DATA Cproduit  ;
		SET AACPrtf.Cproduit       ;
	RUN;
	PROC SORT DATA  =  Cproduit NODUPKEY;   BY CPROD;  RUN  ;

	DATA Segment ; SET Segment1 Segment3 ; RUN ;
	PROC SORT DATA  =  Segment;   BY CPROD;  RUN  ;

	DATA  Segment;
		MERGE  Segment     (  IN  =  a  )
			Cproduit    (  IN  =  b KEEP = CPROD Type_Produit_2 segment Segment_3 )  ;
		BY        CPROD  ;
		IF        a  ;
	RUN  ;
	PROC  SORT  DATA  =  Segment  ;  BY  CPROD  ;  RUN  ;

	DATA        PRDCAPa      (  KEEP  =  CPROD  lprod  );

	ATTRIB CPROD      FORMAT  =  $5.   ;
	ATTRIB lprod      FORMAT  =  $34.  ;

	SET PRDCAP.PRDCAP   (  KEEP  =  cdprod  lbtprod  )  ;
		CPROD       =   cdprod  ;
		lprod       =   lbtprod  ;

	RUN       ;
	PROC  SORT  DATA  =  PRDCAPa   NODUPKEY;   BY   CPROD   ;   RUN   ;

	DATA Segment1;
		MERGE Segment (IN = a DROP = lprod)
			PRDCAPa (IN = b) ;
		BY CPROD ;
		IF a and b ;
	RUN  ;

	DATA  Segment2;
		MERGE  Segment     (  IN  =  a  )
			PRDCAPa     (  IN  =  b")  ;
		BY CPROD ;
		IF a and not b ;
	RUN  ;

	DATA  Segment   ;
		SET  Segment1 Segment2 ;
	RUN;
	PROC SORT DATA  =  Segment   NODUPKEY  ;   BY  reseau CPROD   ;   RUN   ;

	%if &vision. <=201112 %then %do;
		data TABLE_PT_GEST;
			set PT_GEST.PTGST_201201;
		run;
	%end;
	%else %do;
			%derniere_version(PT_GEST, ptgst_, &vision., TABLE_PT_GEST);
	%end;


	/* Ajout des données segment, type_produit_2 et Upper_Mid */
	proc sql;
		create table MVT_CONST_PTF&vision. as
		select a.*, b.segment as segment2, b.type_produit_2, c.Upper_MID
			from MVT_CONST_PTF&vision. a
			left join segment              b on a.cdprod = b.cprod and a.CDPOLE = b.reseau
			left join TABLE_PT_GEST        c on a.PTGST  = c.PTGST
		order by nopol, cdsitp ;
	quit;

	proc sort data=MVT_CONST_PTF&vision. nodupkey; by nopol; run;

%MEND;

```

## PTF_MVTS_CHARG_DATARISK_ONEBI_BIFR.sas

```sas
%macro chargement_data_Risk;

%if &vision >= 202210 %then %do;

    %if "&SYSENCODING." = "utf-8" %then
        %do;
        
            /* Connexion a One BI */
            options source compress=yes;
            
            %let server_to_use= biaa-sg-prod.srv.company;
            %let port_connect =7556;
            options netencryptalgorithm=(AES);
            %let remhost=&server_to_use &port_connect;
            options comamid=tcp remote=remhost;
            signon noscript userid=%upcase(&sysuserid.) password="&MPSSOX";
            
        %end;
    
    
    %if %sysfunc(libref(risk)) %then
        %do;
        
            rsubmit;
                /* ALLOCATION BASES ONE BI sur une session distante */
                libname risk "/var/opt/data/data_project/sas94/bifr/azfronebi/data/shared_lib/IMS_CONTRAT/ird_risk/archives_mensuelles" ;
            endrsubmit;
            
            /* Rendre ris et work visibles sur SASApp_AMOS */
            libname risk server=remhost;
            libname rwork slibref=work server=remhost outencoding='latin9';
            
        %end;
    %else %do;
        %put libref exists;
    %end;
    
    /* FIN DES LIBRAIRIES ONE-BI */
    
    /* LIBRAIRIES DTM CONSTRUCTION */
    /*%let vision = &VISIO;*/
    
    
    x "mkdir /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    LIBNAME OUT "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision." outencoding='latin9';
    
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/02_TRANSCODIFICATION_ABS.sas";
    
    %syslput vision = &vision.;
    
    rsubmit;
    
    /*
        proc sql;
            create table IRD_RISK_Q46_&vision. as
            select  *
            from    risk.IRD_RISK_Q46_&vision. ;
        quit;
    */
    
        data IRD_RISK_Q46_&vision.;
            set risk.IRD_RISK_Q46_&vision.(encoding = "latin9");
            array allchar{*} _CHARACTER_;
            do i=1 to dim(allchar);
                allchar{i}= tranwrd(allchar{i},"é","�");
                allchar{i}= tranwrd(allchar{i},"è","�");
                allchar{i}= tranwrd(allchar{i},"ê","�");
                allchar{i}= tranwrd(allchar{i},"à","�");
                allchar{i}= tranwrd(allchar{i},"â","�");
                allchar{i}= tranwrd(allchar{i},"ù","�");
                allchar{i}= tranwrd(allchar{i},"û","�");
                allchar{i}= tranwrd(allchar{i},"ô","�");
                allchar{i}= tranwrd(allchar{i},"ç","�");
                allchar{i}= tranwrd(allchar{i},"î","�");
                allchar{i}= tranwrd(allchar{i},"°","�");
            end;
            drop i;
        run;
        
        proc download data=IRD_RISK_Q46_&vision.
                      out =OUT.IRD_RISK_Q46_&vision.; run;
        
        data IRD_RISK_Q45_&vision.;
            set risk.IRD_RISK_Q45_&vision.(encoding = "latin9");
            array allchar{*} _CHARACTER_;
            do i=1 to dim(allchar);
                allchar{i}= tranwrd(allchar{i},"é","�");
                allchar{i}= tranwrd(allchar{i},"è","�");
                allchar{i}= tranwrd(allchar{i},"à","�");
                allchar{i}= tranwrd(allchar{i},"â","�");
                allchar{i}= tranwrd(allchar{i},"ù","�");
                allchar{i}= tranwrd(allchar{i},"û","�");
                allchar{i}= tranwrd(allchar{i},"ô","�");
                allchar{i}= tranwrd(allchar{i},"ç","�");
                allchar{i}= tranwrd(allchar{i},"î","�");
                allchar{i}= tranwrd(allchar{i},"°","�");
            end;
            drop i;
        run;
        
        proc download data=IRD_RISK_Q45_&vision.
                      out =OUT.IRD_RISK_Q45_&vision.; run;
        
        data IRD_RISK_QAN_&vision.;
            set risk.IRD_RISK_QAN_&vision.(encoding = "latin9");
            array allchar{*} _CHARACTER_;
            do i=1 to dim(allchar);
                allchar{i}= tranwrd(allchar{i},"é","�");
                allchar{i}= tranwrd(allchar{i},"è","�");
                allchar{i}= tranwrd(allchar{i},"à","�");
                allchar{i}= tranwrd(allchar{i},"â","�");
                allchar{i}= tranwrd(allchar{i},"ù","�");
                allchar{i}= tranwrd(allchar{i},"û","�");
                allchar{i}= tranwrd(allchar{i},"ô","�");
                allchar{i}= tranwrd(allchar{i},"ç","�");
                allchar{i}= tranwrd(allchar{i},"î","�");
                allchar{i}= tranwrd(allchar{i},"°","�");
            end;
            drop i;
        run;
        
        proc download data=IRD_RISK_QAN_&vision.
                      out =OUT.IRD_RISK_QAN_&vision.; run;
    
    
    endrsubmit;
    
%end;

%mend;
%chargement_data_Risk;

```

## PTF_MVTS_CONSOLIDATION_MACRO.sas

```sas
%macro consolidation_az_azec_mvt_ptf;

PROC SQL;
    /*Portefeuille az + azec*/
    CREATE TABLE MVT_PTF&vision. AS (SELECT /*"AZ" "AS"*/ NOPOL1,
        NOPOL11B, DIRCOM, CDPOLE, NOINT, CDPROD, CDMACH, CSEG, CSSSEG,
        DTECHDM,ponacta_f1, runacta_r1, cediacta_r1,
        input(substr(put(DTECHAMM,5.),length(put(DTECHAMM,5.))-3,2),4.)
        AS MOIS_ECHEANCE,
        input(substr(put(DTECHAMM,5.),length(put(DTECHAMM,5.))-1,2),4.)
        AS JOUR_ECHEANCE, DTRESLIP, DTTRAMM, CDMATP, CDSTTP, PTGST,
        CDREG, CDCEEENT, MTCA, CDNAF, /*"LIB_CDNAF"*/ (CASE
        substr(CDTRE,1,1) when '*' THEN substr(CDTRE,2,3) ELSE CDTRE
        END) AS CDTRE, CDCOAS AS CDCOASS, COASS, TOP_COASS,
        /*Dans les on a uniquement des affaires directes*/ "0" AS
        TYPE_AFFAIRE, Primeto as PRIMES_PTF_INTERP, Cotis_100 as
        PRIMES_PTF_100_INTERP, PARTCIE as PART_CIE, PRIMES_PTF, NBPFT,
        EXPO_YTD, EXPO_GLI, TOP_TEMP,
        /*TOP_LTA, TOP_AOP, PERTE_EXP, RISQUE_DIRECT, VALUE_INSURED, SMP_100, LCI_100,*/
        CDMOTRES, CDCASRES, CDPOLVIT, VISION, EXECMUE, MOISVUE,NOCLT,
        NOCLT,/*"CDACTPRO"*/ CDFACTPT, QUARISQ, NMMRISQ, NMRISQ, RESRISQ,
        RUERISQ, LIDIRISQ, POSRISQ, VILRISQ, NBAPN, NBRES,
        NBAPN_ANTICIPE, NBRES_ANTICIPE, PRIMES_APM,
        PRIMES_RES,UPPER_MID,DT_DEB_EXPO,DT_FIN_EXPO, segment2,
        type_produit_2,CDRETPAR,D_DESCCR_LOC as DSTCSG,DATDMCH
        as DTOUCHM,ACTPDIM,CTPRVTRY,MTSMPR,LBNATTRV,
        TOP_REVISABLE,CRITERE_REVISION,CDGREV,NBRPT,NBRPC,Primes_BPC,Primes_BPT, MTCEAMP, MTCASST, MTCAVNT
        FROM MVT_CONST_PTF&vision.) OUTER UNION CORR (SELECT POLICE AS
        NOPOL , POLICE AS NOPOL11B, "AZEC" AS DIRCOM, "I" AS CDPOLE,
        INTERMED AS NOINT, PRODUIT AS CDPROD, CMARCH, CSEG, CSSSEG,
        EFFECTROL AS DTECHDM, month(DTECHAMM) AS MOIS_ECHEANCE,
        day(DTECHAMM) AS JOUR_ECHEANCE, DATEPIN AS DTRESLIP, DATRESLI AS
        DTTRAMM, CDMATP, "" AS CDSTTP, POIDGEST AS PTGST, "" AS CDREG, .
        AS CDCEEENT, MTCA, CDNAF, /*"LIB_CDNAF"*/ (CASE substr(CODE_TRE,1,1)
        when '*' THEN substr(CODE_TRE,2,3) ELSE CODE_TRE END) AS CDTRE,
        CDCOAS AS CDCOASS, COASS, TYPE_AFFAIRE,
        /*Dans les on a uniquement des affaires directes*/ "0" AS
        TYPE_AFFAIRE, Primeto as PRIMES_PTF_INTERP, Cotis_100 as
        PRIMES_PTF_100_INTERP, PARTCIE as PART_CIE, PRIMES_PTF, NBPFT,
        EXPO_YTD, EXPO_GLI, TOP_TEMP,
        /*TOP_LTA, .AS TOP_AOP, PERTE_EXP, RISQUE_DIRECT, VALUE_INSURED, SMP_100, LCI_100,*/
        /*MODIFICATION motivation resiliation AZEC*/ (CASE
        ORIGRES WHEN "LT" THEN "A" WHEN "AP" THEN "L" WHEN "LI" THEN "Z"
        WHEN "NN" THEN "" ELSE "X" END) AS CDMOTRES, MOTIFRES AS CDCASRES,
        "" AS CDPOLVIT, VISION, EXECMUE, MOISVUE, "" AS NOCLT,NOCLT AS
        NOCLT, /*"" AS CDACTPRO"*/ "" AS CDFACTPT, "" AS QUARISQ, "" AS
        NMMRISQ, "" AS NMRISQ, "" AS RESRISQ, "" AS RUERISQ,
        LIDIRISQ, "" AS POSRISQ,EFFECTROL AS DTFEAPN, EFFETPCL AS
        DTTERMI, "" AS VILRISQ, NBAPN, NBRES,
        NBRES_ANTICIPE,PRIMES_APM, PRIMES_RES, segment2,
        type_produit_2,CDRETPAR,D_DESCCR_LOC as DSTCSG,DATDMCH
        as DTOUCHM,DATRECEP as DTRECTRX, DATFINCH as DTRCPPR,LIQUALITE
        as DTRISQU,UPPER_MID,DT_DEB_EXPO,DT_FIN_EXPO ,"" as ACTPDIM,.
        as CTPRVTRY,. as MTSMPR,." as
        LBNATTRV,TOP_REVISABLE,CRITERE_REVISION,CDGREV,0 as NBRPT,0 as
        NBRPC,0 as Primes_BPC,0 as Primes_BPT, 0 as MTCEAMP, 0 as MTCASST, 0 as MTCAVNT FROM AZEC_PTF&vision.);
    QUIT;
%end;
%else %do;

    PROC SQL;
        /*Portefeuille az */
        CREATE TABLE MVT_PTF&vision. AS (SELECT /*"AZ" "AS"*/ NOPOL1,
            NOPOL11B, DIRCOM, CDPOLE, NOINT, CDPROD, CDMACH, CSEG, CSSSEG,
            DTECHDM,ponacta_f1, runacta_r1, cediacta_r1,
            input(substr(put(DTECHAMM,5.),length(put(DTECHAMM,5.))-3,2),4.)
            AS MOIS_ECHEANCE,
            input(substr(put(DTECHAMM,5.),length(put(DTECHAMM,5.))-1,2),4.)
            AS JOUR_ECHEANCE, DTRESLIP, DTTRAMM, CDMATP, CDSTTP, PTGST,
            CDREG, CDCEEENT, MTCA, CDNAF, /*"LIB_CDNAF"*/ (CASE
            substr(CDTRE,1,1) when '*' THEN substr(CDTRE,2,3) ELSE CDTRE
            END) AS CDTRE, CDCOAS AS CDCOASS, COASS, TOP_COASS,
            /*Dans les on a uniquement des affaires directes*/ "0" AS
            TYPE_AFFAIRE, Primeto as PRIMES_PTF_INTERP, Cotis_100 as
            PRIMES_PTF_100_INTERP, PARTCIE as PART_CIE, PRIMES_PTF, NBPFT,
            EXPO_YTD, EXPO_GLI, TOP_TEMP,
            /*TOP_LTA, TOP_AOP, PERTE_EXP, RISQUE_DIRECT, VALUE_INSURED, SMP_100, LCI_100,*/
            CDMOTRES, CDCASRES, CDPOLVIT, VISION, EXECMUE, MOISVUE,NOCLT,
            NOCLT,/*"CDACTPRO"*/ CDFACTPT, QUARISQ, NMMRISQ, NMRISQ, RESRISQ,
            RUERISQ, LIDIRISQ, POSRISQ, VILRISQ, NBAPN, NBRES,
            NBAPN_ANTICIPE, NBRES_ANTICIPE, PRIMES_APM,
            PRIMES_RES,UPPER_MID,DT_DEB_EXPO,DT_FIN_EXPO, segment2,
            type_produit_2,CDRETPAR,D_DESCCR_LOC as DSTCSG,DATDMCH
            as DTOUCHM,ACTPDIM,CTPRVTRY,MTSMPR,LBNATTRV,
            TOP_REVISABLE,CRITERE_REVISION,CDGREV,NBRPT,NBRPC,Primes_BPC,Primes_BPT, MTCEAMP, MTCASST, MTCAVNT
            FROM MVT_CONST_PTF&vision.);
    QUIT;
%end;

%if &vision. >= 202210 %then %do;

    /* Q45 */
    data ird_risk_q46_&vision.;
        set cube.ird_risk_q46_&vision.;
        format DTOUCHM_RISK ddmmyy10. DTRECTRX_RISK ddmmyy10. DTREFFIN_RISK
            ddmmyy10. ;
        DTOUCHM_RISK=datepart(DTOUCHM);
        DTRECTRX_RISK=datepart(DTRECTRX);
        DTREFFIN_RISK=datepart(DTREFFIN);
        CTPRVTRV_RISK=CTPRVTRV ;
        CTDEFFRA_RISK=CTDEFFRA ;
        LBNATTRV_RISK=LBNATTRV;
        LBDSTCSC_RISK=LBDSTCSC;
        keep NOPOL DTOUCHM_RISK DTRECTRX_RISK DTREFFIN_RISK CTPRVTRV_RISK
            CTDEFFRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
    run;

    proc sql;
        create table MVT_PTF&vision. as select a.*,b.* from MVT_PTF&vision.
            a left join ird_risk_q46_&vision. b on a.nopol=b.nopol;
    quit;

    data MVT_PTF&vision.;
        set MVT_PTF&vision.;

        if missing(DTOUCHM) then DTOUCHM=DTOUCHM_RISK;
        if missing(DTRECTRX) then DTRECTRX=DTRECTRX_RISK;
        if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
        if missing(CTDEFFRA) then CTDEFFRA=CTDEFFRA_RISK;
        if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
        if missing(DTREFFIN) then DTREFFIN=DTREFFIN_RISK;
        if missing(LBDSTCSC) then LBDSTCSC=LBDSTCSC_RISK;
        drop DTOUCHM_RISK DTRECTRX_RISK CTPRVTRV_RISK CTDEFFRA_RISK
            LBNATTRV_RISK DTREFFIN_RISK LBDSTCSC_RISK;
    run;

    data ird_risk_q45_&vision.;
        set cube.ird_risk_q45_&vision.;
        format DTOUCHM_RISK ddmmyy10. DTRECTRX_RISK ddmmyy10. DTREFFIN_RISK
            ddmmyy10. ;
        DTOUCHM_RISK=datepart(DTOUCHM);
        DTRECTRX_RISK=datepart(DTRECTRX);
        DTREFFIN_RISK=datepart(DTREFFIN);
        CTPRVTRV_RISK=CTPRVTRV ;
        CTDEFFRA_RISK=CTDEFFRA ;
        LBNATTRV_RISK=LBNATTRV;
        LBDSTCSC_RISK=LBDSTCSC;
        keep NOPOL DTOUCHM_RISK DTRECTRX_RISK DTREFFIN_RISK CTPRVTRV_RISK
            CTDEFFRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
    run;

    proc sql;
        create table MVT_PTF&vision. as select a.*,b.* from MVT_PTF&vision.
            a left join ird_risk_q45_&vision. b on a.nopol=b.nopol;
    quit;

    data MVT_PTF&vision.;
        set MVT_PTF&vision.;

        if missing(DTOUCHM) then DTOUCHM=DTOUCHM_RISK;
        if missing(DTRECTRX) then DTRECTRX=DTRECTRX_RISK;
        if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
        if missing(CTDEFFRA) then CTDEFFRA=CTDEFFRA_RISK;
        if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
        if missing(DTREFFIN) then DTREFFIN=DTREFFIN_RISK;
        if missing(LBDSTCSC) then LBDSTCSC=LBDSTCSC_RISK;
        drop DTOUCHM_RISK DTRECTRX_RISK CTPRVTRV_RISK CTDEFFRA_RISK
            LBNATTRV_RISK DTREFFIN_RISK LBDSTCSC_RISK;
    run;
%end;
%else %do;

    data ird_risk_qan_&vision.;
        set cube.ird_risk_qan_&vision.;
        format DTOUCHM_RISK ddmmyy10. DTRCPRPR_RISK ddmmyy10. ;
        DTOUCHM_RISK=datepart(DTOUCHM);
        DTRCPRPR_RISK=datepart(DTRCPPR);
        CTPRVTRV_RISK=CTPRVTRV ;
        CTDEFFRA_RISK=CTDEFFRA ;
        LBNATTRV_RISK=LBNATTRV;
        LBDSTCSC_RISK=LBDSTCSC;
        keep NOPOL DTOUCHM_RISK DTRCPRPR_RISK DTREFFIN_RISK CTPRVTRV_RISK
            CTDEFFRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
    run;

    proc sql;
        create table MVT_PTF&vision. as select a.*,b.* from MVT_PTF&vision.
            a left join ird_risk_qan_&vision. b on a.nopol=b.nopol;
    quit;

    data MVT_PTF&vision.;
        set MVT_PTF&vision.;

        if missing(DTOUCHM) then DTOUCHM=DTOUCHM_RISK;
        if missing(DTRCPPR) then DTRCPPR=DTRCPRPR_RISK;
        if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
        if missing(CTDEFFRA) then CTDEFFRA=CTDEFFRA_RISK;
        if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
        if missing(LBDSTCSC) then LBDSTCSC=LBDSTCSC_RISK;
        drop DTOUCHM_RISK DTRCPRPR_RISK CTPRVTRV_RISK CTDEFFRA_RISK
            LBNATTRV_RISK LBDSTCSC_RISK;
    run;
%end;

%if &vision. >= 202210 %then %do;
%end;
%else %do;

    data ird_risk_qan_202210;
        set RISK_REF.ird_risk_qan_202210;
        format DTOUCHM_RISK ddmmyy10. DTRECTRX_RISK ddmmyy10. DTREFFIN_RISK
            ddmmyy10. ;
        DTOUCHM_RISK=datepart(DTOUCHM);
        DTRECTRX_RISK=datepart(DTRECTRX);
        DTREFFIN_RISK=datepart(DTREFFIN);
        CTPRVTRV_RISK=CTPRVTRV ;
        CTDEFFRA_RISK=CTDEFFRA ;
        LBNATTRV_RISK=LBNATTRV;
        LBDSTCSC_RISK=LBDSTCSC;
        keep NOPOL DTOUCHM_RISK DTRECTRX_RISK DTREFFIN_RISK CTPRVTRV_RISK
            CTDEFFRA_RISK LBNATTRV_RISK LBDSTCSC_RISK;
    run;

    proc sql;
        create table MVT_PTF&vision. as select a.*,b.* from MVT_PTF&vision.
            a left join ird_risk_q46_202210 b on a.nopol=b.nopol;
    quit;

    data MVT_PTF&vision.;
        set MVT_PTF&vision.;

        if missing(DTOUCHM) then DTOUCHM=DTOUCHM_RISK;
        if missing(DTRECTRX) then DTRECTRX=DTRECTRX_RISK;
        if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
        if missing(CTDEFFRA) then CTDEFFRA=CTDEFFRA_RISK;
        if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
        if missing(DTREFFIN) then DTREFFIN=DTREFFIN_RISK;
        if missing(LBDSTCSC) then LBDSTCSC=LBDSTCSC_RISK;
        drop DTOUCHM_RISK DTRECTRX_RISK CTPRVTRV_RISK CTDEFFRA_RISK
            LBNATTRV_RISK DTREFFIN_RISK LBDSTCSC_RISK;
    run;

    data ird_risk_q45_202210;
        set RISK_REF.ird_risk_q45_202210;
        format DTOUCHM_RISK ddmmyy10. DTRECTRX_RISK ddmmyy10. DTREFFIN_RISK
            ddmmyy10. ;
        DTOUCHM_RISK=datepart(DTOUCHM);
        DTRECTRX_RISK=datepart(DTRECTRX);
        DTREFFIN_RISK=datepart(DTREFFIN);
        CTPRVTRV_RISK=CTPRVTRV ;
        CTDEFFRA_RISK=CTDEFFRA ;
        LBNATTRV_RISK=LBNATTRV;
        LBDSTCSC_RISK=QSTCSC;
        keep NOPOL DTOUCHM_RISK DTRCPRPR_RISK CTPRVTRV_RISK CTDEFFRA_RISK
            LBNATTRV_RISK LBDSTCSC_RISK;
    run;

    proc sql;
        create table MVT_PTF&vision. as select a.*,b.* from MVT_PTF&vision.
            a left join ird_risk_qan_202210 b on a.nopol=b.nopol;
    quit;

    data MVT_PTF&vision.;
        set MVT_PTF&vision.;
        if missing(CTPRVTRV) then CTPRVTRV=CTPRVTRV_RISK;
        if missing(CTDEFFRA) then CTDEFFRA=CTDEFFRA_RISK;
        if missing(LBNATTRV) then LBNATTRV=LBNATTRV_RISK;
        if missing(DTREFFIN) then DTREFFIN=DTREFFIN_RISK;
        if missing(LBDSTCSC) then LBDSTCSC=LBDSTCSC_RISK;
        drop DTOUCHM_RISK DTRECTRX_RISK CTPRVTRV_RISK CTDEFFRA_RISK
            LBNATTRV_RISK DTREFFIN_RISK LBDSTCSC_RISK;
    run;
%end;

data MVT_PTF&vision.;
    set MVT_PTF&vision.;
    format DTREFFIN ddmmyy10.;
run;

/*proc sql; create table test as select distinct nopol,dtrecpol,segment2,DTOUCHM,DTREFFIN,DTRCPPR,DTRECTRX from cube.MVT_PTF202305 where segment2="Chantiers"; run;*/
data MVT_PTF&vision.;
    set MVT_PTF&vision.;
    /*if segment2="Chantiers" then do;*/
    if missing(DTRCPPR) and not missing(DTREFFIN) then do;
        DTRCPPR=DTREFFIN;
    end;
    /* end; */
run;

proc sql;
    create table mvt_ptf&vision. as select
        a.*,coalesce(c1.cdsirect,c3.cdsirect) as cdsirect
        a.coalesce(c1.cdsirep,c3.cdsirep) as cdsirep from
        mvt_ptf&vision. a left join binaeo.histo_note_risque t2 on
        a.cdsiren=t3.cdsiren and t2.dtdeb_valid <= "&dtfimm"d and
        t2.dtfin_valid > "&dtfimm"d ;
quit;

proc sql;
    create table MVT_PTF&vision. as select t1.*,t2.DESTINAT from
        MVT_PTF&vision. t1 left join DEST.DO_DEST202110 t2 on t1.NOPOL=
        t2.NOPOL;
quit;

data CUBE.MVT_PTF&vision.;
    set MVT_PTF&vision.;

    if segment2="Chantiers" and missing(DESTINAT) then do;

        if PRDMATCH ( "/(UNIT)/", UPCASE ( DSTCSC ) ) OR PRDMATCH (
            "/(UNIT)/", UPCASE ( LBNATTRV ) ) OR PRDMATCH ( "/(LOI)/",
            UPCASE ( DSTCSC ) ) OR PRDMATCH ( "/(LOI)/", UPCASE ( LBNATTRV
            ) ) OR PRDMATCH ( "/(LGT)/", UPCASE ( DSTCSC ) ) OR PRDMATCH (
            "/(APRS)/", UPCASE ( LBNATTRV ) ) OR PRDMATCH ( "/(VILLA)/",
            UPCASE ( LBNATTRV ) ) OR (UPCASE ( LBNATTRV )) =? "/MAISON/" OR UPCASE (
            LBNATTRV ) ) OR PRDMATCH ( "/(INDIV)/", UPCASE ( DSTCSC ) ) OR
            PRDMATCH ( "/(INDIV)/", UPCASE ( LBNATTRV ) ) THEN DESTINAT=
            "Habitation" ;
        else DESTINAT="Autres";

        if DSTCSC in("01","02","03","04","1","2","3","4","23") then DESTINAT
            ="Habitation";
    end;
run;

data CUBE.MVT_PTF&vision.;
    retain nopol dircom noint cdpole cdmach cseg cssseg;
    set CUBE.MVT_PTF&vision.;
run;

data TABSPEC_CONST_&vision.;
    nopol="";
    NOINT="";
    CDPROD="";
    CDPOLE="";
    CDACTCONST="";
    CDACTCONST2="";
    CDNAP="";
    MTCA_RIS=.;
    run;

%if %sysfunc(exist(IPSPE1.IPPM024))=1 %then %do;

    PROC SQL;
        CREATE TABLE TABSPEC_CONST_&vision. AS select * from
            TABSPEC_CONST_&vision. OUTER UNION CORR (select
            NOPOL,NOINT,CDPROD,"I" AS CDPOLE, CDACTCONST AS CDACTCONST,
            CDACTPRIB2 as CDACTCONST2,"" AS CDNAP, as MTCA_RIS from
            IPSPE1.IPPM024) OUTER UNION CORR (select
            NOPOL,NOINT,CDPROD,"I" AS CDPOLE, CDACTCONST AS CDACTCONST,
            "" AS CDACTCONST2,"" AS CDNAP, CDACTCONST AS CDACTCONST, ACTSE1
            as CDACTCONST2,CDNAP,MTCA1 AS MTCA_RIS from IPSPE1.IPPM63)
            OUTER UNION CORR (select NOPOL,NOINT,CDPROD,"I" AS CDPOLE,
            ACTPRIM AS CDACTCONST, ACTSE2 as CDACTCONST2,"" AS CDNAP,MTCA1 AS
            MTCA_RIS from IPSPE3.IPPM63) ;
    QUIT;
%end;

%if %sysfunc(exist(IPSPE1.IPPM99))=1 %then %do;

    PROC SQL;
        CREATE TABLE TABSPEC_CONST_&vision. AS select * from
            TABSPEC_CONST_&vision. OUTER UNION CORR (select
            NOPOL,NOINT,CDPROD,"I" AS CDPOLE, substr(CDACPR1,1,4) AS
            CDACTCONST, CDACPR2 as CDACTCONST2,"" AS CDNAP,MTCA1 AS MTCA_RIS
            from IPSPE1.IPPM99) OUTER UNION CORR (select
            NOPOL,NOINT,CDPROD,"3" AS CDPOLE, CDACPR1 AS CDACTCONST2,CDNAP,MTCA1 AS MTCA_RIS from IPSPE3.IPPM99) ;
    QUIT;
%end;

data TABSPEC_CONST_&vision.;
    set TABSPEC_CONST_&vision.;
    if missing(nopol) then delete;
run;

PROC SQL;
    CREATE TABLE CUBE.MVT_PTF&vision. AS SELECT t1.*,t2.CDACTT as
        CDACTCONST, CDACPRIN AS CDACTCONST,
        coalece(t3.CDACTCONST,t1.ACTPRIN) AS ACTPRIH2, CASE WHEN t3.CDNAP
        ne "" then t3.CDNAP ELSE t1.CDNAF END AS CDNAP2, t3.CDACTCONST2 FROM
        CUBE.MVT_PTF&vision. PTF LEFT JOIN M6.BASECLI_INV M6 ON (PTF.NOCLT=
        M6.NOCLT) LEFT JOIN CLIENT1.CLACERT1 C1 ON (PTF.NOCLT=C1.NOCLT) LEFT
        JOIN CLIENT3.CLACERT3 C3 ON (PTF.NOCLT=C3.NOCLT);
QUIT;

x "grip.&vue./basecli_inv.sas7bdat.gz";

data CUBE.MVT_PTF&vision.;
    set CUBE.MVT_PTF&vision.;
    if CDNAP03_CLI in ("00" "0002" "9999") then CDNAP03_CLI="";
    if CDNAP08_M6 in ("00002" then CDNAP08_M6="";
run;

%code_isic_construction(CUBE.MVT_PTF&vision., &vision.);

/*<�r�alisation du cote 1SIC Global */
libname 1SIC
    "/saasprod/produits/SASEnterpriseBIServer/segrac/NETGRPES/DATAMART/ALEX_M15/Ref";

PROC SQL;
    CREATE TABLE CUBE.MVT_PTF&vision. AS SELECT t1.*, t2.ISIC_Global as
        ISIC_CODE_GBL FROM CUBE.MVT_PTF&vision. t1 LEFT JOIN
        1SIC.1SIC_LG_202306 t2 on ( t2.ISIC_local=t1.ISIC_CODE );
QUIT;

data CUBE.MVT_PTF&vision.;
    set CUBE.MVT_PTF&vision.;
    if ISIC_CODE="22000" then ISIC_CODE_GBL="D22000";
    if ISIC_CODE=" 22000" then ISIC_CODE="D22000";
    if ISIC_CODE="24021" then ISIC_CODE_GBL="D24000";
    if ISIC_CODE="24021" then ISIC_CODE_GBL="D24000";
    if ISIC_CODE="242025" then ISIC_CODE_GBL="242005";
    if ISIC_CODE="329020" then ISIC_CODE_GBL="329000";
    if ISIC_CODE="331024" then ISIC_CODE_GBL="331000";
    if ISIC_CODE="333010" then ISIC_CODE_GBL="333000";
    if ISIC_CODE="01020" then ISIC_CODE_GBL="B01020";
    if ISIC_CODE="01020" then ISIC_CODE_GBL="B01020";
    if ISIC_CODE="01023" then ISIC_CODE_GBL="B01000";
    if ISIC_CODE=" 01023" then ISIC_CODE_GBL="B01000";
    if ISIC_CODE="981020" then ISIC_CODE_GBL="981000";
run;

data CUBE.MVT_PTF&vision.;
    set CUBE.MVT_PTF&vision.;
    if NOINT in ("AA6160","4A6160","4A69547","AA6956") then TOP_PARTENARIAT=1;
    if NOINT in ("AA6160","4A6947") then TOP_BERLITZ=1;
run;

%mend;

```

## PTF_MVTS_RUN.sas

```sas
%macro run_ptf_mvts();

	options commid=tcp;
	%let serveur=STP3 7013;
	signon noscript remote=serveur user=%upcase(&sysuserid.) password="&MPWINDOWS";
	
	/* LIBRAIRIES STP3 */
	
	%let annee = %sysfunc(compress(%substr(&vision.,1,4)));
	%let mois = %sysfunc(compress(%substr(&vision.,5,2)));
	%put &annee &mois ;
	
	data _null_;
		ma_date=mdy(month("&sysdate9."d), 1, year("&sysdate9."d))-1;
		an_date=year(ma_date);
		mois_date=month(ma_date);
		AH0 =8+(&annee. -2011-1)*12 +&mois. ; /* Calcul de la génération de la table */
		call symput('AYM0',put(AH0,z3.));
		call symput('ASYS',an_date);
		call symput('MSYS',mois_date);
	run;
	
	%if &annee = &ASYS. and &mois.=&MSYS. %then %do;
		/* VISION EN COURS */
		LIBNAME PTF16   "INFP.IIA0P6$$.IPFE16"   disp=shr server=serveur;
		LIBNAME PTF36   "INFP.IIA0P6$$.IPFE36"   disp=shr server=serveur;
		LIBNAME PTF16a  "INFP.IIA0P6$$.IPFE1SPE"   disp=shr server=serveur;  /** Pour extraire des données AGT liées aux produit 01099 */
		LIBNAME PTF36a  "INFP.IIA0P6$$.IPF3SPE"   disp=shr server=serveur;  /** Pour extraire des données CRT liées aux produit 01099 */
	
	%end;
	%else %do;
		/* HISTORIQUE */
		LIBNAME PTF16   "INFH.IIMMP6$$.IPFE16.G08AMMD.V00"   disp=shr server=serveur;
		LIBNAME PTF36   "INFH.IIMMP6$$.IPFE36.G08AMMD.V00"   disp=shr server=serveur;
		LIBNAME PTF16a  "INFP.IIA0P6$$.IPFE1SPE"   disp=shr server=serveur;  /** Pour extraire des données AGT liées aux produit 01099 */
		LIBNAME PTF36a  "INFP.IIA0P6$$.IPF3SPE"   disp=shr server=serveur;  /** Pour extraire des données CRT liées aux produit 01099 */
	
	%end;
	
	proc contents data=PTF16.IPF;run;
	/*
	%if &annee = &ASYS. %then %do;
		LIBNAME segmprdt  "INFH.IDAAP6$$.SEGNPRDT(0)"  DISP = SHR  SERVER = serveur ;
	%end;
	%else %do;
		%let t = %sysfunc(max(%eval((&ASYS.-&annee.)*(-1)),-13));
		%put &t.;
		LIBNAME segmprdt "INFH.IDAAP6$$.SEGNPRDT(&t.)"  DISP = SHR  SERVER = serveur ;
	%end;
	*/
	LIBNAME segmprdt  "INFH.IDAAP6$$.SEGNPRDT(0)"  DISP = SHR  SERVER = serveur ;
	LIBNAME PRDCAP    'INFP.IIA0P6$$.PRDCAP'        DISP = SHR  SERVER = serveur ;
	
	LIBNAME CLIENT1 'infp.ima0p6$$.cliact14' DISP=SHR SERVER=SERVEUR;
	LIBNAME CLIENT3 'infp.ima0p6$$.cliact3'  DISP=SHR SERVER=SERVEUR;
	
	/* FIN DES LIBRAIRIES STP3*/
	
	
	/* LIBRAIRIES SAS */
	LIBNAME Dest "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/Construction";
	LIBNAME Ref "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref";
	LIBNAME MIG_AZEC "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL";
	LIBNAME PT_GEST "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/ref/pros_midcorp/ref/points_gestion";
	LIBNAME AACPRTF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/lkhoumh/sasdata/BDC/AACPRTF";
	LIBNAME BINSEE "/sasprod/prod/run/azi/d2d/w6/particuliers/dm_crm/partage/param/pro";
	LIBNAME W6 "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/ALEX_MIS/basecli_cumule";
	*LIBNAME RISK_REF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/202210";
	
	/* FIN DES LIBRAIRIES SAS*/
	
	
	/* LIBRAIRIES DTM CONSTRUCTION */
	
	x "mkdir /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
	LIBNAME CUBE "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
	
	/*
	%if %sysfunc(exist(cube.MVT_PTF202304)) = 0 %then %do; */
	
		/* MACROS-PGM UTILES */
		
		%include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/legouil/fonctions/generiques_v4.sas";
		%include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/CODIFICATION_ISIC_CONSTRUCTION.sas";
		
		OPTIONS sasautos = ("/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/_commun/Exploitation/sasamcr/sasalloc",
							"/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/_commun/Exploitation/sasamcr/sastools",
							"/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/aitihvi/saspgm/cubes",SASAUTOS)
							"/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/aitihvi/saspgm/cubes";
		
		mautosource mrecall;
		
		%if %substr(&vision.,1,4) < %sysfunc(year(%sysfunc(today()))) - 5 %then
			%do;
				%if &annee. <= 2014 %then
					%do;
						%put annee = 15  mois = 12 ;
						%alloc_spe_v3(2015,12,IPFSPE);
					%end;
				%else
					%do;
						%put annee = &annee.  mois = 12 ;
						%alloc_spe_v3(&annee.,12,IPFSPE);
					%end;
			%end;
		%if %substr(&vision.,1,4) = %sysfunc(year(%sysfunc(today()))) - 5 %then
			%do;
				%if %substr(&vision.,5,2) < %sysfunc(month(%sysfunc(today()))) %then
					%do;
						%let annee_1 = %eval(&annee. - 1);
						%put &annee_1;
						%put annee = &annee_1  mois = 12 ;
						%alloc_spe_v3(&annee_1.,12,IPFSPE);
					%end;
				%else
					%do;
						%put annee = &annee.  mois = &mois. ;
						%alloc_spe_v3(&annee.,&mois.,IPFSPE);
					%end;
			%end;
		%if %substr(&vision.,1,4) > %sysfunc(year(%sysfunc(today()))) - 5 %then
			%do;
				%put annee = &annee.  mois = &mois. ;
				%alloc_spe_v3(&annee.,&mois.,IPFSPE);
			%end;
		
		
		/* AZEC */
		%if &vision. <=202008 and &vision. >= 201211 %then %do;
			%alloc_azec_v3(&annee., &mois.);
		%end;
		
		
		data _null_;
		
			format Date ddmmyy10. Dtrefn ddmmyy6. Dtrefn1 ddmmyy10.;
			Date      = mdy(substr("&VISION.",5,2),10,substr("&VISION.",1,4));
			/*Date    = intnx('month', today(), -1, 'e');*/
			Annee     = put(year(date),z4.);
			Mois      = put(month(date),z2.);
			
			Dtrefn=intnx('MONTH',mdy(Mois,1,Annee),1);
			Dtrefn=intnx('DAY',Dtrefn,-1);
			Dtrefn1=intnx('MONTH',mdy(Mois,1,Annee-1),1);
			A = substr(Annee,3,2);
			M = Mois;
			MA = cats(M,A);
			Vision = cats(Annee,Mois);
			
			call symput('DTFIN',put(dtrefn,date9.));
			call symput('dtfinmm',put(dtrefn,date9.));
			call symput('dtfinmm1',put(dtrefn1,date9.));
			call symput('finmoisn1',put(dtrefn1,date9.));
			call symput('DTOBS',trim(left(Annee))||Mois);
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
			call symput("Vision",compress(Vision));
			call symputx("date_run",date_run);
			call symputx("hour",hour);
		
		run;
		
		%az_mvt_ptf(&annee., &mois.);
		
		%include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/REF_segmentation_azec.sas";
		%azec_mvt_ptf(&vision.);
		%consolidation_az_azec_mvt_ptf;
		
	/*
	%end; */

%mend;

%run_ptf_mvts();

```

## PTF_MVTS_RUN_RECETTE.sas

```sas
%MACRO traitement_ptf_mvts (cdpole, annee, mois, sheet);

	/*
	%let annee = %sysfunc(compress(%substr(&vision.,1,4)));
	%let mois  = %sysfunc(compress(%substr(&vision.,5,2)));
	%put &annee &mois ;
	%let an= %substr(&annee, 3, 2);
	*/

	/*Importation dans la work au format sas  */
	proc import datafile    = "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/Portefeuilles_company_IAAD_&mois_an..xls"
		out         = mvt_&cdpole.
		dbms        = xls;
		sheet       = "&sheet.";
	run;

	PROC CONTENTS DATA = mvt_&cdpole. OUT = WORK.nom_variable (keep=name varnum) noprint; RUN;

	data _null_;
		set nom_variable;
		if varnum = 2 then do;
			call symput("col1",name);
		end;
	run;

	data mvt2_&cdpole.;
		set mvt_&cdpole.(rename=( "&col1."n = LoB
								  "Nombre de contrats clôture N"n = Nbre_pol
								  "Nbr entrées N"n = Nbre_AFN
								  "Nbr sorties N"n = Nbre_resil ));

		length cdpole $1.;
		cdpole=&cdpole.;
		where LoB in (  "TOTAL CONSTRUCTION");
		prime_cloture= Nbre_pol* "Prime moyenne clôture N"n;
		prime_resil= Nbre_resil*"Prime moyenne sorties N"n;
		Prime_AFN= Nbre_AFN *"Prime moyenne entrées N"n;
		Pri_moy_Ptf_act  ="Prime moyenne clôture N"n;
		Pri_moy_an_act ="Prime moyenne entrées N"n;
		Pri_moy_res_act ="Prime moyenne sorties N"n;
	run;

	/* Adaptation de la base de la finance à celle du cube*/
	data mvt_&cdpole._fin;
		set mvt2_&cdpole.( keep = Cdpole LoB Nbre_pol Nbre_pol Nbre_AFN Nbre_resil prime_cloture Prime_AFN prime_resil prime_cloture Pri_moy_Ptf_act
							Pri_moy_an_act Pri_moy_res_act);
		if LoB in ("TOTAL CONSTRUCTION") then
			do;
				cmarch='6';
				cseg='2';
			end;
	run;

%MEND;

%MACRO calcul_ecart_ptf_mvts(annee, mois);
	%local a; %let a = %nrstr(%mend);

	PROC SQL;
		CREATE TABLE  table_finale AS
		(SELECT t1.cdpole ,t1.lob ,t1.cmarch, t1.cseg, t1.Nbre_pol, t1.Nbre_AFN ,t1.Nbre_resil,t1.prime_cloture, t1.Prime_AFN, t1.prime_resil
		 FROM work.mvt_1_fin t1)  UNION
		(SELECT t1.cdpole ,t1.lob ,t1.cmarch, t1.cseg, t1.Nbre_pol ,t1.Nbre_AFN ,t1.Nbre_resil , t1.prime_cloture, t1.Prime_AFN ,t1.prime_resil
		 FROM work.mvt_3_fin t1);
	QUIT;

	PROC SQL;
		CREATE TABLE cube&annee.&mois. AS
			SELECT t1.vision, t1.CDPOLE, t1.cmarch, t1.CSEG,(SUM(t1.NBAFN)) AS SUM_of_NBAFN,
				(SUM(t1.NBRES)) AS SUM_of_NBRES,(SUM(t1.Primes_AFN)) AS SUM_of_Primes_AFN,
				(SUM(t1.Primes_RES)) AS SUM_of_Primes_RES
			FROM cube.mvt_ptf&vision. t1
			WHERE t1.vision  = &vision. and t1.TOP_TEMP = 0
			GROUP BY t1.vision, t1.CDPOLE, t1.cmarch, t1.CSEG;
	QUIT;

	proc sort data=table_finale;
		by  cdpole cmarch cseg;
	run;

	proc sort data=cube&annee.&mois.;
		by  cdpole cmarch cseg;
	run;

	data base_finale;
		merge table_finale  ( in=a)  cube&annee.&mois.;
		by cdpole cmarch cseg;

		if a;
	run;

	PROC SQL;
		CREATE TABLE ptf&annee.&mois. AS
			SELECT t1.vision, t1.CDPOLE,t1.CMARCH,
				t1.CSEG, (SUM(t1.NBPTF)) AS SUM_of_NBPTF, (SUM(t1.Primes_PTF)) AS SUM_of_Primes_PTF
			FROM cube.mvt_ptf&vision. t1
			WHERE t1.vision  = &vision. and t1.TOP_TEMP = 0
			GROUP BY t1.vision , t1.CDPOLE, t1.CMARCH, t1.CSEG;
	QUIT;

	proc sort data=base_finale;
		by vision cdpole cmarch cseg;
	run;

	proc sort data=ptf&annee.&mois.;
		by  vision cdpole cmarch cseg;
	run;

	data base_finale;
		merge base_finale  ( in=a) ptf&annee.&mois.;
		by vision cdpole cmarch cseg;

		if a;
	run;

	data ecart_mvt_ptf&annee.&mois.;
		set base_finale;
		format Pri_moy_Ptf_dt 10.2;
		vision=&vision.;

		if cmarch = '6' and cseg='2' then
			libelle_segment='CONSTRUCTION' ;

		SUM_of_Primes_PTF= round(SUM_of_Primes_PTF, 1);
		SUM_of_Primes_AFN= round(SUM_of_Primes_AFN, 1);
		SUM_of_Primes_RES= round(SUM_of_Primes_RES, 1);
		prime_cloture= round(prime_cloture, 1);
		prime_afn= round(prime_afn, 1);
		prime_resil= round(prime_resil, 1);
		Pri_moy_Ptf_dt  =round(SUM_of_Primes_PTF/SUM_of_NBPTF ,1);
		Pri_moy_an_dt =round(SUM_of_Primes_AFN/SUM_of_NBAFN ,1);
		Pri_moy_res_dt =round(SUM_of_Primes_RES/SUM_of_NBRES ,1);
		Pri_moy_Ptf_act  =round(prime_cloture/Nbre_pol,1);
		Pri_moy_an_act =round(prime_afn/Nbre_afn ,1);
		Pri_moy_res_act =round(prime_resil/Nbre_resil,1);
		ecart_Police =round(SUM_of_NBPTF-Nbre_pol ,1);
		ecart_nbafn =round(SUM_of_NBAFN - Nbre_afn,1);
		ecart_nbres =round(SUM_of_NBRES - Nbre_resil,1);
		ecart_PM_PTF =round(Pri_moy_Ptf_dt-Pri_moy_Ptf_act,1);
		ecart_PM_AN =round(Pri_moy_an_dt - Pri_moy_an_act,1);
		ecart_PM_RES =round(Pri_moy_res_dt - Pri_moy_res_act,1);
		ecart_Primes_Ptf =ROUND(SUM_of_Primes_PTF-prime_cloture,1);
		ecart_Primeafn =round(SUM_of_Primes_AFN - prime_afn ,1);
		ecart_Primeres =round(SUM_of_Primes_RES - prime_resil ,1);
	run;

	/* Ecarts PTF - Finance*/
	proc sql;
		create table RESUL.ecart_ptf&annee.&mois. as
		select vision, cdpole, cmarch, cseg, libelle_segment, SUM_of_NBPTF, Nbre_pol,
			Pri_moy_Ptf_dt, Pri_moy_Ptf_act, SUM_of_Primes_PTF, prime_cloture,
			ecart_Police, ecart_PM_PTF, ecart_Primes_Ptf
		from ecart_mvt_ptf&annee.&mois.;
	quit;

	/* Ecarts MVT - Finance*/
	proc sql;
		create table RESUL.ecart_mvt&annee.&mois. as
		select vision, cdpole, cmarch, cseg, libelle_segment, SUM_of_NBAFN, Nbre_afn,
			SUM_of_NBRES, Nbre_resil, Pri_moy_an_dt, Pri_moy_an_act, Pri_moy_res_dt,
			Pri_moy_res_act, SUM_of_Primes_AFN, prime_afn, SUM_of_Primes_RES, prime_resil,
			ecart_nbafn, ecart_nbres, ecart_PM_AN, ecart_PM_RES, ecart_Primeafn, ecart_Primeres
		from ecart_mvt_ptf&annee.&mois.;
	quit;

%MEND;

data _null_;
	Date_run = cats(put(year(today()),z4.),put(month(today()),z2.),put(day(today()),z2.));
	Hour      = cats(put(hour(time()),z2.),"H",put(minute(time()),z2.));
	format Date ddmmyy10. Dtrefn ddmmyy6. Dtrefni ddmmyy10.;
	Date      = mdy(substr("&VISION.",5,2),10,substr("&VISION.",1,4));
	/*Date     = intnx('month', today(), -1, 'e');*/
	Annee     = put(year(date),z4.);
	Mois      = put(month(date),z2.);

	Dtrefn=intnx('MONTH',mdy(Mois,1,Annee),1);
	Dtrefn=intnx('DAY',Dtrefn,-1);
	Dtrefni=intnx('MONTH',mdy(Mois,1,Annee-1),1);
	A = substr(Annee,3,2);
	M = Mois;
	MA = cats(M,A);
	Vision = cats(Annee,Mois);

	call symput('DTFIN',put(dtrefn,date9.));
	call symput('dtfinmn',put(dtrefn,date9.));
	call symput('dtfinmni',put(dtrefni,date9.));
	call symput('finmoisn1',put(dtrefn1,date9.));
	call symput('DTOBS',trim(left(Annee))||Mois);
	call symput('DTFIN_AN',put(mdy(1,1,Annee+)-1,date9.));
	call symput('DTDEB_AN',put(mdy(1,1,Annee),date9.));
	call symput('dtdebn',put(mdy(1,1,Annee),date9.));
	call symput('DTDEB_MOIS_OBS',put(mdy(Mois,1,Annee),date9.));
	call symput('an',A);
	call symputx('FINMOIS', dtrefn);
	call symput("mois_an",ma);

	call symput('DATEARRET', quote(put(date,date9.)) || 'd');

	call symput("ANNEE", Annee);
	call symput("MOIS", Mois);
	call symput("Vision",compress(Vision));
	call symputx("date_run",date_run);
	call symputx("hour",hour);
	call symput('date_time', compress(put(datetime(),datetime20.)));

run;

LIBNAME CUBE "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
LIBNAME RESUL "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Resultat";

%macro exec_recette_ptf_mvts ;

	%if %sysfunc(fileexist("/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Recette/Input/Portefeuilles_company_IAAD_&mois_an..xls")) = 1
	%then %do ;

		%traitement_ptf_mvts(1, &annee., &mois., Agents yc MAEL);
		%traitement_ptf_mvts(3, &annee., &mois., Courtage hors Berlioz /*Courtage yc MAEL*/);
		%calcul_ecart_ptf_mvts(&annee., &mois.);

		PROC PRINT data=RESUL.ecart_ptf&annee.&mois.; run;
		PROC PRINT data=RESUL.ecart_mvt&annee.&mois.; run;

	%end;

%mend;

%exec_recette_ptf_mvts ;

```

## PTF_MVTS_RUN_REPR_HIST.sas

```sas
%macro run_ptf_mvts_repr_hist();

    options comamid=tcp;
    %let serveur=STP3 7013;
    signon noscript remote=serveur user=%upcase(&sysuserid.) password="&MPWINDOWS";

    /* LIBRAIRIES STP3 */

    %let annee = %sysfunc(compress(%substr(&vision.,1,4)));
    %let mois  = %sysfunc(compress(%substr(&vision.,5,2)));
    %put &annee &mois ;

    %let P = %eval(&annee. - %eval(%sysfunc(year(%sysfunc(today())))-1));
    %put &P.;

    libname PTF16 "INFH.IIAAP6$$.IPFE16(&P.)" disp=shr server=serveur;
    libname PTF36 "INFH.IIAAP6$$.IPFE36(&P.)" disp=shr server=serveur;


    LIBNAME segmprdt "INFH.IDAAP6$$.SEGMPRDT(0)"  DISP = SHR  SERVER = serveur ;
    LIBNAME PRDCAP   'INFP.IIAAP6$$.PRDCAP'       DISP = SHR  SERVER = serveur ;

    LIBNAME CLIENT1 'infp.imaap6$$.cliact14' DISP=SHR SERVER=SERVEUR;
    LIBNAME CLIENT3 'infp.imaap6$$.cliact3'  DISP=SHR SERVER=SERVEUR;

    /* FIN DES LIBRAIRIES STP3 */


    /* LIBRAIRIES SAS */
    LIBNAME Dest "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/construction";
    LIBNAME Ref "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref";
    LIBNAME MTG_AZEC "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL";
    LIBNAME PT_GEST "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/points_gestion";
    LIBNAME AACPRTF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/1khoumm/saspgm/BDC/AACPRTF";
    LIBNAME BINSEE "/sasprod/prod/prod/run/az1/d2d/ws_particuliers/dm_cnmpartage/param/pro";
    LIBNAME WG "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/ALEX_MIS/base13_cumule";
    LIBNAME RISK_REF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/202210";

    /* FIN DES LIBRAIRIES SAS */


    /* LIBRAIRIES DTM CONSTRUCTION */

    x "mkdir /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    LIBNAME CUBE "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";

    /*
    %if %sysfunc(exist(cube.MVT_PTF202304)) = 0 %then %do; */

        /* MACROS-PGM UTILES */

        %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/legouli/fonctions/generiques_v4.sas";
        %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Production_DTM_Construction/CODIFICATION_ISIC_CONSTRUCTION.sas";

        OPTIONS sasautos = ("/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/_commun/Exploitation/sasmcr/sasalloc",
                            "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/_commun/Exploitation/sasmcr/sastools",
                            "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/aitlhvi/saspgm/Cubes",SASAUTOS)
        mautosource mrecall;

        %if %substr(&vision.,1,4) < %sysfunc(year(%sysfunc(today()))) - 5 %then
            %do;
                %if &annee. <= 2014 %then
                    %do;
                        %put annee = 15  mois = 12 ;
                        %alloc_spe_v3(2015,12,IPFSPE);
                    %end;
                %else
                    %do;
                        %put annee = &annee.  mois = 12 ;
                        %alloc_spe_v3(&annee.,12,IPFSPE);
                    %end;
            %end;
        %if %substr(&vision.,1,4) > %sysfunc(year(%sysfunc(today()))) - 5 %then
            %do;
                %put annee = &annee.  mois = 12 ;
                %alloc_spe_v3(&annee.,&mois.,IPFSPE);
            %end;

        %if %substr(&vision.,1,4) = %sysfunc(year(%sysfunc(today()))) - 5 %then
        %do;
            %if %substr(&vision.,5,2) < %sysfunc(month(%sysfunc(today()))) %then
                %do;
                    %let annee_1 = %eval(&annee. - 1);
                    %put &annee_1;
                    %put annee = &annee_1  mois = 12 ;
                    %alloc_spe_v3(&annee_1.,12,IPFSPE);
                %end;
            %else
                %do;
                    %put annee = &annee.  mois = &mois. ;
                    %alloc_spe_v3(&annee.,&mois.,IPFSPE);
                %end;
        %end;

        /* AZEC */
        %if &vision. <=202008 and &vision. >= 201211 %then %do;
            %alloc_azec_v3(&annee., &mois.);
        %end;


        data _null_;

            format Date ddmmyy10. Dtrefn ddmmyy6. Dtrefn1 ddmmyy10.;
            Date        = mdy(substr("&VISION.",5,2),10,substr("&VISION.",1,4));
            /*Date      = intnx('month', today(), -1, 'e');*/
            Annee       = put(year(date),z4.);
            Mois        = put(month(date),z2.);

            Dtrefn=intnx('MONTH',mdy(Mois,1,Annee),1);
            Dtrefn=intnx('DAY',Dtrefn,-1);
            Dtrefn1=intnx('MONTH',mdy(Mois,1,Annee-1),1);
            A = substr(Annee,3,2);
            M = Mois;
            MA = cats(M,A);
            Vision = cats(Annee,Mois);

            call symput('DTFIN',put(dtrefn,date9.));
            call symput('dtfinmm',put(dtrefn,date9.));
            call symput('dtfinmn1',put(dtrefn1,date9.));
            call symput('finmoisn1',put(dtrefn1,date9.));
            call symput('DTOBS',trim(left(Annee))||Mois);
            call symput('DTFIN_AN',put(mdy(1,1,Annee+1)-1,date9.));
            call symput('DTDEB_AN',put(mdy(1,1,Annee),date9.));
            call symput('dtdebn',put(mdy(1,1,Annee),date9.));
            call symput('DTDEB_MOIS_OBS',put(mdy(Mois,1,Annee),date9.));
            call symput('an',A);
            call symputx('FINMOIS', dtrefn);
            call symputx('mois_an',ma);

            call symput('DATEARRET', quote(put(date,date9.)) || ' d');

            call symput("ANNEE", Annee);
            call symput("MOIS", Mois);
            call symput("Vision",compress(Vision));
            call symputx("date_run",date_run);
            call symputx("hour",hour);

        run;

        %az_mvt_ptf(&annee., &mois.);
        %if &vision >=201211 %then %do;
            %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm/REF_segmentation_azec.sas";
            %azec_mvt_ptf(&vision.);
        %end;
        %consolidation_az_azec_mvt_ptf;

    %mend;

    %run_ptf_mvts_repr_hist();

```

## REF_segmentation_azec.sas

```sas
%let arbobase = /net/home/&sysuserid./METGAZDT;
%let date     = &DATEARRET.;
%let PrJnv1a  = "01JAN&annee."d;

DATA        _NULL_;

		NOM_CLIENT   =  "TEST"  ;

IF          month ( &date.   ) NE 01
THEN DO  ;
			mois_az = month ( &date.  ) - 1;
			an      = year  ( &date.  ) ;
		END;
ELSE DO  ;
			mois_az = 12;
			an      = year  ( &date.  ) - 1 ;
		END;

			IF   mois_az LT 10   THEN  mois_az1 = 0||trim(left(mois_az)) ;
			                     ELSE  mois_az1 = mois_az ;

			CALL SYMPUTX ( 'ME'       , mois_az1      , "G" ) ;
			CALL SYMPUTX ( 'AE'       , an            , "G" ) ;
			CALL SYMPUTX ( 'Client'   , NOM_CLIENT    , "G" ) ;
			CALL SYMPUTX ( 'ANNEE'    , an ) ;
			CALL SYMPUTX ( 'mois_az'  , put(mois_az, z2.)) ;

RUN       ;


%LET POL_IST    =   (   '084443777','084443778','084548930','084548939',
						'084700586','084700587','054463672','54463672' ) ;
/*
%LET POL_IST1   =   (   '084214'  )  ;
%LET POL_IST2   =   (   '84214'   )  ;*/

%LET POL_IST1   =   (   '084214'  )  ;
%LET POL_IST2   =   (   '84214'   )  ;*/

%LET PRODUIT    =   (  'A00' 'A01' 'AA1' 'AB1' 'AG1' 'AG8' 'AR1' 'ING' 'PG1' 'AA6' 'AG6' 'AR2'
					   'AU1' 'AA4' 'A03' 'AA3' 'AG3' 'AM3' 'MA1' 'MA0' 'MA1' 'MA2' 'MA3' 'MA4'
					   'MA5' 'MT0' 'MR0' 'AAC' 'GAV' 'GNC' 'MB1' 'MB2' 'MB3' 'MED' 'MH0' 'MP0'
					   'MP1' 'MP2' 'MP3' 'MP4' 'MPG' 'MPP' 'MI0' 'MI1' 'MI2' 'MI3' 'PI2' ) ;


%MACRO          SELECTION  ;

ATTRIB          NMCLIENT        FORMAT = $50. ;

IF              POLICE  IN  &POL_IST    ;
/*
IF              SUBSTR (POLICE,1,6)  IN  &POL_IST1 OR
				SUBSTR (POLICE,1,5)  IN  &POL_IST2 ;*/

				NMCLIENT =  "&Client"   ;

%MEND   ;


%MACRO          PGST_LG ;


IF  PRODUIT IN ("FA7") THEN DO;

IF  categoaa IN ("C38") THEN protocole="LCD";
IF  categoaa IN ("C48") THEN protocole="LLD";
IF  INTERMED IN ("14142") THEN protocole="ARVAL";
IF  INTERMED IN ("35158") THEN protocole="FLEXITRANS";
IF  INTERMED IN ("25043") THEN protocole="AON";
IF  (INTERMED IN ("23387") AND categoaa IN ("C66")) THEN protocole="Auto Ecole Filhet Allart";
IF  (INTERMED IN ("23387") AND categoaa IN ("C48")) THEN protocole="Athlon";
IF  (INTERMED IN ("35364") AND categoaa IN ("C48")) THEN protocole="Avis G.E.";
IF  (INTERMED IN ("21928") AND categoaa IN ("C48")) THEN protocole="Avis G.E.";
IF  (INTERMED IN ("27047") AND categoaa IN ("C48")) THEN protocole="Satec";
IF  (INTERMED IN ("27384") AND categoaa IN ("C48")) THEN protocole="DCS Fleet";
IF  (INTERMED IN ("14142") AND categoaa IN ("C66")) THEN protocole="Auto Ecole Carone";
IF  (INTERMED IN ("10227") AND ((txcommis=18 AND categoaa IN ("C11"))
    or (txcommis=20 AND categoaa NOT IN ("C11")))) THEN protocole="Verlingue";
IF  (INTERMED IN ("13447") AND txcommis=21) THEN protocole="Marsh";
IF  POLICE IN ("N26186204" "N26186213" "029474812" "029604671"
    "029773332") THEN protocole="Alphabet GS berger Simon";
IF  POLICE IN ("L76200012") THEN protocole="Programme International";
IF  POLICE IN ("070851144") THEN protocole="Sogepa";
IF  POLICE IN ("029175361" "029175362" "029175363"
    "029175364" "029176893" "029254559" "029322411" "029693540") THEN protocole="DIF";
IF  POLICE IN ("CO1802621" "CO1802591") THEN protocole="Fast Carone";
IF  protocole NOT IN ("") THEN poingest="H27";

END;

%MEND ;


%MACRO Lib_PTGST ( Tab = ) ;

/*-----------------------------------------------------------*/
/*                   REGION                                  */
/*-----------------------------------------------------------  Debut  ------------------*/

	PROC  SORT  DATA  =  &Tab              ;  BY  PTGST  ;  RUN  ;

	DATA                &Tab       ;
	MERGE               &Tab       (  IN  =  a  )
						MPTGST     (  IN  =  b  );
	BY                  PTGST      ;

/*----------------------------------------------------------------------------------------*/
/*                   REGION                                                               */
/*----------------------------  FIN  -----------------------------------------------------*/

%MEND ;


%MACRO          SEGMENTA ;

	LENGTH  CDPROD $5  CPROD $5 cmarch $1 lmarch $44 lmarch2 $46 cseg $1 lseg $52 lseg2 $54 cssseg $1 lssseg $45 lssseg2 $47 lprod $34  segment $12    ;
	if _N_ = 1 THEN DO ;
	DECLARE HASH Tab_Prd_CSTR ( DATASET : "LOB" ,
	                            ORDERED : "YES" ) ;
	Tab_Prd_CSTR.defineKey ("PRODUIT") ;
	Tab_Prd_CSTR.defineData ("CDPROD","CPROD","cmarch","lmarch","lmarch2","cseg","lseg","lseg2","cssseg","lssseg","lssseg2","lprod","segment") ;
	Tab_Prd_CSTR.defineDone () ;
	CALL missing ( CDPROD, CPROD, cmarch, lmarch, lmarch2, cseg, lseg, lseg2, cssseg, lssseg, lssseg2, lprod, segment ) ;
	END ;

%MEND ;


%MACRO          TYP_PRD_CSTR  (  TABLE  =  )  ;

PROC   SORT     DATA  =  &TABLE              ;   BY   POLICE   ;   RUN   ;

DATA            &TABLE         ;

ATTRIB          Segment_3              FORMAT  =   $27.  ;

MERGE           &TABLE         (  IN  =  x  )
				CONSTRUC       (  IN  =  y  )
						KEEP  =   POLICE   Type_Produit  )  ;

BY              POLICE  ;
IF              x ;

		IF      lmarch2 IN ('6_CONSTRUCTION')    THEN   DO  :
		IF      Type_Produit  IN  ("Artisans")
												THEN   Segment_3  =  "Artisans"  ;  ELSE

		IF      Type_Produit  IN  ("TRC","DO","CNR","Autres Chantiers")
												THEN   Segment_3  =  "Chantiers"  ;
												ELSE   Segment_3  =  "Renouvelables hors artisans"  ;
												END  ;

		Type_Produit_2  =  Type_Produit  ;

RUN             ;

%MEND  ;


PROC FORMAT  ;

	PICTURE  montant       low-<0  =  '000 000 000 999,99'  (  PREFIX  =  '-'   MULT  =  100  )
						   0-high  =  '000 000 000 999,99'  (  MULT   =  100  )  ;
	PICTURE  nombre        low-<0  =  '000 000 009'         (  PREFIX  =  '-'  )
						   0-high  =  '000 000 009'  ;

OPTIONS  LINESIZE  =  132  PAGESIZE  =  500  MISSING  =  .  ;
RUN ;

	LIBNAME   SAS_ASP             "$SASDATA/SPA"                                                  ;  RUN   ;
	LIBNAME   SASDT               "$SASDATA"                                                      ;  RUN   ;

	LIBNAME   SAS_P               "$SASDATA/PTF"                                                  ;  RUN   ;
	LIBNAME   SAS_C               "$SASDATA/CSTR"                                                 ;  RUN   ;

	LIBNAME   AAFICHE             "$SASDATA/BDC/AAFICHE"                                          ;  RUN   ;

	LIBNAME   AATDBTTM            "&arbobase./utilisateurs/lkhoumm/sasdata/BDA/AATDBTTM"         ;  RUN   ;
	LIBNAME   AA_GRANU            "&arbobase./utilisateurs/lkhoumm/sasdata/BDA/AA_GRANU"         ;  RUN   ;

	LIBNAME   AACPRTF             "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AACPRTF"          ;  RUN   ;
	LIBNAME   AAFICHE             "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AAFICHE"          ;  RUN   ;
	LIBNAME   AA_CSIN             "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AA_CSIN"          ;  RUN   ;
	LIBNAME   AARCAGD             "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AARCAGD"          ;  RUN   ;
	LIBNAME   AASTCLNT            "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AASTCLNT"         ;  RUN   ;
	LIBNAME   AATDBMC             "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AATDBMC"          ;  RUN   ;
	LIBNAME   AA_BTP              "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AA_BTP"           ;  RUN   ;
	LIBNAME   AA_ROC_A            "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AA_ROC_A"         ;  RUN   ;
	LIBNAME   AAIPC               "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AAIPC"            ;

	LIBNAME   SASDT_C             "&arbobase./utilisateurs/lkhoumm/sasdata/SMC/&annee/&mois_az"  ;  RUN   ;

	LIBNAME   SAS_P               "&arbobase./utilisateurs/lkhoumm/sasdata/PTF"                  ;  RUN   ;
	LIBNAME   SAS_C               "&arbobase./utilisateurs/lkhoumm/sasdata/CSTR"                 ;  RUN   ;


DATA       LOB          ;
SET        SAS_C.LOB    ;
IF         cmarch    IN  ('6')  ;

RUN  ;

PROC  SORT  DATA  =  LOB          ;  BY   PRODUIT  CPROD   CDPROD  ;  RUN  ;

DATA       MPTGST ;
SET        SAS_C.PTGST  (  KEEP  =  PTGST   REGION   P_Num  )  ;
RUN  ;

PROC  SORT  DATA  =  MPTGST   NODUPKEY  ;  BY  PTGST  ;  RUN  ;


DATA    categ  ;   SET  SAS_C.import_catmin  ;   RUN  ;

	LIBNAME   POLIC_CU            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/polic_cu"     ;
	LIBNAME   GARANTCU            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/garantcu"     ;
	LIBNAME   CONSTRUC            "/net/home/tohoun/METGAZDT/AZEC_HISTO/SAS_save_diverscu/202009/construc";
	*LIBNAME  CONSTRUC            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/construc"     ;
	LIBNAME   RISTECCU            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/risteccu"     ;
	*LIBNAME  RISTECCU            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/risteccu"     ;

	LIBNAME   MULPROCU            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/mulprocu"     ;
	LIBNAME   SINISTCU            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/sinistcu"     ;

	LIBNAME   PRIMGRCU            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/prigarcu"     ;

	LIBNAME   REGSINCU            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/regsincu"     ;
	LIBNAME   capitxcu            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/capitxcu"     ;
	LIBNAME   INCENDCU            "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/incendcu"     ;
	LIBNAME   MPACU               "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/mpacu";
	LIBNAME   RCENTCU             "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/rcentcu"      ;



/*
LIBNAME   CONSTRUC            '$CONSTRUC'  ;
LIBNAME   RISTECCU            '$RISTECCU'  ; */


DATA       typmarc4  (  KEEP  =  POLICE  typmarc4  )
		   typmarc5  (  KEEP  =  POLICE  typmarc5  )
		   typmarc6  (  KEEP  =  POLICE  typmarc6  )  ;

SET        GARANTCU.GARANTCU  (  KEEP  =  POLICE  garantie  branche  );

IF         branche    IN  (  'CO','RT')  ;

		LENGTH  typmarc4  $8.  typmarc5  $8.  typmarc6  $8.  ;

		IF   garantie  =  'CMAA'  THEN   DO  ;  typmarc4  =  '01 02 06'  ;
											OUTPUT    typmarc4  ;
											END  ;

		IF   garantie  =  'CNAP'  THEN   DO  ;  typmarc5  =  '04    '  ;
											OUTPUT    typmarc5  ;
											END  ;

		IF   garantie  =  'CNPR'  THEN   DO  ;  typmarc6  =  '08 09 10'  ;
											OUTPUT    typmarc6  ;
											END  ;

RUN;

LIBNAME   GARANTCU                   CLEAR     ;

PROC  SORT  DATA  =  typmarc4   NODUPKEY  ;  BY   POLICE   ;  RUN  ;
PROC  SORT  DATA  =  typmarc5   NODUPKEY  ;  BY   POLICE   ;  RUN  ;
PROC  SORT  DATA  =  typmarc6   NODUPKEY  ;  BY   POLICE   ;  RUN  ;

DATA       MARCHE  (  KEEP  =  POLICE  typmarc7  )  ;

MERGE      typmarc4  typmarc5  typmarc6  ;

BY         POLICE  ;

		LENGTH    typmarc7  $8.  ;

		IF   typmarc4  =  ' '  AND  typmarc5  =  ' '  THEN   typmarc7  =  typmarc6  ;
		IF   typmarc4  =  ' '  AND  typmarc6  =  ' '  THEN   typmarc7  =  typmarc5  ;
		IF   typmarc5  =  ' '  AND  typmarc6  =  ' '  THEN   typmarc7  =  typmarc4  ;

RUN  ;

PROC   DATASETS  ;   DELETE  typmarc4  ;   QUIT  ;   RUN  ;
PROC   DATASETS  ;   DELETE  typmarc5  ;   QUIT  ;   RUN  ;
PROC   DATASETS  ;   DELETE  typmarc6  ;   QUIT  ;   RUN  ;


DATA       CONSTRUCU
(  KEEP  =  POLICE  PRODUIT  typmarc1  ltvpmar1  formule  nat_cnt  )  ;

		%SEGMENTA  ;

SET        CONSTRUCU.CONSTRUCU
(  KEEP  =  POLICE  PRODUIT  typmarc1  ltvpmar1  formule  nat_cnt  )
		   RISTECCU.RISTECCU
(  KEEP  =  POLICE  PRODUIT                      formule            )  ;

		IF                Tab_Prd_CSTR.find ()  =  0    ;

		IF                CMARCH .IN  ('6')  ;

RUN  ;

	*LIBNAME   CONSTRUCU                 CLEAR     ;
	*LIBNAME   RISTECCU                  CLEAR     ;

PROC  SORT  DATA  =  CONSTRUCU   NODUPKEY  ;  BY  POLICE  ;  RUN  ;
PROC  SORT  DATA  =  MARCHE      NODUPKEY  ;  BY  POLICE  ;  RUN  ;

DATA       CONSTRUCU  ;

ATTRIB     activite          FORMAT  =  $30.  ;

MERGE      CONSTRUCU  (  IN  =  x  )
		   MARCHE     (  IN  =  y  )  ;

BY         POLICE  ;
IF         x  ;

		IF    typmarc1  =  ' '   THEN   typmarc1  =  typmarc7  ;

/********* nature de contrat champs valable que pour le PRODUIT DPC *********/

		IF    PRODUIT  NOT IN  ('DPC')   THEN   nat_cnt  =  '  '  ;

/*************** creation du champs activite depuis ltvpmarc1 ***************/
		IF    PRODUIT  IN  ('RBA' 'RCD' )  AND  typmarc1  =  '01'
													THEN      activite   =    'ARTISAN'  ;

		IF    PRODUIT  IN  ('RBA' 'RCD')  AND
		(  typmarc1  IN  ('02' '03' '04')  OR  (  typmarc1  =  '  '  AND  formule  IN  ('DA' 'DB' 'DC')  )  )
													THEN      activite   =    'ENTREPRISE'  ;

		IF    PRODUIT  IN  ('RBA' 'RCD')  AND
		(  typmarc1  IN  ('05')  OR  (  typmarc1  =  '  '  AND  formule  =  'DD')  )
													THEN      activite   =    'M. OEUVRE'  ;

		IF    PRODUIT  IN  ('RBA' 'RCD')  AND
		(  typmarc1  IN  ('06' '14')  OR  (  typmarc1  =  '  '  AND  formule  =  'DE'  )  )
													THEN      activite   =    'FABRICANT'  ;

		IF    PRODUIT  IN  ('RBA')  AND  typmarc1  IN  ('07')
													THEN      activite   =    'NEGOCIANT'  ;

		IF    PRODUIT  IN  ('RBA' 'RCD')  AND  typmarc1  IN  ('08')
													THEN      activite   =    'PROMOTEUR'  ;

		IF    PRODUIT  IN  ('RBA' 'RCD')  AND  typmarc1  IN  ('09')
													THEN      activite   =    'M. OUVRAGE'  ;

		IF    PRODUIT  IN  ('RBA' 'RCD')  AND  typmarc1  IN  ('10')
													THEN      activite   =    'MARCHAND'  ;

		IF    PRODUIT  IN  ('RBA'  )  AND  typmarc1  IN  ('11')
													THEN      activite   =    'FABRICANT'  ;

		IF    PRODUIT  IN  ('RBA' 'RCD')  AND  typmarc1  IN  ('12')
													THEN      activite   =    'M. OEUVRE G.C';

		IF    PRODUIT  IN  ('RBA' 'RCD')  AND  typmarc1  IN  ('13')
													THEN      activite   =    'M. OEUVRE G.C'  ;

		IF    PRODUIT  IN  ('RCD')  AND  typmarc1  IN  ('07' '14')
													THEN      DO  :
															  PRODUIT    =    'RBA'  ;
															  activite   =    'NEGOCIANT'  ;
															  END  ;

		IF    PRODUIT  =  'DPC'

								THEN      DO  :
								IF    nat_cnt  =  '01'  THEN   activite  =  'COMP. GROUPE'  ;
								IF    nat_cnt  =  '02'  THEN   activite  =  'PUC BATIMENT'  ;
								IF    nat_cnt  =  '03'  THEN   activite  =  'PUC G.C.    '  ;
								IF    nat_cnt  =  '04'  THEN   activite  =  'GLOBAL CHANT'  ;
								IF    nat_cnt  =  '05'  THEN   activite  =  'DEC G.C.     '  ;
								IF    nat_cnt  =  '06'  THEN   activite  =  'DIVERS       '  ;
								IF    nat_cnt  =  '07'  THEN   activite  =  'DEC BATIMENT'  ;
								END  ;

RUN;

PROC   DATASETS  ;   DELETE  MARCHE  ;   QUIT  ;   RUN  ;

DATA       POLIC_C  ;

ATTRIB     PTGST                       FORMAT  =  $3.  ;

		%SEGMENTA  ;

SET        POLIC_CU.POLIC_CU  ;

		IF                Tab_Prd_CSTR.find ()  =  0    ;

		IF                CMARCH  IN  ('6')  ;

		IF       INDREGUL  IN  ('O')     THEN   revisable  =  'oui'  ;
										ELSE   revisable  =  'non'  ;

		PTGST  =  poingest  ;

RUN             ;

%Lib_PTGST  (  Tab  =  POLIC_C  )  ;


PROC  SORT  DATA  =  CONSTRUCU    NODUPKEY  ;  BY   POLICE   ;  RUN  ;
PROC  SORT  DATA  =  POLIC_C      NODUPKEY  ;  BY   POLICE   ;  RUN  ;

DATA       CONSTRUCU  ;

MERGE      CONSTRUCU  (  IN  =  x  )
		   POLIC_C    (  IN  =  y  )  ;

BY         POLICE  ;
IF         y  ;

RUN  ;

PROC   DATASETS  ;   DELETE  POLIC_C  ;   QUIT  ;   RUN  ;

PROC   SORT      DATA  =  CONSTRUCU                    ;  BY   activite  ;  RUN  ;
PROC   SORT  NODUPKEY  DATA  =  SAS_C.Typrd_2          ;  BY   activite  ;  RUN  ;
DATA       CONSTRUCU  ;

MERGE      CONSTRUCU       (  IN  =  a  )
		   SAS_C.Typrd_2   (  IN  =  b  )
		   WHERE  =  (  activite  NOT IN  (  "",
					"DOMMAGES OUVRAGES",
					"RC ENTREPRISES DE CONSTRUCTION",
					"RC DECENNALE",
					"TOUS RISQUES CHANTIERS"  )  )  );

BY         activite  ;
IF         a  ;

		IF      NOT  b     THEN     DO  ;

		IF   LSSSEG  IN  (  "TOUS RISQUES CHANTIERS"  )  THEN   Type_Produit  =  "TRC"   ;  ELSE
		IF   LSSSEG  IN  (  "DOMMAGES OUVRAGES"  )       THEN   Type_Produit  =  "DO"    ;  ELSE
														Type_Produit  =  "Autres"  ;
														END  ;

		IF   PRODUIT  in  ('RCC')                        THEN   Type_Produit  =  "Entreprises"  ;
		IF   LSSSEG  =  "RC DECENNALE"  AND  Type_Produit  IN  ("Artisans")
														THEN   cssseg  =  7  ;

RUN  ;

PROC  SORT  DATA  =  CONSTRUCU   NODUPKEY  ;  BY   POLICE   ;  RUN  ;

data CONSTRUCU_AZEC;
set CONSTRUCU;
keep POLICE CDPROD SEGMENT TYPE_PRODUIT;
run;

```

## REPRISES_HISTORIQUES_PTF_MVTS.sas

```sas
%macro reprise_historique_ptf_mvts(vision);

    %let MPWINDOWS = A$EPSIL3;
    %let rep_Pgm_Prod_DTM_Construction = /sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction;

    /*  %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_CHARG_DATARISK_ONEBI_BIFR.sas"; */
        %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_AZ_MACRO.sas";
        %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_AZEC_MACRO.sas";
        %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_CONSOLIDATION_MACRO.sas";

            %if %eval(%sysfunc(year(%sysfunc(today()))) - %substr(&vision.,1,4)) > 10 %then %do ;
                %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_RUN_REPR_HIST.sas";
            %end;
            %else %do;
                %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_RUN.sas";
            %end;

%mend;

%reprise_historique_ptf_mvts(vision=200812);

%reprise_historique_ptf_mvts(vision=200912);

%reprise_historique_ptf_mvts(vision=201012);

%reprise_historique_ptf_mvts(vision=201112);

%reprise_historique_ptf_mvts(vision=201212);

%reprise_historique_ptf_mvts(vision=201312);

%reprise_historique_ptf_mvts(vision=201411);
%reprise_historique_ptf_mvts(vision=201412);

%reprise_historique_ptf_mvts(vision=201511);
%reprise_historique_ptf_mvts(vision=201512);


%reprise_historique_ptf_mvts(vision=201611);
%reprise_historique_ptf_mvts(vision=201612);


%reprise_historique_ptf_mvts(vision=201711);
%reprise_historique_ptf_mvts(vision=201712);



%reprise_historique_ptf_mvts(vision=201801);
%reprise_historique_ptf_mvts(vision=201802);
%reprise_historique_ptf_mvts(vision=201803);
%reprise_historique_ptf_mvts(vision=201804);
%reprise_historique_ptf_mvts(vision=201805);
%reprise_historique_ptf_mvts(vision=201806);
%reprise_historique_ptf_mvts(vision=201807);
%reprise_historique_ptf_mvts(vision=201808);
%reprise_historique_ptf_mvts(vision=201809);
%reprise_historique_ptf_mvts(vision=201810);
%reprise_historique_ptf_mvts(vision=201811);
%reprise_historique_ptf_mvts(vision=201812);

%reprise_historique_ptf_mvts(vision=201901);
%reprise_historique_ptf_mvts(vision=201902);
%reprise_historique_ptf_mvts(vision=201903);
%reprise_historique_ptf_mvts(vision=201904);
%reprise_historique_ptf_mvts(vision=201905);
%reprise_historique_ptf_mvts(vision=201906);
%reprise_historique_ptf_mvts(vision=201907);
%reprise_historique_ptf_mvts(vision=201908);
%reprise_historique_ptf_mvts(vision=201909);
%reprise_historique_ptf_mvts(vision=201910);
%reprise_historique_ptf_mvts(vision=201911);
%reprise_historique_ptf_mvts(vision=201912);

%reprise_historique_ptf_mvts(vision=202001);
%reprise_historique_ptf_mvts(vision=202002);
%reprise_historique_ptf_mvts(vision=202003);
%reprise_historique_ptf_mvts(vision=202004);
%reprise_historique_ptf_mvts(vision=202005);
%reprise_historique_ptf_mvts(vision=202006);
%reprise_historique_ptf_mvts(vision=202007);
%reprise_historique_ptf_mvts(vision=202008);
%reprise_historique_ptf_mvts(vision=202009);
%reprise_historique_ptf_mvts(vision=202010);
%reprise_historique_ptf_mvts(vision=202011);
%reprise_historique_ptf_mvts(vision=202012);

%reprise_historique_ptf_mvts(vision=202101);
%reprise_historique_ptf_mvts(vision=202102);
%reprise_historique_ptf_mvts(vision=202103);
%reprise_historique_ptf_mvts(vision=202104);
%reprise_historique_ptf_mvts(vision=202105);
%reprise_historique_ptf_mvts(vision=202106);
%reprise_historique_ptf_mvts(vision=202107);
%reprise_historique_ptf_mvts(vision=202108);
%reprise_historique_ptf_mvts(vision=202109);
%reprise_historique_ptf_mvts(vision=202110);
%reprise_historique_ptf_mvts(vision=202111);
%reprise_historique_ptf_mvts(vision=202112);

%reprise_historique_ptf_mvts(vision=202201);
%reprise_historique_ptf_mvts(vision=202202);
%reprise_historique_ptf_mvts(vision=202203);
%reprise_historique_ptf_mvts(vision=202204);
%reprise_historique_ptf_mvts(vision=202205);
%reprise_historique_ptf_mvts(vision=202206);
%reprise_historique_ptf_mvts(vision=202207);
%reprise_historique_ptf_mvts(vision=202208);
%reprise_historique_ptf_mvts(vision=202209);
%reprise_historique_ptf_mvts(vision=202210);
%reprise_historique_ptf_mvts(vision=202211);
%reprise_historique_ptf_mvts(vision=202212);

%reprise_historique_ptf_mvts(vision=202301);
%reprise_historique_ptf_mvts(vision=202302);
%reprise_historique_ptf_mvts(vision=202303);
%reprise_historique_ptf_mvts(vision=202304);
%reprise_historique_ptf_mvts(vision=202305);
%reprise_historique_ptf_mvts(vision=202306);
%reprise_historique_ptf_mvts(vision=202307);
%reprise_historique_ptf_mvts(vision=202308);
%reprise_historique_ptf_mvts(vision=202309);
%reprise_historique_ptf_mvts(vision=202310);
%reprise_historique_ptf_mvts(vision=202311);
%reprise_historique_ptf_mvts(vision=202312);

```

## indexation_v2.sas

```sas
/* Macro d'indexation des capitaux*/
%MACRO indexation_v2(DATE = ., IND = 1, NOMMT = MTCAPI, NOMNAT = CDPRVB, NOMIND = PRPRVC);

    OPTIONS FMTSEARCH=(INDICES);
    FORMAT VAL1 VAL2 $8.;
    FORMAT DATE DDMMYY10.;

    %IF &DATE EQ . %THEN %DO;
        JOUR1=SUBSTR(PUT(DTECHANN,4.),3,2);
        MOIS1=SUBSTR(PUT(DTECHANN,4.),1,2);
        ANNEE1=YEAR(DATE());
        DATE=MDY(MOIS1,JOUR1,ANNEE1);
        IF DATE > DATE()
        THEN DO;
            ANNEE1=ANNEE1-1;
            DATE=MDY(MOIS1,JOUR1,ANNEE1);
        END;
        INDXORIG=&NOMIND&IND;
    %END;
    %ELSE %DO;
        JOUR1=DAY(&DATE);
        MOIS1=MONTH(&DATE);
        ANNEE1=YEAR(&DATE);
        DATE1=MDY(MOIS1,JOUR1,ANNEE1);
        JOUR2=SUBSTR(PUT(DTECHANN,4.),3,2);
        MOIS2=SUBSTR(PUT(DTECHANN,4.),1,2);
        DATE2=MDY(MOIS2,JOUR2,ANNEE1);
        IF DATE1 < DATE2 THEN DO;
            ANNEE1=ANNEE1-1;
            DATE=MDY(MOIS2,JOUR2,ANNEE1);
        END;
        ELSE DATE=DATE2;
        VAL1=&NOMNAT&IND||PUT(DTEFSITT,Z5.);
        IF SUBSTR(VAL1,1,1)='0' THEN INDXORIG=PUT(VAL1,$INDICE.);
        ELSE INDXORIG=1;
    %END;
    VAL2=&NOMNAT&IND||PUT(DATE,Z5.);
    IF SUBSTR(VAL2,1,1)='0' THEN INDXINTG=PUT(VAL2,$INDICE.);
    ELSE INDXINTG=1;
    IF DATE<DTEFSITT THEN DO;
        INDXINTG=1;
        INDXORIG=1;
    END;
    FORMAT &NOMMT&IND.i 16.;
    &NOMMT&IND.i=IFN(INDXINTG/INDXORIG=.,&NOMMT&IND,&NOMMT&IND*(INDXINTG/INDXORIG));
    INDXINTG&IND.i = INDXINTG;
    INDXORIG&IND.i = INDXORIG;

    /* DROP DATE; JOUR: MOIS1 MOIS2 ANNEE: VAL1 VAL2 /*INDXORIG INDXINTG*/

%MEND;
```

