%let arbobase = /net/home/&sysuserid./METGAZDT;
%let date = &DATEARRET.;
%let PrJnv1a = "01JAN&annee."d;

DATA _NULL_;
    NOM_CLIENT = "TEST" ;
    IF month ( &date. ) NE 01 THEN DO ;
        mois_az = month ( &date. ) - 1;
        an = year ( &date. ) ;
    END;
    ELSE DO ;
        mois_az = 12;
        an = year ( &date. ) - 1 ;
    END;
    IF mois_az LT 10 THEN mois_az1 = 0||trim(left(mois_az)) ;
    ELSE mois_az1 = mois_az ;
    CALL SYMPUTX ( 'ME' , mois_az1 , "G" ) ;
    CALL SYMPUTX ( 'AE' , an , "G" ) ;
    CALL SYMPUTX ( 'Client' , NOM_CLIENT , "G" ) ;
    CALL SYMPUTX ( 'ANNEE' , an ) ;
    CALL SYMPUTX ( 'mois_az' , put(mois_az, z2.)) ;
RUN ;

%LET POL_IST = ( '084443777','084443778','084548930','084548939', '084700586','084700587','054463672','54463672' ) ;
/* %LET POL_IST1 = ( '084214' ) ; %LET POL_IST2 = ( '84214' ) ;*/
%LET PRODUIT = ( 'A00' 'A01' 'AA1' 'AB1' 'AG1' 'AG8' 'AR1' 'ING' 'PG1' 'AA6' 'AG6' 'AR2' 'AU1' 'AA4' 'A03' 'AA3' 'AG3' 'AM3' 'MAI' 'MA0' 'MA1' 'MA2' 'MA3' 'MA4' 'MA5' 'MT0' 'MR0' 'AAC' 'GAV' 'GMC' 'MB1' 'MB2' 'MB3' 'MED' 'MH0' 'MP0' 'MP1' 'MP2' 'MP3' 'MP4' 'MPG' 'MPP' 'MI0' 'MI1' 'MI2' 'MI3' 'PI2' ) ;

%MACRO SELECTION ;
    ATTRIB NMCLIENT FORMAT = $50. ;
    IF POLICE IN &POL_IST ;
    /* IF SUBSTR (POLICE,1,6) IN &POL_IST1 OR SUBSTR (POLICE,1,5) IN &POL_IST2 ;*/
    NMCLIENT = "&Client" ;
%MEND ;

%MACRO PGST_LG ;
    IF PRODUIT IN ("FA7") THEN DO;
        IF categoaa IN ("C38") THEN protocole="LCD";
        IF categoaa IN ("C48") THEN protocole="LLD";
        IF INTERMED IN ("14142") THEN protocole="ARVAL";
        IF INTERMED IN ("35158") THEN protocole="FLEXITRANS";
        IF INTERMED IN ("25043") THEN protocole="AON";
        IF (INTERMED IN ("23387") AND categoaa IN ("C66")) THEN protocole="Auto Ecole Filhet Allart";
        IF (INTERMED IN ("35364") AND categoaa IN ("C48")) THEN protocole="Athlon";
        IF (INTERMED IN ("21928") AND categoaa IN ("C48")) THEN protocole="Avis G.E.";
        IF (INTERMED IN ("27047") AND categoaa IN ("C48")) THEN protocole="Satec";
        IF (INTERMED IN ("27384") AND categoaa IN ("C48")) THEN protocole="DCS Fleet";
        IF (INTERMED IN ("14142") AND categoaa IN ("C66")) THEN protocole="Auto Ecole Carène";
        IF (INTERMED IN ("10227") AND ((txcommis=18 AND categoaa IN ("C11")) or (txcommis=20 AND categoaa NOT IN ("C11")))) THEN protocole="Verlingue";
        IF (INTERMED IN ("13447") AND txcommis=21) THEN protocole="Marsh";
        IF POLICE IN ("N26186204" "N26186213" "029474812" "029604671" "029773332") THEN protocole="Alphabet GS berger Simon";
        IF POLICE IN ("L76200012") THEN protocole="Programme International";
        IF POLICE IN ("070851144") THEN protocole="Sogepa";
        IF POLICE IN ("029175361" "029175362" "029175363" "029175364" "029176893" "029254559" "029322411" "029693540") THEN protocole="DIF";
        IF POLICE IN ("CO1802621" "CO1802591") THEN protocole="Fast Carène";
        IF protocole NOT IN (" ") THEN poingest="M27";
    END;
%MEND ;

%MACRO Lib_PTGST ( Tab = ) ;
    /*---------------------------------------------------------------*/
    /* REGION */
    /*--------------------------- Debut ------------------------------*/
    PROC SORT DATA = &Tab ;
        BY PTGST ;
    RUN ;
    DATA &Tab ;
        MERGE &Tab ( IN = a ) WPTGST ( IN = b ) ;
        BY PTGST ;
        IF a ;
        IF NOT b THEN REGION = 'Autres' ;
    RUN ;
    /*-----------------------------------------------------------------*/
    /* REGION */
    /*--------------------------- FIN -----------------------------*/
%MEND ;

%MACRO SEGMENTA ;
    LENGTH CDPROD $5 CPROD $5 cmarch $1 lmarch $44 lmarch2 $46 cseg $1 lseg $52 lseg2 $54 cssseg $1 lssseg $45 lssseg2 $47 lprod $34 segment $12 ;
    if _N_ = 1 THEN DO ;
        DECLARE HASH Tab_Prd_CSTR ( DATASET : "LOB" , ORDERED : "YES" ) ;
        Tab_Prd_CSTR.defineKey ("PRODUIT") ;
        Tab_Prd_CSTR.defineData ("CDPROD","CPROD","cmarch","lmarch","lmarch2","cseg","lseg","lseg2","cssseg","lssseg","lssseg2","lprod","segment") ;
        Tab_Prd_CSTR.defineDone () ;
        CAll missing ( CDPROD, CPROD, cmarch, lmarch, lmarch2, cseg, lseg, lseg2, cssseg, lssseg, lssseg2, lprod, segment ) ;
    END ;
%MEND ;

%MACRO TYP_PRD_CSTR ( TABLE = ) ;
    PROC SORT DATA = &TABLE ;
        BY POLICE ;
    RUN ;
    DATA &TABLE ;
        ATTRIB Segment_3 FORMAT = $27. ;
        MERGE &TABLE ( IN = x ) CONSTRCU ( IN = y KEEP = POLICE Type_Produit ) ;
        BY POLICE ;
        IF x ;
        IF lmarch2 IN ('6_CONSTRUCTION') THEN DO ;
            IF Type_Produit IN ("Artisans") THEN Segment_3 = "Artisans" ;
            ELSE IF Type_Produit IN ("TRC","DO","CNR","Autres Chantiers") THEN Segment_3 = "Chantiers" ;
            ELSE Segment_3 = "Renouvelables hors artisans" ;
        END ;
        Type_Produit_2 = Type_Produit ;
    RUN ;
%MEND ;

PROC FORMAT ;
    PICTURE montant low-<0 = '000 000 000 999,99' ( PREFIX = '-' MULT = 100 ) 0-high = '000 000 000 999,99' ( MULT = 100 ) ;
    PICTURE nombre low-<0 = '000 000 009' ( PREFIX = '-' ) 0-high = '000 000 009' ;
    OPTIONS LINESIZE = 132 PAGESIZE = 500 MISSING = . ;
RUN ;

LIBNAME SAS_ASP "$SASDATA/SPA" ; RUN ;
LIBNAME SASDT "$SASDATA" ; RUN ;
LIBNAME SAS_P "$SASDATA/PTF" ; RUN ;
LIBNAME SAS_C "$SASDATA/CSTR" ; RUN ;
LIBNAME AAFICHE "$SASDATA/BDC/AAFICHE" ; RUN ;
LIBNAME AATDBTTM "&arbobase./utilisateurs/lkhoumm/sasdata/BDA/AATDBTTM" ; RUN ;
LIBNAME AA_GRANU "&arbobase./utilisateurs/lkhoumm/sasdata/BDA/AA_GRANU" ; RUN ;
LIBNAME AACPRTF "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AACPRTF" ; RUN ;
LIBNAME AAFICHE "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AAFICHE" ; RUN ;
LIBNAME AA_CSIN "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AA_CSIN" ; RUN ;
LIBNAME AARCAGD "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AARCAGD" ; RUN ;
LIBNAME AASTCLNT "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AASTCLNT" ; RUN ;
LIBNAME AATDBMC "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AATDBMC" ; RUN ;
LIBNAME AA_BTP "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AA_BTP" ; RUN ;
LIBNAME AA_ROC_A "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AA_ROC_A" ; RUN ;
LIBNAME AAIPC "&arbobase./utilisateurs/lkhoumm/sasdata/BDC/AAIPC" ; RUN ;
LIBNAME SASDT_C "&arbobase./utilisateurs/lkhoumm/sasdata/SMC/&annee/&mois_az" ; RUN ;
LIBNAME SAS_P "&arbobase./utilisateurs/lkhoumm/sasdata/PTF" ; RUN ;
LIBNAME SAS_C "&arbobase./utilisateurs/lkhoumm/sasdata/CSTR" ; RUN ;

DATA LOB ;
    SET SAS_C.LOB ;
    IF cmarch IN ('6') ;
RUN ;

PROC SORT DATA = LOB ;
    BY PRODUIT CPROD CDPROD ;
RUN ;

DATA WPTGST ;
    SET SAS_C.PTGST ( KEEP = PTGST REGION P_Num ) ;
RUN ;

PROC SORT DATA = WPTGST NODUPKEY ;
    BY PTGST ;
RUN ;

DATA categ ;
    SET SAS_C.import_catmin ;
RUN ;

LIBNAME POLIC_CU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/polic_cu" ;
LIBNAME GARANTCU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/garantcu" ;
LIBNAME CONSTRCU "/net/home/tohoun/METGAZDT/AZEC_HISTO/SAS_save_diverscu/202009/constrcu";
*LIBNAME CONSTRCU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/constrcu" ;
LIBNAME RISTECCU "/net/home/tohoun/METGAZDT/AZEC_HISTO/SAS_save_diverscu/202009/risteccu";
*LIBNAME RISTECCU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/risteccu" ;
LIBNAME MULPROCU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/mulprocu" ;
LIBNAME SINISTCU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/sinistcu" ;
LIBNAME PRIMGRCU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/prigarcu" ;
LIBNAME REGSINCU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/regsincu" ;
LIBNAME capitxcu "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/capitxcu" ;
LIBNAME INCENDCU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/incendcu" ;
LIBNAME MPACU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/mpacu";
LIBNAME RCENTCU "&arbobase./AZEC_HISTO/SAS_save_diverscu/202009/rcentcu" ;

/* LIBNAME CONSTRCU '$CONSTRCU' ; LIBNAME RISTECCU '$RISTECCU' ; */

DATA typmarc4 ( KEEP = POLICE typmarc4 ) typmarc5 ( KEEP = POLICE typmarc5 ) typmarc6 ( KEEP = POLICE typmarc6 ) ;
    SET GARANTCU.GARANTCU ( KEEP = POLICE garantie branche );
    IF branche IN ( 'CO','RT') ;
    LENGTH typmarc4 $8. typmarc5 $8. typmarc6 $8. ;
    IF garantie = 'CNAA' THEN DO ;
        typmarc4 = '01 02 06' ;
        OUTPUT typmarc4 ;
    END ;
    IF garantie = 'CNAP' THEN DO ;
        typmarc5 = '04 ' ;
        OUTPUT typmarc5 ;
    END ;
    IF garantie = 'CNPR' THEN DO ;
        typmarc6 = '08 09 10' ;
        OUTPUT typmarc6 ;
    END ;
RUN;

LIBNAME GARANTCU CLEAR ;

PROC SORT DATA = typmarc4 NODUPKEY ;
    BY POLICE ;
RUN ;

PROC SORT DATA = typmarc5 NODUPKEY ;
    BY POLICE ;
RUN ;

PROC SORT DATA = typmarc6 NODUPKEY ;
    BY POLICE ;
RUN ;

DATA MARCHE ( KEEP = POLICE typmarc7 ) ;
    MERGE typmarc4 typmarc5 typmarc6 ;
    BY POLICE ;
    LENGTH typmarc7 $8. ;
    IF typmarc4 = ' ' AND typmarc5 = ' ' THEN typmarc7 = typmarc6 ;
    IF typmarc4 = ' ' AND typmarc6 = ' ' THEN typmarc7 = typmarc5 ;
    IF typmarc5 = ' ' AND typmarc6 = ' ' THEN typmarc7 = typmarc4 ;
RUN ;

PROC DATASETS ;
    DELETE typmarc4 ;
QUIT ;
RUN ;

PROC DATASETS ;
    DELETE typmarc5 ;
QUIT ;
RUN ;

PROC DATASETS ;
    DELETE typmarc6 ;
QUIT ;
RUN ;

DATA CONSTRCU ( KEEP = POLICE PRODUIT typmarc1 ltypmar1 formule nat_cnt ) ;
    %SEGMENTA ;
    SET CONSTRCU.CONSTRCU ( KEEP = POLICE PRODUIT typmarc1 ltypmar1 formule nat_cnt ) RISTECCU.RISTECCU ( KEEP = POLICE PRODUIT formule ) ;
    IF Tab_Prd_CSTR.find () = 0 ;
    IF CMARCH IN ('6') ;
RUN ;

*LIBNAME CONSTRCU CLEAR ;
*LIBNAME RISTECCU CLEAR ;

PROC SORT DATA = CONSTRCU NODUPKEY ;
    BY POLICE ;
RUN ;

PROC SORT DATA = MARCHE NODUPKEY ;
    BY POLICE ;
RUN ;

DATA CONSTRCU ;
    ATTRIB activite FORMAT = $30. ;
    MERGE CONSTRCU ( IN = x ) MARCHE ( IN = y ) ;
    BY POLICE ;
    IF x ;
    IF typmarc1 = ' ' THEN typmarc1 = typmarc7 ;
    /******** nature de contrat champs valable que pour le PRODUIT DPC ********/
    IF PRODUIT NOT IN ('DPC') THEN nat_cnt = ' ' ;
    /*************** creation du champs activite depuis ltypmarc1 **************/
    IF PRODUIT IN ('RBA' 'RCD' ) AND typmarc1 = '01' THEN activite = 'ARTISAN' ;
    IF PRODUIT IN ('RBA' 'RCD') AND ( typmarc1 IN ('02' '03' '04') OR ( typmarc1 = ' ' AND formule IN ('DA' 'DB' 'DC') ) ) THEN activite = 'ENTREPRISE' ;
    IF PRODUIT IN ('RBA' 'RCD') AND ( typmarc1 IN ('05') OR ( typmarc1 = ' ' AND formule = 'DD') ) THEN activite = 'M. OEUVRE' ;
    IF PRODUIT IN ('RBA' 'RCD') AND ( typmarc1 IN ('06' '14') OR ( typmarc1 = ' ' AND formule = 'DE' ) ) THEN activite = 'FABRICANT' ;
    IF PRODUIT IN ('RBA') AND typmarc1 IN ('07') THEN activite = 'NEGOCIANT' ;
    IF PRODUIT IN ('RBA' 'RCD') AND typmarc1 IN ('08') THEN activite = 'PROMOTEUR' ;
    IF PRODUIT IN ('RBA' 'RCD') AND typmarc1 IN ('09') THEN activite = 'M. OUVRAGE' ;
    IF PRODUIT IN ('RBA' 'RCD') AND typmarc1 IN ('10') THEN activite = 'MARCHAND' ;
    IF PRODUIT IN ('RBA' ) AND typmarc1 IN ('11') THEN activite = 'FABRICANT' ;
    IF PRODUIT IN ('RBA' 'RCD') AND typmarc1 IN ('12') THEN activite = 'M. OEUVRE G.C';
    IF PRODUIT IN ('RBA' 'RCD') AND typmarc1 IN ('13') THEN activite = 'M. OEUVRE G.C' ;
    IF PRODUIT IN ('RCD') AND typmarc1 IN ('07' '14') THEN DO ;
        PRODUIT = 'RBA' ;
        activite = 'NEGOCIANT' ;
    END ;
    IF PRODUIT = 'DPC' THEN DO ;
        IF nat_cnt = '01' THEN activite = 'COMP. GROUPE' ;
        IF nat_cnt = '02' THEN activite = 'PUC BATIMENT' ;
        IF nat_cnt = '03' THEN activite = 'PUC G.C. ' ;
        IF nat_cnt = '04' THEN activite = 'GLOBAL CHANT' ;
        IF nat_cnt = '05' THEN activite = 'DEC G.C. ' ;
        IF nat_cnt = '06' THEN activite = 'DIVERS ' ;
        IF nat_cnt = '07' THEN activite = 'DEC BATIMENT' ;
    END ;
RUN;

PROC DATASETS ;
    DELETE MARCHE ;
QUIT ;
RUN ;

DATA POLIC_C ;
    ATTRIB PTGST FORMAT = $3. ;
    %SEGMENTA ;
    SET POLIC_CU.POLIC_CU ;
    IF Tab_Prd_CSTR.find () = 0 ;
    IF CMARCH IN ('6') ;
    IF INDREGUL IN ('O') THEN revisable = 'oui' ;
    ELSE revisable = 'non' ;
    PTGST = poingest ;
RUN ;

%Lib_PTGST ( Tab = POLIC_C ) ;

PROC SORT DATA = CONSTRCU NODUPKEY ;
    BY POLICE ;
RUN ;

PROC SORT DATA = POLIC_C NODUPKEY ;
    BY POLICE ;
RUN ;

DATA CONSTRCU ;
    MERGE CONSTRCU ( IN = x ) POLIC_C ( IN = y ) ;
    BY POLICE ;
    IF y ;
RUN ;

PROC DATASETS ;
    DELETE POLIC_C ;
QUIT ;
RUN ;

PROC SORT DATA = CONSTRCU ;
    BY activite ;
RUN ;

PROC SORT NODUPKEY DATA = SAS_C.Typrd_2 ;
    BY activite ;
RUN ;

DATA CONSTRCU ;
    MERGE CONSTRCU ( IN = a ) SAS_C.Typrd_2 ( IN = b WHERE = ( activite NOT IN ( "", "DOMMAGES OUVRAGES", "RC ENTREPRISES DE CONSTRUCTION", "RC DECENNALE", "TOUS RISQUES CHANTIERS" ) ) );
    BY activite ;
    IF a ;
    IF NOT b THEN DO ;
        IF LSSSEG IN ( "TOUS RISQUES CHANTIERS" ) THEN Type_Produit = "TRC" ;
        ELSE IF LSSSEG IN ( "DOMMAGES OUVRAGES" ) THEN Type_Produit = "DO" ;
        ELSE Type_Produit = "Autres" ;
    END ;
    IF PRODUIT in ('RCC') THEN Type_Produit = "Entreprises" ;
    IF LSSSEG = "RC DECENNALE" AND Type_Produit IN ("Artisans") THEN cssseg = 7 ;
RUN ;

PROC SORT DATA = CONSTRCU NODUPKEY ;
    BY POLICE ;
RUN ;

data CONSTRCU_AZEC;
    set CONSTRCU;
    keep POLICE CDPROD SEGMENT TYPE_PRODUIT;
run;
