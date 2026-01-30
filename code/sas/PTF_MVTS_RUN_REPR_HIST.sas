/******************************************************************************
 * FILE: PTF_MVTS_RUN_REPR_HIST.sas
 * 
 * Description: Historical reprises runner for PTF_MVTS pipeline
 *              Executes portfolio and movements processing for visions > 5 years old
 *              with special library allocation logic based on vision age
 *
 * Logic:
 *   - Visions < 5 years old: Use current year allocation
 *   - Visions = 5 years old: Conditional allocation based on month
 *   - Visions > 5 years old: Fixed December allocation
 *   - AZEC processing only for visions between 201211 and 202008
 *
 * Dependencies:
 *   - Remote STP3 server connection
 *   - Historical portfolio library allocation macros
 *   - AZ and AZEC processing macros
 ******************************************************************************/


/******************************************************************************
 * MACRO: run_ptf_mvts_repr_hist
 * 
 * Description: Main orchestration macro for historical PTF_MVTS reprises
 *
 * Parameters: None (uses &vision. macro variable)
 ******************************************************************************/

%macro run_ptf_mvts_repr_hist();

    /* ========================================================================
     * REMOTE SERVER CONNECTION
     * ======================================================================== */
    
    options comamid=tcp;
    %let serveur=STP3 7013;
    signon noscript remote=serveur user=%upcase(&sysuserid.) password="&MPWINDOWS";

    /* ========================================================================
     * DATE AND VISION PARAMETERS
     * ======================================================================== */
    
    /* LIBRAIRIES STP3 */
    %let annee = %sysfunc(compress(%substr(&vision., 1, 4)));
    %let mois = %sysfunc(compress(%substr(&vision., 5, 2)));
    %put &annee &mois;

    /* Calculate historical period offset */
    %let P = %eval(&annee. - %eval(%sysfunc(year(%sysfunc(today()))) - 1));
    %put &P.;

    /* ========================================================================
     * STP3 LIBRARY ASSIGNMENTS (HISTORICAL)
     * ======================================================================== */
    
    libname PTF16 "INFH.IIAAP6$$.IPFE16(&P.)" disp=shr server=serveur;
    libname PTF36 "INFH.IIAAP6$$.IPFE36(&P.)" disp=shr server=serveur;

    LIBNAME segmprdt "INFH.IDAAP6$$.SEGMPRDT(0)" DISP = SHR SERVER = serveur;
    LIBNAME PRDCAP 'INFP.IIA0P6$$.PRDCAP' DISP = SHR SERVER = serveur;
    LIBNAME CLIENT1 'infp.ima0p6$$.cliact14' DISP=SHR SERVER=SERVEUR;
    LIBNAME CLIENT3 'infp.ima0p6$$.cliact3' DISP=SHR SERVER=SERVEUR;
    /* FIN DES LIBRAIRIES STP3 */

    /* ========================================================================
     * LOCAL SAS LIBRARIES
     * ======================================================================== */
    
    /* LIBRAIRIES SAS */
    LIBNAME Dest "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/Construction";
    LIBNAME Ref "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref";
    LIBNAME MIG_AZEC "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/REFERENTIEL";
    LIBNAME PT_GEST "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/transverse/sasdata/pros_midcorp/Ref/points_gestion";
    LIBNAME AACPRTF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/lkhoumm/sasdata/BDC/AACPRTF";
    LIBNAME BINSEE "/sasprod/prod/prod/run/azi/d2d/w6/particuliers/dm_crm/partage/param/pro";
    LIBNAME W6 "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/ALEX_MIS/basecli_cumule";
    LIBNAME RISK_REF "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/202210";
    /* FIN DES LIBRAIRIES SAS */

    /* ========================================================================
     * DTM CONSTRUCTION LIBRARIES
     * ======================================================================== */
    
    /* LIBRAIRIES DTM CONSTRUCTION */
    x "mkdir /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
    LIBNAME CUBE "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";

    /* %if %sysfunc(exist(cube.MVT_PTF202304)) = 0 %then %do; */

    /* ========================================================================
     * INCLUDE UTILITY MACROS  
     * ======================================================================== */
    
    /* MACROS-PGM UTILES */
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/legouil/fonctions/generiques_v4.sas";
    %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/CODIFICATION_ISIC_CONSTRUCTION.sas";
    OPTIONS sasautos = ("/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/_commun/Exploitation/sasmcr/sasalloc", 
                        "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/_commun/Exploitation/sasmcr/sastools", 
                        "/sasprod/produits/SASEnterpriseBIServer/segrac/METGAZDT/utilisateurs/aitihvi/saspgm/Cubes",
                        SASAUTOS) mautosource mrecall;

    /* ========================================================================
     * SPECIAL LIBRARY ALLOCATION BASED ON VISION AGE
     * ======================================================================== */
    
    /* Visions > 5 years old */
    %if %substr(&vision., 1, 4) < %sysfunc(year(%sysfunc(today()))) - 5 %then %do;
        %if &annee. <= 2014 %then %do;
            %put annee = 15 mois = 12;
            %alloc_spe_v3(2015, 12, IPFSPE);
        %end;
        %else %do;
            %put annee = &annee. mois = 12;
            %alloc_spe_v3(&annee., 12, IPFSPE);
        %end;
    %end;

    /* Visions exactly 5 years old */
    %if %substr(&vision., 1, 4) = %sysfunc(year(%sysfunc(today()))) - 5 %then %do;
        %if %substr(&vision., 5, 2) < %sysfunc(month(%sysfunc(today()))) %then %do;
            %let annee_1 = %eval(&annee. - 1);
            %put &annee_1;
            %put annee = &annee_1 mois = 12;
            %alloc_spe_v3(&annee_1., 12, IPFSPE);
        %end;
        %else %do;
            %put annee = &annee. mois = &mois.;
            %alloc_spe_v3(&annee., &mois., IPFSPE);
        %end;
    %end;

    /* Visions < 5 years old */
    %if %substr(&vision., 1, 4) > %sysfunc(year(%sysfunc(today()))) - 5 %then %do;
        %put annee = &annee. mois = &mois.;
        %alloc_spe_v3(&annee., &mois., IPFSPE);
    %end;

    /* ========================================================================
     * AZEC SPECIFIC ALLOCATION (FOR ELIGIBLE VISIONS)
     * ======================================================================== */
    
    /* AZEC */
    %if &vision. <=202008 and &vision. >= 201211 %then %do;
        %alloc_azec_v3(&annee., &mois.);
    %end;

    /* ========================================================================
     * DATE MACRO VARIABLES SETUP
     * ======================================================================== */
    
    data _null_;
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
        call symput("ANNEE", Annee);
        call symput("MOIS", Mois);
        Call symput("Vision", compress(Vision));
        call symputx("date_run", date_run);
        call symputx("hour", hour);
    run;

    /* ========================================================================
     * EXECUTE PTF_MVTS PIPELINE
     * ======================================================================== */
    
    /* Process AZ data */
    %az_mvt_ptf(&annee., &mois.);

    /* Process AZEC data (if applicable) */
    %if &vision >=201211 %then %do;
        %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm/REF_segmentation_azec.sas";
        %azec_mvt_ptf(&vision.);
    %end;

    /* Consolidate AZ and AZEC */
    %consolidation_az_azec_mvt_ptf;
    
%mend;


/* ============================================================================
 * EXECUTION
 * ============================================================================ */

%run_ptf_mvts_repr_hist();
