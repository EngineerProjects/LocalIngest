/******************************************************************************
 * FILE: PTF_MVTS_CHARG_DATARISK_ONEBI_BIFR.sas
 * 
 * Description: Loads RISK data from One BI for visions >= 202210
 *              Handles character encoding (UTF-8 to LATIN9) for French accents
 *              Extracts three RISK tables: Q46, Q45, and QAN
 *
 * Input Sources:
 *   - RISK.IRD_RISK_Q46_&vision. : Risk questionnaire 46 data
 *   - RISK.IRD_RISK_Q45_&vision. : Risk questionnaire 45 data
 *   - RISK.IRD_RISK_QAN_&vision. : Risk annual questionnaire data
 *
 * Output:
 *   - OUT.IRD_RISK_Q46_&vision. : Q46 data with cleaned encoding
 *   - OUT.IRD_RISK_Q45_&vision. : Q45 data with cleaned encoding
 *   - OUT.IRD_RISK_QAN_&vision. : QAN data with cleaned encoding
 *
 * Logic:
 *   - Only runs for visions >= 202210
 *   - Removes accented characters to avoid encoding issues
 ******************************************************************************/


/******************************************************************************
 * MACRO: chargement_data_Risk
 * 
 * Description: Main macro for loading and cleaning RISK data from One BI
 *
 * Parameters: None (uses &vision. macro variable)
 ******************************************************************************/

%macro chargement_data_Risk;

    /* ========================================================================
     * VISION CHECK - ONLY FOR VISIONS >= 202210
     * ======================================================================== */
    
    %if &vision >= 202210 %then %do;
    
        /* ====================================================================
         * ONE BI REMOTE CONNECTION SETUP (UTF-8 ENCODING ONLY)
         * ==================================================================== */
        
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

        /* ====================================================================
         * ONE BI RISK LIBRARY ALLOCATION
         * ==================================================================== */
        
        %if %sysfunc(libref(risk)) %then %do;
            rsubmit;
                /* ALLOCATION BASES ONE BI sur une session distante */
                libname risk "/var/opt/data/data_project/sas94/bifr/azfronebi/data/shared_lib/IMS_CONTRAT/ird_risk/archives_mensuelles";
            endrsubmit;

            /* Rendre risk et Work visibles sur SASApp_AMOS */
            libname risk server=remhost;
            libname rwork slibref=work server=remhost outencoding='latin9';
        %end;
        %else %do;
            %put libref exists;
        %end;

        /* FIN DES LIBRAIRIES ONE-BI */

        /* ====================================================================
         * DTM CONSTRUCTION OUTPUT LIBRARY
         * ==================================================================== */
        
        /* LIBRAIRIES DTM CONSTRUCTION */
        /*%let vision = &VISIO;*/
        x "mkdir /sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision.";
        LIBNAME OUT "/sasprod/produits/SASEnterpriseBIServer/segrac/METGTECH/CONSTRUCTION/Output/&vision." outencoding='latin9';

        /* ====================================================================
         * INCLUDE TRANSCODIFICATION MACRO
         * ==================================================================== */
        
        %include "/sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction/02_TRANSCODIFICATION_ABS.sas";

        %syslput vision = &vision.;

        /* ====================================================================
         * REMOTE RISK DATA EXTRACTION AND ENCODING CLEANUP
         * ==================================================================== */
        
        rsubmit;
            /*
            ================================================================
            COMMENTED OUT: Old character replacement approach
            ================================================================
            proc sql;
                create table IRD_RISK_Q46_&vision. as select * from risk.IRD_RISK_Q46_&vision. ;
            quit;

            DATA IRD_RISK_Q46_&vision.;
                set IRD_RISK_Q46_&vision.;
                LBNATTRV =tranwrd(LBNATTRV,"-"," ");
                LBNATTRV =tranwrd(LBNATTRV,"_"," ");
                ... [character by character replacements]
                keep NOPOL DTOUCHAN DTRECTRX DTREFFIN CTPRVTRV CTDEFTRA LBNATTRV LBDSTCSC;
            RUN;
            proc download data=IRD_RISK_Q46_&vision. out =OUT.IRD_RISK_Q46_&vision.; run;
            ================================================================
            */

            /* ================================================================
             * Q46 RISK DATA - REMOVE ACCENTED CHARACTERS
             * ================================================================ */
            
            data IRD_RISK_Q46_&vision.;
                set risk.IRD_RISK_Q46_&vision.(encoding = "latin9");
                array allchar{*} _CHARACTER_;
                do i=1 to dim(allchar);
                    allchar{i}= tranwrd(allchar{i},"é","");
                    allchar{i}= tranwrd(allchar{i},"è","");
                    allchar{i}= tranwrd(allchar{i},"ê","");
                    allchar{i}= tranwrd(allchar{i},"à","");
                    allchar{i}= tranwrd(allchar{i},"â","");
                    allchar{i}= tranwrd(allchar{i},"ù","");
                    allchar{i}= tranwrd(allchar{i},"û","");
                    allchar{i}= tranwrd(allchar{i},"ô","");
                    allchar{i}= tranwrd(allchar{i},"ç","");
                    allchar{i}= tranwrd(allchar{i},"î","");
                    allchar{i}= tranwrd(allchar{i},"°","");
                end;
                drop i;
            run;
            proc download data=IRD_RISK_Q46_&vision. out =OUT.IRD_RISK_Q46_&vision.; 
            run;

            /* ================================================================
             * Q45 RISK DATA - REMOVE ACCENTED CHARACTERS
             * ================================================================ */
            
            data IRD_RISK_Q45_&vision.;
                set risk.IRD_RISK_Q45_&vision.(encoding = "latin9");
                array allchar{*} _CHARACTER_;
                do i=1 to dim(allchar);
                    allchar{i}= tranwrd(allchar{i},"é","");
                    allchar{i}= tranwrd(allchar{i},"è","");
                    allchar{i}= tranwrd(allchar{i},"ê","");
                    allchar{i}= tranwrd(allchar{i},"à","");
                    allchar{i}= tranwrd(allchar{i},"â","");
                    allchar{i}= tranwrd(allchar{i},"ù","");
                    allchar{i}= tranwrd(allchar{i},"û","");
                    allchar{i}= tranwrd(allchar{i},"ô","");
                    allchar{i}= tranwrd(allchar{i},"ç","");
                    allchar{i}= tranwrd(allchar{i},"î","");
                    allchar{i}= tranwrd(allchar{i},"°","");
                end;
                drop i;
            run;
            proc download data=IRD_RISK_Q45_&vision. out =OUT.IRD_RISK_Q45_&vision.; 
            run;

            /* ================================================================
             * QAN RISK DATA - REMOVE ACCENTED CHARACTERS
             * ================================================================ */
            
            data IRD_RISK_QAN_&vision.;
                set risk.IRD_RISK_QAN_&vision.(encoding = "latin9");
                array allchar{*} _CHARACTER_;
                do i=1 to dim(allchar);
                    allchar{i}= tranwrd(allchar{i},"é","");
                    allchar{i}= tranwrd(allchar{i},"è","");
                    allchar{i}= tranwrd(allchar{i},"ê","");
                    allchar{i}= tranwrd(allchar{i},"à","");
                    allchar{i}= tranwrd(allchar{i},"â","");
                    allchar{i}= tranwrd(allchar{i},"ù","");
                    allchar{i}= tranwrd(allchar{i},"û","");
                    allchar{i}= tranwrd(allchar{i},"ô","");
                    allchar{i}= tranwrd(allchar{i},"ç","");
                    allchar{i}= tranwrd(allchar{i},"î","");
                    allchar{i}= tranwrd(allchar{i},"°","");
                end;
                drop i;
            run;
            proc download data=IRD_RISK_QAN_&vision. out =OUT.IRD_RISK_QAN_&vision.; 
            run;
        endrsubmit;
    %end;
%mend;


/* ============================================================================
 * EXECUTION
 * ============================================================================ */

%chargement_data_Risk;
