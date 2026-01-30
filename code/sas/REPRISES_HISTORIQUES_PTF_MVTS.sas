/******************************************************************************
 * FILE: REPRISES_HISTORIQUES_PTF_MVTS.sas
 * 
 * Description: Historical reprises orchestration for PTF_MVTS pipeline
 *              Executes portfolio and movements processing for historical visions
 *              Dynamically selects appropriate RUN program based on vision age
 *
 * Logic:
 *   - For visions > 10 years old: Uses PTF_MVTS_RUN_REPR_HIST.sas
 *   - For more recent visions: Uses PTF_MVTS_RUN.sas
 *
 * Coverage: 
 *   - Historical visions from 2008-12 through 2023-12
 *   - Monthly granularity from 2018 onwards
 ******************************************************************************/


/******************************************************************************
 * MACRO: reprise_historique_ptf_mvts
 * 
 * Description: Orchestrates historical reprises for a specific vision
 *              Includes required macros and executes appropriate RUN program
 *
 * Parameters:
 *   - vision : Vision to process (YYYYMM format)
 *
 * Dependencies:
 *   - PTF_MVTS_AZ_MACRO.sas
 *   - PTF_MVTS_AZEC_MACRO.sas  
 *   - PTF_MVTS_CONSOLIDATION_MACRO.sas
 *   - PTF_MVTS_RUN_REPR_HIST.sas or PTF_MVTS_RUN.sas
 ******************************************************************************/

%macro reprise_historique_ptf_mvts(vision);

    /* ========================================================================
     * PARAMETER SETUP
     * ======================================================================== */
    
    %let MPWINDOWS = A$EPSIL3;
    %let rep_Pgm_Prod_DTM_Construction = /sasprod/produits/SASEnterpriseBIServer/segrac/METGDCPS/DATAMART/CONSTRUCTION/Pgm_Production_DTM_Construction;

    /* ========================================================================
     * INCLUDE REQUIRED MACROS
     * ======================================================================== */
    
    /*  %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_CHARG_DATARISK_ONEBI_BIFR.sas"; */
    %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_AZ_MACRO.sas";
    %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_AZEC_MACRO.sas";
    %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_CONSOLIDATION_MACRO.sas";

    /* ========================================================================
     * SELECT APPROPRIATE EXECUTION PROGRAM BASED ON VISION AGE
     * ======================================================================== */
    
    %if %eval(%sysfunc(year(%sysfunc(today()))) - %substr(&vision., 1, 4)) > 10 %then %do;
        /* Vision > 10 years old: Use historical reprises program */
        %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_RUN_REPR_HIST.sas";
    %end;
    %else %do;
        /* Recent vision: Use standard RUN program */
        %include "&rep_Pgm_Prod_DTM_Construction./PTF_MVTS_RUN.sas";
    %end;

%mend;


/* ============================================================================
 * HISTORICAL REPRISES EXECUTION
 * ============================================================================ */

/* --- 2008-2013: Annual visions --- */
%reprise_historique_ptf_mvts(vision=200812);
%reprise_historique_ptf_mvts(vision=200912);
%reprise_historique_ptf_mvts(vision=201012);
%reprise_historique_ptf_mvts(vision=201112);
%reprise_historique_ptf_mvts(vision=201212);
%reprise_historique_ptf_mvts(vision=201312);

/* --- 2014-2017: Nov-Dec visions --- */
%reprise_historique_ptf_mvts(vision=201411);
%reprise_historique_ptf_mvts(vision=201412);

%reprise_historique_ptf_mvts(vision=201511);
%reprise_historique_ptf_mvts(vision=201512);

%reprise_historique_ptf_mvts(vision=201611);
%reprise_historique_ptf_mvts(vision=201612);

%reprise_historique_ptf_mvts(vision=201711);
%reprise_historique_ptf_mvts(vision=201712);

/* --- 2018: Full year monthly visions --- */
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

/* --- 2019: Full year monthly visions --- */
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

/* --- 2020: Full year monthly visions --- */
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

/* --- 2021: Full year monthly visions --- */
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

/* --- 2022: Full year monthly visions --- */
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

/* --- 2023: Full year monthly visions --- */
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
