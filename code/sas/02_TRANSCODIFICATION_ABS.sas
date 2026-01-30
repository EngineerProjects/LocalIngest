/******************************************************************************
 * MACRO: transco_tab
 * 
 * Description: Handles character encoding conversion from UTF-8 to LATIN9
 *              Attempts direct UTF-8 transcoding first, and if that fails due
 *              to character loss, forces LATIN9 reading with manual character
 *              replacement for accented characters (é, à, è, ô)
 *
 * Parameters:
 *   - in  : Input table name (temporary table downloaded in UTF8)
 *   - out : Output table name (final table in LATIN9)
 *
 * Method:
 *   A. First attempt: Direct UTF-8 transcoding
 *   B. Fallback method: Force LATIN9 reading with character replacement
 *   C. Cleanup: Remove intermediate tables
 *
 * Note: Macro constructed in collaboration with Ramdane AKSIL (DOSI)
 ******************************************************************************/

/******************** ABS *****************************/
/* 1ère étape passage de la base de utf8 à latin 9 */
/* REVALO */
/*libname base_ABS "/net/home/&userid./METGDCPS/utilisateurs/agnes";*/


%macro transco_tab(in, out);

    /* ========================================================================
     * STEP 1: SETUP LOG FILE FOR ERROR DETECTION
     * ======================================================================== */
    
    /* A : On essaye de lire la table directement en UTF */
    /* a) On copie la log dans la work pour vérifier les erreurs */
    
    %let work_path = %sysfunc(pathname(work));
    filename myfile "&work_path./log_file.log";
    
    proc printto log=myfile NEW;
    run;

    /* ========================================================================
     * STEP 2: ATTEMPT DIRECT UTF-8 TRANSCODING
     * ======================================================================== */
    
    /* b) On transcode la table directement */
    
    data &out.;
        set &in. (encoding = 'utf-8');
    run;
    
    proc printto;
    run;

    /* ========================================================================
     * STEP 3: CHECK FOR TRANSCODING ERRORS
     * ======================================================================== */
    
    /* b) On check si une erreur de transcodification a été détectée */
    
    data _T_my_log;
        infile "&work_path./log_file.log" truncover;
        input a_line $200.;
        if index(a_line, "ERROR: Some character data was lost during transcoding in the dataset") > 0;
    run;
    
    proc sql noprint;
        select count(*) into :error
        from _T_my_log;
    quit;
    
    %put &error.;

    /* ========================================================================
     * STEP 4: FALLBACK - FORCE LATIN9 READING WITH CHARACTER REPLACEMENT
     * ======================================================================== */
    
    %if &error. > 0 %then %do;
        
        /* ----------------------------------------------------------------
         * 4a: Select text variables > 2 characters
         * ---------------------------------------------------------------- */
        
        /* Etape 1 : On se concentre sur les variables alphanumériques 
           supérieurs à 2 caractères */
        
        Proc contents data = &in. noprint 
            out = _T_tab_var (keep=name format length rename=(name=nom_var));
        run;
        
        DATA _T_tab_var;
            set _T_tab_var;
            if format = "$";
            if length > 2;
            drop format length;
        run;

        /* ----------------------------------------------------------------
         * 4b: Transcode these variables to avoid reading errors
         * ---------------------------------------------------------------- */
        
        data _T_temp;
            set &in. (encoding = 'latin9');
        run;
        
        Data _null_;
            set work._T_tab_var end=eof;
            call symputx(cats('var_input', put(_n_, 8. -L)), nom_var, G);
            if eof = 1 then call symputx("nobs", put(_n_, 8. -L), G);
        Run;
        
        %put |----------------------------|;
        %put Nb variables à traiter=&nobs;
        %put |----------------------------|;
        
        DATA &out.;
            set _T_temp;
            
            %do i = 1 %to &nobs;
                nom_var&i. = /*lowcase()*/ &&var_input&i.;
                nom_var&i. = tranwrd(nom_var&i., 'é', '�');
                nom_var&i. = tranwrd(nom_var&i., 'à', '�');
                nom_var&i. = tranwrd(nom_var&i., 'è', '�');
                nom_var&i. = tranwrd(nom_var&i., 'ô', '�');
                &&var_input&i. = nom_var&i.;
                drop nom_var&i;
            %end;
        RUN;
        
    %end;

    /* ========================================================================
     * STEP 5: CLEANUP INTERMEDIATE TABLES (OPTIONAL - CURRENTLY COMMENTED)
     * ======================================================================== */
    
    /*
    %let tab = %scan(&in., 2, ".");
    
    proc datasets lib = base_abs nolist;
        delete &tab.;
    quit;
    run;
    
    proc datasets lib = work nolist;
        delete _T_:;
    quit;
    run;
    */

%mend;
