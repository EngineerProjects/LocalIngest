/******************************************************************************
 * FILE: generiques_v4.sas
 * Description: Bibliothèque de macros génériques réutilisables v4
 * Version: 4.0 - 2025-12-01
 ******************************************************************************/


/* ============================================================================
 * MACRO: derniere_version
 * Paramètres: nom_lib, nom_table_req, vision_max, table_sortie
 * ============================================================================ */
%macro derniere_version(nom_lib, nom_table_req, vision_max, table_sortie);
    %local a;
    %let a = %nrstr(%mend);
    
    %let req = %sysfunc(quote(ls %sysfunc(pathname(&nom_lib.))%str(/)%str(*).sas7bdat));
    filename temp1 pipe &req.;
    
    DATA T1;
        infile temp1;
        length nom_table_add $200.;
        input nom_table_add;
    RUN;
    
    %let nom_table_req = %sysfunc(lowcase(&nom_table_req.));
    
    PROC SQL;
        CREATE TABLE T2 AS
        SELECT *,
               prxparse("/&nom_table_req.20[0-4][0-9](0[1-9]|1[0-2]).sas7bdat/") as pos_nom_table,
               ifc(prxmatch(calculated pos_nom_table, nom_table_add), prxposn(calculated pos_nom_table, 0, nom_table_add), " ") as nom_table,
               prxparse("/20[0-4][0-9](0[1-9]|1[0-2])/") as pos_vision,
               ifn(prxmatch(calculated pos_vision, nom_table_add), input(prxposn(calculated pos_vision, 0, nom_table_add), best.), 0) as vision
        FROM T1;
        
        CREATE TABLE T3 AS
        SELECT MAX(nom_table) AS nom_table_max
        FROM T2 t1
        WHERE NOT nom_table IS MISSING AND vision <= &vision_max.;
    QUIT;
    
    proc sql noprint;
        select nom_table_max into :nom_table from T3;
    quit;
    
    %let nom_table_sans_ext = %substr(&nom_table., 1, %eval(%length(&nom_table.) - 9));
    
    PROC SQL;
        CREATE TABLE &table_sortie. AS SELECT * FROM &nom_lib..&nom_table_sans_ext.;
    QUIT;
    
    %drop_table(T1 T2 T3);
%mend derniere_version;


/* ============================================================================
 * MACRO: gerer_erreur
 * Paramètres: message
 * ============================================================================ */
%macro gerer_erreur(message);
    %put ERROR: &message.;
    %abort;
%mend;


/* ============================================================================
 * MACRO: lister_champs_table
 * Paramètres: table_source, dlm (défaut: espace)
 * ============================================================================ */
%macro lister_champs_table(table_source, dlm=%str( ));
    %local outvar dsid nvars x rc dlm;
    
    %let dsid=%sysfunc(open(&table_source));
    
    %if &dsid %then %do;
        %let nvars=%sysfunc(attrn(&dsid,NVARS));
        
        %if &nvars>0 %then %do;
            %let outvar=%sysfunc(varname(&dsid,1));
            %do x=2 %to &nvars;
                %let outvar=&outvar.&dlm%sysfunc(varname(&dsid,&x));
            %end;
        %end;
        
        %let rc=%sysfunc(close(&dsid));
    %end;
    %else %do;
        %gerer_erreur(Impossible d_ouvrir la table &table_source.)
    %end;
    
    &outvar.
%mend;


/* ============================================================================
 * MACRO: in_array
 * Paramètres: champs_recherche, liste_champs
 * ============================================================================ */
%macro in_array(champs_recherche, liste_champs);
    %local a;
    %let a = %nrstr(%mend);
    %let trouve_in_array = 0;
    
    %do i_in_array = 1 %to %sysfunc(countw(&liste_champs., %str( )));
        %if %upcase(&champs_recherche.) = %upcase(%scan(&liste_champs.,&i_in_array, %str( ))) %then %do;
            %let trouve_in_array = 1;
        %end;
    %end;
    
    &trouve_in_array.
%mend;


/* ============================================================================
 * MACRO: creer_liste_avec_separateur
 * Paramètres: liste_in, separateur
 * ============================================================================ */
%macro creer_liste_avec_separateur(liste_in, separateur);
    %local a;
    %let a = %nrstr(%mend);
    %let nb_items = %sysfunc(countw(&liste_in., %str( )));
    
    %if &nb_items. > 0 %then %do;
        %let liste_out = %scan(&liste_in.,1, %str( ));
        %if &nb_items. > 1 %then %do;
            %do i_liste = 2 %to &nb_items.;
                %let liste_out = &liste_out.&separateur.%scan(&liste_in.,&i_liste., %str( ));
            %end;
        %end;
    %end;
    
    &liste_out.
%mend;


/* ============================================================================
 * MACRO: drop_table
 * Paramètres: liste_tables_source
 * ============================================================================ */
%macro drop_table(liste_tables_source);
    %local a;
    %let a = %nrstr(%mend);
    
    %do i_dr = 1 %to %sysfunc(countw(&liste_tables_source., %str( )));
        %let table_courante = %scan(&liste_tables_source.,&i_dr, %str( ));
        %if (%sysfunc(exist(&table_courante.)) = 1) %then %do;
            proc sql;
                DROP TABLE &table_courante.;
            quit;
        %end;
    %end;
%mend;


/* ============================================================================
 * MACRO: verifier_cle_unique
 * Paramètres: table_source_verif, champs_cles_verif
 * ============================================================================ */
%macro verifier_cle_unique(table_source_verif, champs_cles_verif);
    %local a;
    %let a = %nrstr(%mend);
    %let champs_cles_verif = %creer_liste_avec_separateur(&champs_cles_verif., %str(, ));
    
    PROC SQL noprint;
        SELECT COUNT(*) into: nobs1 FROM &table_source_verif.;
        SELECT COUNT(*) into: nobs2 FROM (SELECT DISTINCT &champs_cles_verif. FROM &table_source_verif. t1);
    QUIT;
    
    %if &nobs1. ^= &nobs2. %then %do;
        %gerer_erreur(La clé &champs_cles_verif. proposée pour la table &table_source_verif. n_est pas unique);
    %end;
    %else %do;
        %put NOTE: La clé &champs_cles_verif. proposée pour la table &table_source_verif. est bien unique;
    %end;
%mend;


/* ============================================================================
 * MACRO: full_outer_join_sur_cles
 * Paramètres: liste_tables_source, liste_champs_cles, table_sortie
 * ============================================================================ */
%macro full_outer_join_sur_cles(liste_tables_source, liste_champs_cles, table_sortie);
    %local a;
    %let a = %nrstr(%mend);
    %let liste_champs_cles_sep = %creer_liste_avec_separateur(&liste_champs_cles.,%str(, ));
    %let continue = 1;
    
    %do i_foj = 1 %to %sysfunc(countw(&liste_tables_source., %str( )));
        %let table_courante = %scan(&liste_tables_source.,&i_foj, %str( ));
        
        %if (%sysfunc(exist(&table_courante.)) = 1) %then %do;
            %let champs_table = %lister_champs_table(&table_courante.);
            
            %do j_foj = 1 %to %sysfunc(countw(&liste_champs_cles., %str( )));
                %if %in_array(%scan(&liste_champs_cles.,&j_foj., %str( )), &champs_table.) = 0 %then %do;
                    %gerer_erreur(le champs %scan(&liste_champs_cles.,&j_foj., %str( )) n_a pas été trouvé dans la table %scan(&liste_tables_source.,&i_foj., %str( )));
                    %let continue = 0;
                %end;
            %end;
            
            %verifier_cle_unique(%scan(&liste_tables_source.,&i_foj., %str( )), &liste_champs_cles.);
            
            %if &i_foj. = 1 %then %do;
                PROC SQL;
                    CREATE TABLE &table_sortie. AS SELECT &liste_champs_cles_sep. FROM &table_courante.;
                QUIT;
            %end;
            %else %do;
                PROC SQL;
                    CREATE TABLE &table_sortie. AS
                    SELECT &liste_champs_cles_sep. FROM &table_sortie.
                    UNION CORR SELECT * FROM &table_courante.;
                    
                    CREATE TABLE &table_sortie. AS SELECT DISTINCT * FROM &table_sortie.;
                QUIT;
            %end;
        %end;
        %else %do;
            %gerer_erreur(La table &table_courante. na pas été trouvée);
            %let continue = 0;
        %end;
    %end;
    
    %if &continue. = 1 %then %do;
        %let req_on =;
        %let req_cond =;
        
        %do j_foj = 1 %to %sysfunc(countw(&liste_champs_cles., %str( )));
            %let champ_cle_courant = %scan(&liste_champs_cles.,&j_foj., %str( ));
            %let req_cond = (t1.&champ_cle_courant.=t2.&champ_cle_courant.);
            %let req_on = &req_on. &req_cond.;
        %end;
        
        %let req_on = %creer_liste_avec_separateur(%quote(&req_on.), %str( AND ));
        
        %do i_foj = 1 %to %sysfunc(countw(&liste_tables_source., %str( )));
            %let table_courante = %scan(&liste_tables_source.,&i_foj, %str( ));
            %let champs_table = %lister_champs_table(&table_courante.);
            %let req_champs_a_joindre = %str(t1.*);
            
            %do k_foj = 1 %to %sysfunc(countw(&champs_table., %str( )));
                %let champ_courant = %scan(&champs_table.,&k_foj., %str( ));
                
                %if %in_array(&champ_courant., &liste_champs_cles.) = 0 %then %do;
                    %let trouve_champ_foj = 0;
                    
                    %do l_foj = 1 %to %sysfunc(countw(&liste_tables_source., %str( )));
                        %let autre_table = %scan(&liste_tables_source.,&l_foj, %str( ));
                        %let champs_autre_table = %lister_champs_table(&autre_table.);
                        
                        %if (&autre_table. ^= &table_courante.) & %in_array(&champ_courant., &champs_autre_table.) = 1 %then %do;
                            %let trouve_champ_foj = 1;
                        %end;
                    %end;
                    
                    %let req_suf=;
                    %if &trouve_champ_foj. = 1 %then %do;
                        %let req_suf=_&table_courante.;
                    %end;
                    
                    %let req_champs_a_joindre = &req_champs_a_joindre.,%str( )t2.&champ_courant. AS &champ_courant.&req_suf.;
                %end;
            %end;
            
            PROC SQL;
                CREATE TABLE &table_sortie. AS
                SELECT t1.*, &req_champs_a_joindre.
                FROM &table_sortie. t1
                LEFT OUTER JOIN &table_courante. t2 ON &req_on.;
            QUIT;
        %end;
    %end;
%mend;


/* ============================================================================
 * MACRO: derniere_vision
 * Paramètres: table_source, champ_vision, liste_champs_cle_output, 
 *             liste_champs_par_vision, table_sortie
 * ============================================================================ */
%macro derniere_vision(table_source, champ_vision, liste_champs_cle_output, liste_champs_par_vision, table_sortie);
    %local a;
    %let a = %nrstr(%mend);
    
    %if (%sysfunc(exist(&table_source.)) ne 1) %then %do;
        %gerer_erreur(La table &table_source. na pas été trouvée);
    %end;
    %else %do;
        %let champs_table = %lister_champs_table(&table_source.);
        %let continue = 1;
        
        %do j_dv = 1 %to %sysfunc(countw(&champ_vision. &liste_champs_cle_output. &liste_champs_par_vision., %str( )));
            %let champ_courant = %scan(&champ_vision. &liste_champs_cle_output. &liste_champs_par_vision.,&j_dv, %str( ));
            %if %in_array(&champ_courant., &champs_table.) = 0 %then %do;
                %gerer_erreur(le champs &champ_courant. n_a pas été trouvé dans la table &table_source.);
                %let continue = 0;
            %end;
        %end;
        
        %if &continue. = 1 %then %do;
            PROC SQL;
                CREATE TABLE TEMP as
                select distinct %creer_liste_avec_separateur(&liste_champs_cle_output.,%str(, ))
                from &table_source.;
            QUIT;
            
            DATA TEMP1;
                set TEMP;
                Index =_N_;
            run;
            
            PROC SORT data= TEMP1; by &liste_champs_cle_output.; run;
            PROC SORT data= &table_source.; by &liste_champs_cle_output.; run;
            
            data &table_source.;
                merge &table_source. TEMP1;
                by &liste_champs_cle_output.;
            run;
            
            %drop_table(TEMP1 TEMP);
            %verifier_cle_unique(&table_source., &champ_vision. index);
            
            %let liste_champs_derniere_vision=;
            %let champs_table = %lister_champs_table(&table_source.);
            
            %do j_dv = 1 %to %sysfunc(countw(&champs_table., %str( )));
                %if %in_array(%scan(&champs_table.,&j_dv., %str( )), &champ_vision. &liste_champs_par_vision.) = 0 %then %do;
                    %let liste_champs_derniere_vision=&liste_champs_derniere_vision.%str( )%scan(&champs_table.,&j_dv., %str( ));
                %end;
            %end;
            
            PROC SQL;
                CREATE TABLE DERNIERE_VISION AS
                SELECT MAX(&champ_vision.) AS DERN_EXERCICE, index
                FROM &table_source. GROUP BY index;
                
                CREATE TABLE SUB_DERNIERE_VISION AS
                SELECT %creer_liste_avec_separateur(&champ_vision. index &liste_champs_derniere_vision.,%str(, ))
                FROM &table_source.;
            QUIT;
            
            PROC SQL;
                CREATE TABLE VAR_DERNIERE_VISION AS
                SELECT t2.*
                FROM DERNIERE_VISION t1
                LEFT JOIN SUB_DERNIERE_VISION t2
                ON t1.DERN_EXERCICE = t2.&champ_vision. and (t1.index = t2.index);
            QUIT;
            
            %drop_table(DERNIERE_VISION SUB_DERNIERE_VISION);
            
            PROC SQL NOPRINT;
                SELECT DISTINCT &champ_vision. INTO :liste_visions separated by ' ' FROM &table_source.;
            QUIT;
            
            %let liste_tables_dvt=;
            
            %do j_dv = 1 %to %sysfunc(countw(&liste_champs_par_vision., %str( )));
                %let liste_tables_dv =;
                
                %do k_dv = 1 %to %sysfunc(countw(&liste_visions., %str( )));
                    %let champ_par_vision = %scan(&liste_champs_par_vision., &j_dv., %str( ));
                    %let vision = %scan(&liste_visions., &k_dv., %str( ));
                    
                    PROC SQL;
                        CREATE TABLE TABLE_&champ_par_vision._&vision. AS
                        SELECT index, &champ_par_vision. AS &champ_par_vision._&vision.
                        FROM &table_source. WHERE &champ_vision. = &vision.;
                    QUIT;
                    
                    %let liste_tables_dv = &liste_tables_dv.%str( )TABLE_&champ_par_vision._&vision.;
                %end;
                
                %full_outer_join_sur_cles(&liste_tables_dv., index, TABLE_&champ_par_vision.);
                %drop_table(&liste_tables_dv.);
                %let liste_tables_dvt=&liste_tables_dvt.%str( )TABLE_&champ_par_vision.;
            %end;
            
            %full_outer_join_sur_cles(&liste_tables_dvt., index , TABLE_TOUS_CHAMPS_PAR_VISION);
            %drop_table(&liste_tables_dvt.);
            
            PROC SQL;
                CREATE TABLE &table_sortie. AS
                SELECT * FROM VAR_DERNIERE_VISION t1
                left join TABLE_TOUS_CHAMPS_PAR_VISION t2 on (t1.index = t2.index);
                
                CREATE TABLE &table_sortie. AS
                SELECT * FROM &table_sortie. ( drop = index);
            QUIT;
            
            %drop_table(VAR_DERNIERE_VISION TABLE_TOUS_CHAMPS_PAR_VISION);
        %end;
    %end;
%mend derniere_vision;


/* ============================================================================
 * FIN DU FICHIER - Messages de chargement
 * ============================================================================ */
%put NOTE: ============================================================;
%put NOTE: generiques_v4.sas chargé avec succès;
%put NOTE: 9 macros disponibles: derniere_version, derniere_vision,;
%put NOTE: gerer_erreur, lister_champs_table, in_array,;
%put NOTE: creer_liste_avec_separateur, verifier_cle_unique,;
%put NOTE: drop_table, full_outer_join_sur_cles;
%put NOTE: ============================================================;
