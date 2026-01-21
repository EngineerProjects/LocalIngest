/******************************************************************************
 * MACRO: indexation_v2
 * 
 * Description: Macro for capital indexation based on construction indices
 *              Adjusts capital amounts from their origin date to a target date
 *              using construction cost indices from the INDICES library
 *
 * Parameters:
 *   - DATE   : Target indexation date (default: DTECHANN)
 *   - IND    : Capital field number (1-14) to index
 *   - NOMMT  : Base name for capital amount field (default: MTCAPI)
 *   - NOMNAT : Base name for construction nature code (default: CDPRVB)
 *   - NOMIND : Base name for evolution coefficient (default: PRPRVC)
 *
 * Input:
 *   - MTCAPI&IND.  : Capital amount to index
 *   - CDPRVB&IND.  : Construction nature code for index selection
 *   - DTEFSITT     : Contract start date (origin date)
 *   - DTECHANN     : Contract termination date
 *
 * Output:
 *   - MTCAPI&IND.i     : Indexed capital amount
 *   - INDXINTG&IND.i   : Target date index value
 *   - INDXORIG&IND.i   : Origin date index value
 *
 * Logic:
 *   - If target date < origin date: no indexation (ratio = 1)
 *   - Otherwise: indexed amount = original amount * (target index / origin index)
 ******************************************************************************/

/* Macro d'indexation des capitaux */
%MACRO indexation_v2(DATE = ., IND = 1, NOMMT = MTCAPI, NOMNAT = CDPRVB, NOMIND = PRPRVC);

    OPTIONS FMTSEARCH=(INDICES);
    FORMAT VAL1 VAL2 $8.;
    FORMAT DATE DDMMYY10.;

    /* ========================================================================
     * DETERMINE ORIGIN DATE AND INDEX
     * ======================================================================== */
    
    %IF &DATE EQ . %THEN %DO;
        /* Use contract termination date (DTECHANN) for indexation */
        JOUR1 = SUBSTR(PUT(DTECHANN, 4.), 3, 2);
        MOIS1 = SUBSTR(PUT(DTECHANN, 4.), 1, 2);
        ANNEE1 = YEAR(DATE());
        DATE = MDY(MOIS1, JOUR1, ANNEE1);
        
        IF DATE > DATE() THEN DO;
            ANNEE1 = ANNEE1 - 1;
            DATE = MDY(MOIS1, JOUR1, ANNEE1);
        END;
        
        INDXORIG = &NOMIND&IND.;
    %END;
    %ELSE %DO;
        /* Use specified target date for indexation */
        JOUR1 = DAY(&DATE);
        MOIS1 = MONTH(&DATE);
        ANNEE1 = YEAR(&DATE);
        DATE1 = MDY(MOIS1, JOUR1, ANNEE1);
        
        /* Determine origin date from contract start */
        JOUR2 = SUBSTR(PUT(DTECHANN, 4.), 3, 2);
        MOIS2 = SUBSTR(PUT(DTECHANN, 4.), 1, 2);
        DATE2 = MDY(MOIS2, JOUR2, ANNEE1);
        
        IF DATE1 < DATE2 THEN DO;
            ANNEE1 = ANNEE1 - 1;
            DATE = MDY(MOIS2, JOUR2, ANNEE1);
        END;
        ELSE DATE = DATE2;
        
        /* Get origin index from construction nature code + contract start date */
        VAL1 = &NOMNAT&IND. !! PUT(DTEFSITT, Z5.);
        IF SUBSTR(VAL1, 1, 1) = '0' THEN INDXORIG = PUT(VAL1, $INDICE.);
        ELSE INDXORIG = 1;
    %END;

    /* ========================================================================
     * DETERMINE TARGET DATE INDEX
     * ======================================================================== */
    
    VAL2 = &NOMNAT&IND. !! PUT(DATE, Z5.);
    IF SUBSTR(VAL2, 1, 1) = '0' THEN INDXINTG = PUT(VAL2, $INDICE.);
    ELSE INDXINTG = 1;

    /* ========================================================================
     * APPLY INDEXATION LOGIC
     * ======================================================================== */
    
    /* No indexation if target date < origin date */
    IF DATE < DTEFSITT THEN DO;
        INDXINTG = 1;
        INDXORIG = 1;
    END;

    /* ========================================================================
     * CALCULATE INDEXED CAPITAL AMOUNT
     * ======================================================================== */
    
    FORMAT &NOMMT&IND.i 16.;
    &NOMMT&IND.i = ifn(INDXINTG / INDXORIG = ., &NOMMT&IND., &NOMMT&IND. * (INDXINTG / INDXORIG));
    INDXINTG&IND.i = INDXINTG;
    INDXORIG&IND.i = INDXORIG;

    /* DROP DATE: JOUR: MOIS1 MOIS2 ANNEE: VAL1 VAL2 /*INDXORIG INDXINTG*/
%MEND;
