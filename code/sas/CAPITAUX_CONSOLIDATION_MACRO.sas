/******************************************************************************
 * MACRO: consolidation_az_azec_capitaux
 * 
 * Description: Consolidates capital data (CAPITAUX) from AZ and AZEC sources
 *              into a single table with proper formatting and field mapping
 *
 * Parameters:
 *   - &vision. : Vision parameter (macro variable)
 *
 * Output: 
 *   - CUBE.AZ_AZEC_CAPITAUX_&vision. table
 ******************************************************************************/

%macro consolidation_az_azec_capitaux;

    PROC SQL;
        CREATE TABLE CUBE.AZ_AZEC_CAPITAUX_&vision. AS 
        (
            /* AZ Data Selection */
            SELECT 
                "AZ" AS DIRCOM,
                NOPOL,
                CDPOLE,
                CDPROD,
                CMARCH,
                CSEG,
                CSSSEG,
                (PERTE_EXP_100_IND + RISQUE_DIRECT_100_IND) AS VALUE_INSURED_100_IND,
                PERTE_EXP_100_IND,
                RISQUE_DIRECT_100_IND,
                LIMITE_RC_100_PAR_SIN,
                LIMITE_RC_100_PAR_AN,
                LIMITE_RC_100,
                SMP_100_IND,
                LCI_100_IND,
                (PERTE_EXP_100 + RISQUE_DIRECT_100) AS VALUE_INSURED_100,
                PERTE_EXP_100,
                RISQUE_DIRECT_100,
                SMP_100,
                LCI_100
            FROM AZ_CAPITAUX_&vision.
        )
        OUTER UNION CORR
        (
            /* AZEC Data Selection */
            SELECT 
                "AZEC" AS DIRCOM,
                NOPOL,
                "3" AS CDPOLE,
                CDPROD,
                CMARCH,
                CSEG,
                CSSSEG,
                VALUE_INSURED_100_IND,
                PERTE_EXP_100_IND,
                RISQUE_DIRECT_100_IND,
                . AS LIMITE_RC_100_PAR_SIN,
                . AS LIMITE_RC_100_PAR_AN,
                . AS LIMITE_RC_100,
                SMP_100_IND,
                LCI_100_IND,
                . AS VALUE_INSURED_100,
                . AS PERTE_EXP_100,
                . AS RISQUE_DIRECT_100,
                . AS SMP_100,
                . AS LCI_100
            FROM AZEC_CAPITAUX_&vision.
        );
    QUIT;

%mend;
