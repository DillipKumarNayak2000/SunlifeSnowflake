CREATE OR REPLACE VIEW CE_NOVA_DEV.DBO.V_RECON_PORTFOLIO_SUMMARY AS
WITH sap_by_field AS (
    SELECT
        ppr.portfolio_number,
        ppr.portfolio_description,
        rcr.major_product,
        sar.financial_field,
        SUM(rcr.transcurbal_ending_balance_period_ytd) AS field_total
    FROM idp_sapledger_prod.dbo.trial_balance rcr
    JOIN CE_NOVA_DEV.DBO.PAM_PORTFOLIO_REFERENCE ppr
        ON rcr.major_product = ppr.major_product
    JOIN CE_NOVA_DEV.DBO.SAP_ACCOUNTS_REFERENCE sar
        ON rcr.gl_account = sar.account_number
    WHERE rcr.period = CURRENT_DATE
      AND sar.financial_field IN (
          'Book Value',
          'Accrued Interest - Income',
          'Accrued Interest - Asset',
          'Unrealized Gain/Loss - Income',
          'Unrealized Gain/Loss - Asset'
      )
    GROUP BY
        ppr.portfolio_number,
        ppr.portfolio_description,
        rcr.major_product,
        sar.financial_field
),
sap_agg AS (
    SELECT
        portfolio_number,
        portfolio_description,
        major_product,
        SUM(ABS(field_total)) AS sap_total
    FROM sap_by_field
    GROUP BY
        portfolio_number,
        portfolio_description,
        major_product
),
pam_by_column AS (
    SELECT
        pam_source_system_portfolio,
        local_currency,
        'ifrs_net_book_value_local' AS column_name,
        SUM(COALESCE(ifrs_net_book_value_local, 0)) AS column_total
    FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist
    WHERE position_date = CURRENT_DATE
      AND most_recent_ind = 'Y'
    GROUP BY pam_source_system_portfolio, local_currency

    UNION ALL

    SELECT
        pam_source_system_portfolio,
        local_currency,
        'ACCRUED_INCOME_LOCAL' AS column_name,
        SUM(COALESCE(ACCRUED_INCOME_LOCAL, 0)) AS column_total
    FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist
    WHERE position_date = CURRENT_DATE
      AND most_recent_ind = 'Y'
    GROUP BY pam_source_system_portfolio, local_currency

    UNION ALL

    SELECT
        pam_source_system_portfolio,
        local_currency,
        'IFRS_UNREALIZED_GAIN_LOSS_LOCAL' AS column_name,
        SUM(COALESCE(IFRS_UNREALIZED_GAIN_LOSS_LOCAL, 0)) AS column_total
    FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist
    WHERE position_date = CURRENT_DATE
      AND most_recent_ind = 'Y'
    GROUP BY pam_source_system_portfolio, local_currency
),
pam_agg AS (
    SELECT
        pam_source_system_portfolio,
        local_currency,
        SUM(ABS(column_total)) AS pam_total
    FROM pam_by_column
    GROUP BY pam_source_system_portfolio, local_currency
)
SELECT
    s.portfolio_number AS "Portfolio Number",
    s.portfolio_description AS "Portfolio Description",
    s.major_product AS "Major Product",
    COALESCE(p.local_currency, '') AS "Currency",
    ROUND(COALESCE(s.sap_total, 0), 2) AS "SAP Total",
    ROUND(COALESCE(p.pam_total, 0), 2) AS "PAM Total",
    ROUND(
        ABS(COALESCE(s.sap_total, 0)) - COALESCE(p.pam_total, 0),
        2
    ) AS "Difference"
FROM sap_agg s
LEFT JOIN pam_agg p
    ON TRIM(s.portfolio_number) = TRIM(p.pam_source_system_portfolio)
ORDER BY s.portfolio_number;