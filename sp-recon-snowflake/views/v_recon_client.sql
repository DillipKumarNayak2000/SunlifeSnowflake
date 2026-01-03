CREATE OR REPLACE VIEW CE_NOVA_DEV.DBO.V_RECON_CLIENT AS
SELECT
    rcr.gl_account AS "Account Number",
    sar.account_description AS "Account Description",
    rcr.transcurbal_ending_balance_period_ytd AS "Value",
    ppr.portfolio_number AS "Portfolio Number",
    ppr.portfolio_description AS "Portfolio Description"
FROM idp_sapledger_prod.dbo.trial_balance rcr
JOIN CE_NOVA_DEV.DBO.SAP_ACCOUNTS_REFERENCE sar
    ON rcr.gl_account = sar.account_number
JOIN CE_NOVA_DEV.DBO.PAM_PORTFOLIO_REFERENCE ppr
    ON rcr.major_product = ppr.major_product
WHERE rcr.period = CURRENT_DATE();