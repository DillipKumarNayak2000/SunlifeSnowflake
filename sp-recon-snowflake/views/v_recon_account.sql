CREATE OR REPLACE VIEW CE_NOVA_DEV.DBO.V_RECON_ACCOUNT AS
SELECT
    rcr.gl_account AS "Account Number",
    sar.account_description AS "Account Description",
    rcr.transcurbal_ending_balance_period_ytd AS "Value",
    rcr.period AS "Period",
    rcr.runid AS "Run ID"
FROM idp_sapledger_prod.dbo.trial_balance rcr
JOIN CE_NOVA_DEV.DBO.SAP_ACCOUNTS_REFERENCE sar
    ON rcr.gl_account = sar.account_number
WHERE rcr.runid = (SELECT MAX(runid) FROM idp_sapledger_prod.dbo.trial_balance WHERE period = rcr.period)
AND sar.financial_field IS NOT NULL;