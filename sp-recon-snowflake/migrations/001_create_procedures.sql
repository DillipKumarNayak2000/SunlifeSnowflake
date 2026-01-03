CREATE OR REPLACE PROCEDURE CE_NOVA_DEV.DBO.SP_RECON_CLIENT_VIEW(
    "PORTFOLIO_NUMBER" VARCHAR(60),
    "P_INPUT_DATE" VARCHAR(60),
    "FINANCIAL_FIELD" VARCHAR(120)
)
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, DecimalType
from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, sum as sum_, abs as abs_, round as round_, lit
import re
import calendar
from datetime import datetime

def main(session: snowpark.Session, portfolio_no, p_input_date, financial_field):

    def create_failure_df(reason: str):
        schema = StructType([
            StructField("status", StringType()),
            StructField("reason", StringType())
        ])
        data = [("FAILED", reason)]
        return session.create_dataframe(data, schema=schema)

    def validate_and_adjust_date(p_input_date):
        MAX_DATE = datetime.today().date()

        if not p_input_date:
            return False, None, create_failure_df("Input date is NULL or empty")

        if not re.match(r"^\d{4}-\d{2}-\d{2}$", p_input_date):
            return False, None, create_failure_df("Invalid date format, expected YYYY-MM-DD")

        try:
            parsed_date = datetime.strptime(p_input_date, "%Y-%m-%d").date()
            corrected_date_str = p_input_date
        except ValueError:
            try:
                year, month, day = map(int, p_input_date.split("-"))
                if month < 1 or month > 12:
                    return False, None, create_failure_df("Invalid month value")
                last_day = calendar.monthrange(year, month)[1]
                parsed_date = datetime(year, month, min(day, last_day)).date()
                corrected_date_str = parsed_date.strftime("%Y-%m-%d")
            except Exception as e:
                return False, None, create_failure_df(f"Invalid date: {str(e)}")

        if parsed_date.day >= 28:
            last_day = calendar.monthrange(parsed_date.year, parsed_date.month)[1]
            parsed_date = parsed_date.replace(day=last_day)
            corrected_date_str = parsed_date.strftime("%Y-%m-%d")

        if parsed_date > MAX_DATE:
            return False, None, create_failure_df(
                f"Date {corrected_date_str} is later than allowed maximum (MAX_DATE)"
            )

        return True, corrected_date_str, parsed_date

    is_valid, corrected_date_str, date_or_df = validate_and_adjust_date(p_input_date)

    if not is_valid:
        return date_or_df

    p_input_date = corrected_date_str

    ff = (financial_field or "Book Value")

    max_runid_query = f"""
        SELECT MAX(runid) as max_runid
        FROM idp_sapledger_prod.dbo.trial_balance
        WHERE period = '{p_input_date}'
    """

    max_runid_result = session.sql(max_runid_query).collect()
    max_runid = max_runid_result[0]["MAX_RUNID"] if max_runid_result else None

    if max_runid is None:
        return create_failure_df(f"No runid found for period {p_input_date}")

    session.sql("""
        CREATE TEMP TABLE IF NOT EXISTS temp_portfolio_ref AS
        SELECT portfolio_number, major_product, portfolio_description
        FROM CE_NOVA_DEV.DBO.PAM_PORTFOLIO_REFERENCE
    """).collect()

    if portfolio_no is not None and financial_field is not None:
        account_query = f"""
            SELECT
                rcr.gl_account as "Account Number",
                sar.account_description as "Account Description",
                rcr.transcurbal_ending_balance_period_ytd as "Value"
            FROM idp_sapledger_prod.dbo.trial_balance rcr
            JOIN temp_portfolio_ref ppr
                ON rcr.major_product = ppr.major_product
            JOIN CE_NOVA_DEV.DBO.SAP_ACCOUNTS_REFERENCE sar
                ON rcr.gl_account = sar.account_number
            WHERE rcr.period = '{p_input_date}'
              AND ppr.portfolio_number = '{portfolio_no}'
              AND sar.financial_field = '{financial_field}'
              AND rcr.runid = {max_runid}
        """
        return session.sql(account_query)

    elif portfolio_no is not None and financial_field is None:

        pam_combined_query = f"""
            SELECT
                'book value' as financial_field,
                SUM(frs.net_book_value_local) AS pam_total
            FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist frs
            WHERE position_date = '{p_input_date}'
              AND most_recent_ind = 'Y'
              AND pam_source_system_portfolio = '{portfolio_no}'
              AND pam_source_system_portfolio IN (
                  SELECT portfolio_number FROM temp_portfolio_ref
              )

            UNION ALL

            SELECT
                'accrued interest - income' as financial_field,
                SUM(ACCRUED_INCOME_LOCAL) AS pam_total
            FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist
            WHERE position_date = '{p_input_date}'
              AND most_recent_ind = 'Y'
              AND pam_source_system_portfolio = '{portfolio_no}'
              AND pam_source_system_portfolio IN (
                  SELECT portfolio_number FROM temp_portfolio_ref
              )

            UNION ALL

            SELECT
                'accrued interest - asset' as financial_field,
                SUM(ACCRUED_INCOME_LOCAL) AS pam_total
            FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist
            WHERE position_date = '{p_input_date}'
              AND most_recent_ind = 'Y'
              AND pam_source_system_portfolio = '{portfolio_no}'
              AND pam_source_system_portfolio IN (
                  SELECT portfolio_number FROM temp_portfolio_ref
              )

            UNION ALL

            SELECT
                'unrealized gain/loss - income' as financial_field,
                SUM(IFRS_UNREALIZED_GAIN_LOSS_LOCAL) AS pam_total
            FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist
            WHERE position_date = '{p_input_date}'
              AND most_recent_ind = 'Y'
              AND pam_source_system_portfolio = '{portfolio_no}'
              AND pam_source_system_portfolio IN (
                  SELECT portfolio_number FROM temp_portfolio_ref
              )

            UNION ALL

            SELECT
                'unrealized gain/loss - asset' as financial_field,
                SUM(IFRS_UNREALIZED_GAIN_LOSS_LOCAL) AS pam_total
            FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist
            WHERE position_date = '{p_input_date}'
              AND most_recent_ind = 'Y'
              AND pam_source_system_portfolio = '{portfolio_no}'
              AND pam_source_system_portfolio IN (
                  SELECT portfolio_number FROM temp_portfolio_ref
              )
        """

        combined_summary_query = f"""
            WITH pam_data AS (
                {pam_combined_query}
            ),
            sap_data AS (
                SELECT
                    sar.financial_field,
                    SUM(rcr.transcurbal_ending_balance_period_ytd) AS sap_total
                FROM idp_sapledger_prod.dbo.trial_balance rcr
                JOIN CE_NOVA_DEV.DBO.SAP_ACCOUNTS_REFERENCE sar
                    ON rcr.gl_account = sar.account_number
                JOIN temp_portfolio_ref ppr
                    ON rcr.major_product = ppr.major_product
                WHERE rcr.period = '{p_input_date}'
                  AND ppr.portfolio_number = '{portfolio_no}'
                  AND rcr.runid = {max_runid}
                  AND sar.financial_field NOT IN ('Interest Income', 'Realized Gain/Loss', 'Cash')
                GROUP BY sar.financial_field
            )
            SELECT
                COALESCE(s.financial_field, p.financial_field) AS "Financial Field",
                ROUND(COALESCE(s.sap_total, 0), 2) AS "SAP Total",
                ROUND(COALESCE(p.pam_total, 0), 2) AS "PAM Total",
                ROUND(
                    ABS(COALESCE(s.sap_total, 0)) - COALESCE(ABS(p.pam_total), 0),
                    2
                ) AS "Difference"
            FROM sap_data s
            FULL OUTER JOIN pam_data p
                ON LOWER(s.financial_field) = LOWER(p.financial_field)
            WHERE COALESCE(s.financial_field, p.financial_field) IS NOT NULL
        """

        return session.sql(combined_summary_query)

    else:

        all_portfolio_summary_query = f"""
            WITH sap_by_field AS (
                SELECT
                    ppr.portfolio_number,
                    ppr.portfolio_description,
                    rcr.major_product,
                    sar.financial_field,
                    SUM(rcr.transcurbal_ending_balance_period_ytd) AS field_total
                FROM idp_sapledger_prod.dbo.trial_balance rcr
                JOIN temp_portfolio_ref ppr
                    ON rcr.major_product = ppr.major_product
                JOIN CE_NOVA_DEV.DBO.SAP_ACCOUNTS_REFERENCE sar
                    ON rcr.gl_account = sar.account_number
                WHERE rcr.period = '{p_input_date}'
                  AND rcr.runid = {max_runid}
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
                WHERE position_date = '{p_input_date}'
                  AND most_recent_ind = 'Y'
                  AND pam_source_system_portfolio IN (
                      SELECT portfolio_number FROM temp_portfolio_ref
                  )
                GROUP BY pam_source_system_portfolio, local_currency

                UNION ALL

                SELECT
                    pam_source_system_portfolio,
                    local_currency,
                    'ACCRUED_INCOME_LOCAL' AS column_name,
                    SUM(COALESCE(ACCRUED_INCOME_LOCAL, 0)) AS column_total
                FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist
                WHERE position_date = '{p_input_date}'
                  AND most_recent_ind = 'Y'
                  AND pam_source_system_portfolio IN (
                      SELECT portfolio_number FROM temp_portfolio_ref
                  )
                GROUP BY pam_source_system_portfolio, local_currency

                UNION ALL

                SELECT
                    pam_source_system_portfolio,
                    local_currency,
                    'IFRS_UNREALIZED_GAIN_LOSS_LOCAL' AS column_name,
                    SUM(COALESCE(IFRS_UNREALIZED_GAIN_LOSS_LOCAL, 0)) AS column_total
                FROM idp_holdings_prod.ga_tdp_pam_lot_positions_mth_hist
                WHERE position_date = '{p_input_date}'
                  AND most_recent_ind = 'Y'
                  AND pam_source_system_portfolio IN (
                      SELECT portfolio_number FROM temp_portfolio_ref
                  )
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
            ORDER BY s.portfolio_number
        """

        return session.sql(all_portfolio_summary_query)
$$;

CREATE OR REPLACE PROCEDURE CE_NOVA_DEV.DBO.SP_RECON_ACCOUNT_VIEW(
    "ACCOUNT_NUMBER" VARCHAR(60),
    "P_INPUT_DATE" VARCHAR(60)
)
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, DecimalType
from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, sum as sum_, abs as abs_, round as round_, lit
import re
from datetime import datetime

def main(session: snowpark.Session, account_number, p_input_date):

    def create_failure_df(reason: str):
        schema = StructType([
            StructField("status", StringType()),
            StructField("reason", StringType())
        ])
        data = [("FAILED", reason)]
        return session.create_dataframe(data, schema=schema)

    if not p_input_date:
        return create_failure_df("Input date is NULL or empty")

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", p_input_date):
        return create_failure_df("Invalid date format, expected YYYY-MM-DD")

    parsed_date = datetime.strptime(p_input_date, "%Y-%m-%d").date()

    account_query = f"""
        SELECT
            rcr.gl_account as "Account Number",
            sar.account_description as "Account Description",
            rcr.transcurbal_ending_balance_period_ytd as "Value"
        FROM idp_sapledger_prod.dbo.trial_balance rcr
        JOIN CE_NOVA_DEV.DBO.SAP_ACCOUNTS_REFERENCE sar
            ON rcr.gl_account = sar.account_number
        WHERE rcr.period = '{p_input_date}'
          AND rcr.gl_account = '{account_number}'
    """
    return session.sql(account_query)
$$;

CREATE OR REPLACE PROCEDURE CE_NOVA_DEV.DBO.SP_RECON_PORTFOLIO_SUMMARY(
    "P_INPUT_DATE" VARCHAR(60)
)
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, DecimalType
from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, sum as sum_, abs as abs_, round as round_, lit
import re
from datetime import datetime

def main(session: snowpark.Session, p_input_date):

    def create_failure_df(reason: str):
        schema = StructType([
            StructField("status", StringType()),
            StructField("reason", StringType())
        ])
        data = [("FAILED", reason)]
        return session.create_dataframe(data, schema=schema)

    if not p_input_date:
        return create_failure_df("Input date is NULL or empty")

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", p_input_date):
        return create_failure_df("Invalid date format, expected YYYY-MM-DD")

    parsed_date = datetime.strptime(p_input_date, "%Y-%m-%d").date()

    portfolio_summary_query = f"""
        SELECT
            ppr.portfolio_number,
            ppr.portfolio_description,
            SUM(rcr.transcurbal_ending_balance_period_ytd) AS total_value
        FROM idp_sapledger_prod.dbo.trial_balance rcr
        JOIN CE_NOVA_DEV.DBO.PAM_PORTFOLIO_REFERENCE ppr
            ON rcr.major_product = ppr.major_product
        WHERE rcr.period = '{p_input_date}'
        GROUP BY ppr.portfolio_number, ppr.portfolio_description
    """
    return session.sql(portfolio_summary_query)
$$;