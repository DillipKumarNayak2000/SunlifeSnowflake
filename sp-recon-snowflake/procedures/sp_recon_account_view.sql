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
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, sum as sum_, round as round_
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

    def validate_date(p_input_date):
        if not p_input_date:
            return False, create_failure_df("Input date is NULL or empty")

        if not re.match(r"^\d{4}-\d{2}-\d{2}$", p_input_date):
            return False, create_failure_df("Invalid date format, expected YYYY-MM-DD")

        try:
            parsed_date = datetime.strptime(p_input_date, "%Y-%m-%d").date()
        except ValueError:
            return False, create_failure_df("Invalid date")

        return True, parsed_date

    is_valid, date_or_df = validate_date(p_input_date)

    if not is_valid:
        return date_or_df

    # Use the validated date for queries
    p_input_date = date_or_df.strftime("%Y-%m-%d")

    account_query = f"""
        SELECT
            rcr.gl_account AS "Account Number",
            sar.account_description AS "Account Description",
            rcr.transcurbal_ending_balance_period_ytd AS "Value"
        FROM idp_sapledger_prod.dbo.trial_balance rcr
        JOIN CE_NOVA_DEV.DBO.SAP_ACCOUNTS_REFERENCE sar
            ON rcr.gl_account = sar.account_number
        WHERE rcr.period = '{p_input_date}'
          AND rcr.gl_account = '{account_number}'
    """

    return session.sql(account_query)
$$;