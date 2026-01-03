CREATE OR REPLACE PROCEDURE CE_NOVA_DEV.DBO.SP_RECON_PORTFOLIO_SUMMARY(
    "PORTFOLIO_NUMBER" VARCHAR(60),
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
import calendar
from datetime import datetime

def main(session: snowpark.Session, portfolio_no, p_input_date):

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

    portfolio_summary_query = f"""
        SELECT
            ppr.portfolio_number,
            ppr.portfolio_description,
            SUM(rcr.transcurbal_ending_balance_period_ytd) AS total_balance
        FROM idp_sapledger_prod.dbo.trial_balance rcr
        JOIN temp_portfolio_ref ppr
            ON rcr.major_product = ppr.major_product
        WHERE rcr.period = '{p_input_date}'
          AND ppr.portfolio_number = '{portfolio_no}'
          AND rcr.runid = {max_runid}
        GROUP BY ppr.portfolio_number, ppr.portfolio_description
    """

    return session.sql(portfolio_summary_query)
$$;