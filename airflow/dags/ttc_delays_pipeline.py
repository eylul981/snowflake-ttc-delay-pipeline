"""
TTC delays pipeline: Stage -> RAW -> STAGING -> dbt MARTS -> quality check.
Runs daily. Every step is idempotent, so re-runs are safe.
"""
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.common.sql.sensors.sql import SqlSensor

SF = "snowflake_default"

with DAG(
    dag_id="ttc_delays_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ttc", "snowflake", "dbt"],
):
    # 1. Load new files from the stage. COPY INTO remembers loaded files,
    #    so files already loaded are skipped automatically.
    load_raw = SQLExecuteQueryOperator(
        task_id="load_raw",
        conn_id=SF,
        sql="""
            COPY INTO TTC_DELAYS.RAW.SUBWAY_DELAYS
            FROM @TTC_DELAYS.RAW.TTC_STAGE/Subway/
            PATTERN = '.*Subway Delay Data.*[.]csv'
            FILE_FORMAT = (FORMAT_NAME = TTC_DELAYS.RAW.FF_TTC_CSV)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (_SOURCE_FILE = METADATA$FILENAME)
            ON_ERROR = 'CONTINUE'
        """,
    )

    # 2. Trigger the Snowflake Task that MERGEs new rows into STAGING.
    run_merge = SQLExecuteQueryOperator(
        task_id="run_merge",
        conn_id=SF,
        sql="EXECUTE TASK TTC_DELAYS.STAGING.TASK_LOAD_SUBWAY",
    )

    # 3. EXECUTE TASK is asynchronous, so wait until the stream is empty,
    #    which means the MERGE has consumed all new rows.
    wait_for_merge = SqlSensor(
        task_id="wait_for_merge",
        conn_id=SF,
        sql="SELECT NOT SYSTEM$STREAM_HAS_DATA('TTC_DELAYS.STAGING.STR_SUBWAY')",
        poke_interval=15,
        timeout=600,
        mode="reschedule",
    )

    # 4. Build and test all dbt models inside Snowflake.
    dbt_build = SQLExecuteQueryOperator(
        task_id="dbt_build",
        conn_id=SF,
        sql="EXECUTE DBT PROJECT TTC_DELAYS.PUBLIC.TTC_DELAYS_DBT ARGS = 'build'",
    )

    # 5. Fail the run if the fact table is empty, has duplicate keys or bad dates.
    quality_check = SQLCheckOperator(
        task_id="quality_check",
        conn_id=SF,
        sql="""
            SELECT
                COUNT(*) > 0                          AS has_rows,
                COUNT(*) = COUNT(DISTINCT delay_id)   AS keys_unique,
                COUNT_IF(delay_date IS NULL) = 0      AS dates_valid
            FROM TTC_DELAYS.MARTS.FCT_DELAYS
        """,
    )

    load_raw >> run_merge >> wait_for_merge >> dbt_build >> quality_check
