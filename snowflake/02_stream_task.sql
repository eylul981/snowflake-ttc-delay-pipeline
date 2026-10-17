-- =====================================================================
-- 02_stream_task.sql
-- Incremental RAW -> STAGING load using a Stream + Task + MERGE
-- - Stream tracks only NEW rows landed in RAW (change data capture)
-- - Task runs every 30 min, but only if the stream has data (no wasted credits)
-- - MERGE on a hash key makes the load idempotent (re-runs never duplicate)
-- =====================================================================

USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;   -- tasks need this privilege
USE ROLE SYSADMIN;
USE DATABASE TTC_DELAYS;

-- ---------- STAGING table: typed + standardized ----------
CREATE OR REPLACE TABLE STAGING.DELAYS (
  delay_id     STRING,          -- SHA2 hash of the business columns
  mode         STRING,          -- subway (bus / streetcar can be added later)
  delay_date   DATE,
  delay_time   TIME,
  day_of_week  STRING,
  line         STRING,
  station      STRING,
  delay_code   STRING,
  min_delay    NUMBER,
  min_gap      NUMBER,
  bound        STRING,
  vehicle      STRING,
  source_file  STRING,
  loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------- Stream on RAW ----------
-- APPEND_ONLY: RAW is insert-only, so only inserts matter.
-- SHOW_INITIAL_ROWS: the first read also returns rows already in RAW.
CREATE OR REPLACE STREAM STAGING.STR_SUBWAY
  ON TABLE RAW.SUBWAY_DELAYS
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = TRUE;

-- ---------- Task: consume the stream with an idempotent MERGE ----------
-- Lessons learned:
--  * CONCAT_WS returns NULL if ANY input is NULL -> COALESCE every column,
--    otherwise delay_id becomes NULL.
--  * The source file contains exact duplicate rows -> QUALIFY keeps one
--    per delay_id inside the batch, MERGE skips ones already loaded.
CREATE OR REPLACE TASK STAGING.TASK_LOAD_SUBWAY
  WAREHOUSE = TTC_WH
  SCHEDULE = '30 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('TTC_DELAYS.STAGING.STR_SUBWAY')
AS
MERGE INTO STAGING.DELAYS tgt
USING (
  SELECT
    SHA2(CONCAT_WS('|', 'subway',
      COALESCE("Date", ''), COALESCE("Time", ''), COALESCE("Station", ''),
      COALESCE("Code", ''), COALESCE("Line", ''), COALESCE("Vehicle", ''),
      COALESCE("Min Delay", '')))           AS delay_id,
    'subway'                                AS mode,
    TRY_TO_DATE("Date", 'YYYY-MM-DD')       AS delay_date,
    TRY_TO_TIME("Time", 'HH24:MI')          AS delay_time,
    INITCAP("Day")                          AS day_of_week,
    UPPER(TRIM("Line"))                     AS line,
    UPPER(TRIM("Station"))                  AS station,
    UPPER(TRIM("Code"))                     AS delay_code,
    TRY_TO_NUMBER("Min Delay")              AS min_delay,
    TRY_TO_NUMBER("Min Gap")                AS min_gap,
    UPPER(TRIM("Bound"))                    AS bound,
    TRIM("Vehicle")                         AS vehicle,
    _SOURCE_FILE                            AS source_file
  FROM STAGING.STR_SUBWAY
  QUALIFY ROW_NUMBER() OVER (PARTITION BY delay_id ORDER BY source_file) = 1
) src
ON tgt.delay_id = src.delay_id
WHEN NOT MATCHED THEN INSERT
  (delay_id, mode, delay_date, delay_time, day_of_week, line, station,
   delay_code, min_delay, min_gap, bound, vehicle, source_file)
VALUES
  (src.delay_id, src.mode, src.delay_date, src.delay_time, src.day_of_week,
   src.line, src.station, src.delay_code, src.min_delay, src.min_gap,
   src.bound, src.vehicle, src.source_file);

-- ---------- Run ----------
-- Tasks are created SUSPENDED. Run once manually:
EXECUTE TASK STAGING.TASK_LOAD_SUBWAY;

-- To run on the schedule (uses credits every time new data arrives):
-- ALTER TASK STAGING.TASK_LOAD_SUBWAY RESUME;

-- ---------- Checks ----------
-- SELECT COUNT(*) FROM STAGING.DELAYS;
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TASK_LOAD_SUBWAY'))
--   ORDER BY SCHEDULED_TIME DESC;
