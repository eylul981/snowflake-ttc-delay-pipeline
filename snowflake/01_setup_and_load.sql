-- =====================================================================
-- 01_setup_and_load.sql
-- Infrastructure + raw load for the TTC subway delay pipeline
-- RAW layer: data lands as-is (all TEXT) with load metadata
-- =====================================================================

USE ROLE SYSADMIN;

-- ---------- Compute: smallest warehouse, suspends after 60s idle ----------
CREATE WAREHOUSE IF NOT EXISTS TTC_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- ---------- Database + medallion-style schemas ----------
CREATE DATABASE IF NOT EXISTS TTC_DELAYS;
USE DATABASE TTC_DELAYS;

CREATE SCHEMA IF NOT EXISTS RAW;      -- files loaded as-is
CREATE SCHEMA IF NOT EXISTS STAGING;  -- typed, cleaned, de-duplicated
CREATE SCHEMA IF NOT EXISTS MARTS;    -- dbt models for analytics

-- ---------- File format ----------
-- PARSE_HEADER lets COPY match CSV columns by name.
-- ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE: the source file has rows
-- with an extra trailing column, which would otherwise fail the load.
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_TTC_CSV
  TYPE = 'CSV'
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_BLANK_LINES = TRUE
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  ENCODING = 'UTF8'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ---------- Internal stage (files uploaded via Snowsight) ----------
CREATE STAGE IF NOT EXISTS RAW.TTC_STAGE
  FILE_FORMAT = RAW.FF_TTC_CSV
  DIRECTORY = (ENABLE = TRUE);

-- ---------- RAW tables: everything TEXT, typing happens downstream ----------
CREATE OR REPLACE TABLE RAW.SUBWAY_DELAYS (
  "Date"       TEXT,
  "Time"       TEXT,
  "Day"        TEXT,
  "Station"    TEXT,
  "Code"       TEXT,
  "Min Delay"  TEXT,
  "Min Gap"    TEXT,
  "Bound"      TEXT,
  "Line"       TEXT,
  "Vehicle"    TEXT,
  _SOURCE_FILE TEXT,
  _LOADED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.DELAY_CODES (
  "CODE"        TEXT,
  "DESCRIPTION" TEXT,
  _SOURCE_FILE  TEXT,
  _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------- Load ----------
COPY INTO RAW.SUBWAY_DELAYS
  FROM @RAW.TTC_STAGE/Subway/
  FILES = ('TTC Subway Delay Data since 2025.csv')
  FILE_FORMAT = (FORMAT_NAME = RAW.FF_TTC_CSV)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  INCLUDE_METADATA = (_SOURCE_FILE = METADATA$FILENAME)
  ON_ERROR = 'CONTINUE';

COPY INTO RAW.DELAY_CODES
  FROM @RAW.TTC_STAGE/Subway/
  FILES = ('Code Descriptions.csv')
  FILE_FORMAT = (FORMAT_NAME = RAW.FF_TTC_CSV)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  INCLUDE_METADATA = (_SOURCE_FILE = METADATA$FILENAME)
  ON_ERROR = 'CONTINUE';
