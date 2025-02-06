-- This query RETURNS the longest queries OVER the specified INTERVAL
-- Change the value of interval_in_days to change how far the past the result is needed
-- The pricing related metrics are for on-demand models

DECLARE
  interval_in_days INT64 DEFAULT 7;
BEGIN
WITH
  src AS (
  SELECT
    user_email AS user,
    query,
    job_id AS jobId,
    project_id AS projectId,
    TIMESTAMP_DIFF(end_time, start_time, SECOND) AS runningTimeInSeconds,
    SAFE_DIVIDE(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), total_bytes_billed) AS runtimeToBytesBilledRatio, -- FOR ON-DEMAND
    start_time AS startTime,
    end_time AS endTime,
    ROUND(COALESCE(total_bytes_billed, 0), 2) AS totalBytesBilled,
    ROUND(COALESCE(total_bytes_billed, 0) / POW(1024, 2), 2) AS totalMegabytesBilled,
    ROUND(COALESCE(total_bytes_billed, 0) / POW(1024, 3), 2) AS totalGigabytesBilled,
    ROUND(COALESCE(total_bytes_billed, 0) / POW(1024, 4), 2) AS totalTerabytesBilled,
    ROUND(SAFE_DIVIDE(total_bytes_billed, POW(1024, 4)) * 5, 2) AS cost, -- FOR ON-DEMAND
    SAFE_DIVIDE(total_slot_ms, TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) AS approximateSlotCount,
    ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY end_time DESC) AS _rnk,
    ROW_NUMBER() OVER(PARTITION BY query ORDER BY total_bytes_billed DESC) AS _queryRank
  FROM
    `<PROJECT_ID>`.<REGION>.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE
    creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL interval_in_days DAY)
    AND CURRENT_TIMESTAMP()
    AND job_type = "QUERY"
    AND total_slot_ms IS NOT NULL
    AND state = "DONE" ),
  jobsDeduplicated AS (
  SELECT
    * EXCEPT(_rnk)
  FROM
    src
  WHERE
    _rnk = 1 ),
  queriesDeduplicated AS (
  SELECT
    * EXCEPT(_queryRank)
  FROM
    jobsDeduplicated
  WHERE
    _queryRank = 1 )
SELECT
  *
FROM
  queriesDeduplicated
ORDER BY
  runningTimeInSeconds DESC;
END
