COPY (
  SELECT
    coalesce(author, '') AS author,
    '__SUBMISSION_ID__' AS submission_id,
    CAST(created_utc AS BIGINT) AS created_utc,
    CAST(epoch(strptime('__CAPTURE_TS__', '%y%m%d%H%M%S')) AS BIGINT) AS capture_utc,
    coalesce(title, '') AS title,
    coalesce(selftext, '') AS body
  FROM read_json('__IN__', format='newline_delimited')
  WHERE CAST(created_utc AS BIGINT) >= __CUTOFF_EPOCH__
  LIMIT 1
) TO '__OUT__'
(FORMAT parquet, COMPRESSION '__COMPRESSION__');
