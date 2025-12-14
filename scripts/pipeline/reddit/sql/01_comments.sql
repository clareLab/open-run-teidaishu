COPY (
  SELECT
    coalesce(author, '') AS author,
    '__SUBMISSION_ID__' AS submission_id,
    coalesce(id, '') AS comment_id,
    coalesce(parent_id, '') AS parent_id,
    CAST(created_utc AS BIGINT) AS created_utc,
    CAST(epoch(strptime('__CAPTURE_TS__', '%y%m%d%H%M%S')) AS BIGINT) AS capture_utc,
    coalesce(body, '') AS body
  FROM read_json('__IN__', format='newline_delimited')
  WHERE id IS NOT NULL
    AND CAST(created_utc AS BIGINT) >= __CUTOFF_EPOCH__
) TO '__OUT__'
(FORMAT parquet, COMPRESSION '__COMPRESSION__');
