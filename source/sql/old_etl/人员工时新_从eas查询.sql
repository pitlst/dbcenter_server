SELECT
  s.fnumber AS s_fnumber,
  s.fname_l2 AS s_fname_l2,
  tr.temp_date AS r_fbizdate,
  SUM(rt.cfworkTimeHS / 60) AS rt_cfworkTimeHS,
  SUM(rt.CFreplenishTime / 60) AS rt_CFreplenishTime
FROM
  ZJEAS7.T_MM_CompletionReport r
  LEFT JOIN ZJEAS7.T_MM_CompletionRAT rt ON r.fid = rt.fparentid
  LEFT JOIN ZJEAS7.T_MM_PROJECT p ON p.fid = r.FPROJECTID
  LEFT JOIN ZJEAS7.T_MM_TRACKNUMBER t ON t.fid = r.ftracknumberid
  LEFT JOIN ZJEAS7.T_PRO_ProjectJCH jch ON jch.fid = r.cfProjectJCHid
  LEFT JOIN ZJEAS7.t_bd_person s ON s.fid = rt.FPERSONNUMBERID
  LEFT JOIN (
    SELECT
      r.fid AS temp_fid,
      TO_DATE(TO_CHAR(r.FBIZDATE, 'yyyy-mm'), 'yyyy-mm') AS temp_date
    FROM
      ZJEAS7.T_MM_CompletionReport r
  ) tr ON r.fid = tr.temp_fid
WHERE
  -- sysdate - r.FBIZDATE <= INTERVAL '1' DAY(4)
  -- r.FBIZDATE > TO_DATE('2023-01-01', 'yyyy-mm-dd')
  add_months(sysdate,-6) <= r.FBIZDATE 
  AND rt.FPERSONNUMBERID IS NOT NULL
  AND r.FPSTORAGEORGUNITID = 'nXnw4GGNRh+AxZ/cMEMFpcznrtQ='
  AND (
    r.FPROJECTID IN ('')
    OR NULL IS NULL
  )
  AND (
    (
      rt.cfworkTimeHS IS NOT NULL
      AND rt.cfworkTimeHS <> 0
    )
    OR (
      rt.CFreplenishTime <> 0
      AND rt.CFreplenishTime IS NOT NULL
    )
  )
GROUP BY
  s.fnumber,
  s.fname_l2,
  tr.temp_date