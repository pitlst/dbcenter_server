SELECT  p.fnumber               AS "项目"
       ,t.fnumber               AS "跟踪号"
       ,jch.fnumber             AS "节车号"
       ,o.fnumber               AS "工序"
       ,o.fname_l2              AS "工序名称"
       ,r.FBIZDATE              AS "时间"
       ,s.fnumber               AS "人员"
       ,s.fname_l2              AS "人员名称"
       ,SUM(rt.cfworkTimeHS)    AS "工时"
       ,SUM(rt.CFreplenishTime) AS "补报工时"
FROM ZJEAS7.T_MM_CompletionReport r
LEFT JOIN ZJEAS7.T_MM_CompletionRAT rt
ON r.fid = rt.fparentid
LEFT JOIN ZJEAS7.T_MM_PROJECT p
ON p.fid = r.FPROJECTID
LEFT JOIN ZJEAS7.T_MM_TRACKNUMBER t
ON t.fid = r.ftracknumberid
LEFT JOIN ZJEAS7.T_PRO_ProjectJCH jch
ON jch.fid = r.cfProjectJCHid
LEFT JOIN ZJEAS7.T_MM_Operation o
ON o.fid = r.cfoperationid
LEFT JOIN ZJEAS7.t_bd_person s
ON s.fid = rt.FPERSONNUMBERID
WHERE r.FBIZDATE >= SYSDATE - INTERVAL '1' YEAR
AND rt.FPERSONNUMBERID is not null
AND ( ( rt.cfworkTimeHS is not null AND rt.cfworkTimeHS <> 0 ) or ( rt.CFreplenishTime <> 0 AND rt.CFreplenishTime is not null ) )
GROUP BY  r.FBIZDATE
         ,p.fnumber
         ,t.fnumber
         ,jch.fnumber
         ,o.fnumber
         ,o.fname_l2
         ,s.fnumber
         ,s.fname_l2
ORDER BY  r.FBIZDATE
         ,p.fnumber
         ,t.fnumber
         ,jch.fnumber
         ,o.fnumber
         ,o.fname_l2
         ,s.fnumber
         ,s.fname_l2