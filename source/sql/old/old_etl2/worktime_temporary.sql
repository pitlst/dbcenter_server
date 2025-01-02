SELECT  f.F28273 AS "员工编码"
       ,f.F28274 AS "姓名"
       ,p.F28264 AS "作业日期"
       ,f.F34267 AS "批准工时"
FROM tabDIYTable3292 f
LEFT JOIN tabDIYTable3290 p
ON f.id = p.id
WHERE getDate() - p.F28264 < 360