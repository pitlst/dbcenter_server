SELECT  p.id                                                                                        AS "id"
       ,p.EmployeeID                                                                                AS "工号"
       ,p.EmployeeName                                                                              AS "姓名"
       ,p.IdentityCard                                                                              AS "身份证"
       ,'在职'                                                                                        AS "是否在职"
       ,case p.EmployDateStart WHEN cast('1900-01-01' AS date) THEN NULL else p.EmployDateStart end AS "雇佣开始日期"
       ,case p.InCompanyDate WHEN cast('1900-01-01' AS date) THEN NULL else p.InCompanyDate end     AS "进入公司日期"
       ,case p.EmployDateEnd WHEN cast('1900-01-01' AS date) THEN NULL else p.EmployDateEnd end     AS "雇佣结束日期"
       ,case p.BeFormalDate WHEN cast('1900-01-01' AS date) THEN NULL else p.BeFormalDate end       AS "实习转正日期"
       ,case p.WeddingDate WHEN cast('1900-01-01' AS date) THEN NULL else p.WeddingDate end         AS "结婚日期"
       ,p.Age                                                                                       AS "年龄"
       ,d0.DeptName                                                                                 AS "末级组织名称"
       ,d1.DeptName                                                                                 AS "四级组织名称"
       ,d2.DeptName                                                                                 AS "三级组织名称"
       ,d3.DeptName                                                                                 AS "二级组织名称"
       ,d4.DeptName                                                                                 AS "一级组织名称"
FROM YQ_KQ_EmployeeInfo p
LEFT JOIN YQ_CM_Department d0
ON p.DeptID = d0.ID
LEFT JOIN YQ_CM_Department d1
ON d0.ParentID = d1.ID
LEFT JOIN YQ_CM_Department d2
ON d1.ParentID = d2.ID
LEFT JOIN YQ_CM_Department d3
ON d2.ParentID = d3.ID
LEFT JOIN YQ_CM_Department d4
ON d3.ParentID = d4.ID
UNION ALL
SELECT  p.id                                                                                        AS "id"
       ,p.EmployeeID                                                                                AS "工号"
       ,p.EmployeeName                                                                              AS "姓名"
       ,p.IdentityCard                                                                              AS "身份证"
       ,'离职'                                                                                        AS "是否在职"
       ,case p.EmployDateStart WHEN cast('1900-01-01' AS date) THEN NULL else p.EmployDateStart end AS "雇佣开始日期"
       ,case p.InCompanyDate WHEN cast('1900-01-01' AS date) THEN NULL else p.InCompanyDate end     AS "进入公司日期"
       ,case p.EmployDateEnd WHEN cast('1900-01-01' AS date) THEN NULL else p.EmployDateEnd end     AS "雇佣结束日期"
       ,case p.BeFormalDate WHEN cast('1900-01-01' AS date) THEN NULL else p.BeFormalDate end       AS "实习转正日期"
       ,case p.WeddingDate WHEN cast('1900-01-01' AS date) THEN NULL else p.WeddingDate end         AS "结婚日期"
       ,p.Age                                                                                       AS "年龄"
       ,d0.DeptName                                                                                 AS "末级组织名称"
       ,d1.DeptName                                                                                 AS "四级组织名称"
       ,d2.DeptName                                                                                 AS "三级组织名称"
       ,d3.DeptName                                                                                 AS "二级组织名称"
       ,d4.DeptName                                                                                 AS "一级组织名称"
FROM YQ_KQ_EmployeeInfoLeave p
LEFT JOIN YQ_CM_Department d0
ON p.DeptID = d0.ID
LEFT JOIN YQ_CM_Department d1
ON d0.ParentID = d1.ID
LEFT JOIN YQ_CM_Department d2
ON d1.ParentID = d2.ID
LEFT JOIN YQ_CM_Department d3
ON d2.ParentID = d3.ID
LEFT JOIN YQ_CM_Department d4
ON d3.ParentID = d4.ID