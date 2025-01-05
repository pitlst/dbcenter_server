SELECT  p.FID                                                                                     AS "FID"
       ,p.FNumber                                                                                 AS "员工编码"
       ,p.fname_l2                                                                                AS "员工姓名"
       ,TO_CHAR(p.CFFilebirthday,'yyyy-mm-dd')                                                    AS "生日"
       ,e.fname_l2                                                                                AS "用工状态"
       ,hy.fname_l2                                                                               AS "婚姻状态"
       ,zzmm.fname_l2                                                                             AS "政治面貌"
       ,p.FCell                                                                                   AS "手机号码"
       ,p.FHomeplace                                                                              AS "出生地"
       ,p.FIDCardNO                                                                               AS "身份证号"
       ,zhiwei.fname_l2                                                                           AS "职位"
       ,zc.fname_l2                                                                               AS "职称"
       ,zyzg.fname_l2                                                                             AS "职业资格"
       ,p.FAddress_l2                                                                             AS "地址"
       ,p.CFIcNum                                                                                 AS "物理卡号"
       ,CASE WHEN p.CFASSIGNTYPETXT = '主要任职' THEN lo0.Fnumber  ELSE lo1.Fnumber END               AS "一级组织编码"
       ,CASE WHEN p.CFASSIGNTYPETXT = '主要任职' THEN lo0.FName_l2  ELSE lo1.FName_l2 END             AS "一级组织名称"
       ,CASE WHEN p.CFASSIGNTYPETXT = '主要任职' THEN o0.Fnumber  ELSE o1.Fnumber END                 AS "二级组织编码"
       ,CASE WHEN p.CFASSIGNTYPETXT = '主要任职' THEN o0.FName_l2  ELSE o1.FName_l2 END               AS "二级组织名称"
       ,CASE WHEN p.CFASSIGNTYPETXT = '主要任职' THEN d0.Fnumber  ELSE d1.Fnumber END                 AS "末级组织ID"
       ,CASE WHEN p.CFASSIGNTYPETXT = '主要任职' THEN d0.FNAME_L2  ELSE d1.FNAME_L2 END               AS "末级组织名称"
       ,CASE WHEN p.CFASSIGNTYPETXT = '主要任职' THEN d0.FDisplayName_l2  ELSE d1.FDisplayName_l2 END AS "完整组织名称"
FROM EAS86.T_BD_PERSON p
LEFT JOIN EAS86.T_HR_BDEMPLOYEETYPE e
ON p.FEmployeetypeiD = e.FID -- 用工表 
LEFT JOIN EAS86.T_ORG_Admin d0
ON p.CFNewAdminOrgID = d0.fid -- 行政组织（末级） 
LEFT JOIN EAS86.T_ORG_Admin d1
ON p.FGKADMIN = d1.fid -- 行政组织（末级） 
LEFT JOIN EAS86.T_ORG_Admin lo0
ON d0.FLevelOneGroupID = lo0.FID -- 连接一级组织表 
LEFT JOIN EAS86.T_ORG_Admin lo1
ON d1.FLevelOneGroupID = lo1.FID -- 连接一级组织表 
LEFT JOIN EAS86.T_ORG_Admin o0
ON d0.FLevelTwoGroupID = o0.FID -- 连接二级组织表 
LEFT JOIN EAS86.T_ORG_Admin o1
ON d1.FLevelTwoGroupID = o1.FID -- 连接二级组织表 
LEFT JOIN EAS86.T_ORG_Position zhiwei
ON p.CFNewPositionID = zhiwei.fid -- 职位表 
LEFT JOIN EAS86.T_BD_HRWed hy
ON p.FWedID = hy.fid -- 婚姻状态 
LEFT JOIN EAS86.T_BD_HRPolitical zzmm
ON p.FPoliticalFaceID = zzmm.fid -- 政治面貌 
LEFT JOIN EAS86.T_HR_BDTechnicalPost zc
ON p.FHighestTechPostID = zc.fid -- 职称 
LEFT JOIN EAS86.T_HR_BDCertifyCompetency zyzg
ON p.FHighestCompetencyID = zyzg.fid -- 职业资格 
WHERE p.FEmployeetypeiD <> '00000000-0000-0000-0000-000000000031A29E85B3'
AND p.FEmployeetypeiD <> 'FTCmJT3SSRi1twufIaRb56KehbM='
AND p.FEmployeetypeiD <> '00000000-0000-0000-0000-000000000008A29E85B3'
-- 用工状态过滤
