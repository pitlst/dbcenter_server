SELECT  p.FID                                  AS "FID"
       ,p.FNumber                              AS "员工编码"
       ,p.fname_l2                             AS "员工姓名"
       ,to_char(p.CFFilebirthday,'yyyy-mm-dd') AS "生日"
       ,e.fname_l2                             AS "用工状态"
       ,hy.fname_l2                            AS "婚姻状态"
       ,zzmm.fname_l2                          AS "政治面貌"
       ,p.fcreatetime                          AS "shr创建时间"
       ,p.cfspecialty                          AS "专业"
       ,p.FCell                                AS "手机号码"
       ,p.fbackupemail                         AS "邮箱"
       ,p.FHomeplace                           AS "出生地"
       ,p.FIDCardNO                            AS "身份证号"
       ,zhiwei.fname_l2                        AS "职位"
       ,zc.fname_l2                            AS "职称"
       ,zyzg.fname_l2                          AS "职业资格"
       ,p.FAddress_l2                          AS "地址"
       ,lo.fnumber                             AS "一级组织编码"
       ,lo.FName_l2                            AS "一级组织名称"
       ,o.fnumber                              AS "二级组织编码"
       ,o.FName_l2                             AS "二级组织名称"
       ,d.fnumber                              AS "未级组织编码"
       ,d.FName_l2                             AS "末级组织名称"
       ,d.FDisplayName_l2                      AS "完整组织名称"
FROM EAS86.T_BD_PERSONHIS p
LEFT JOIN EAS86.T_HR_BDEMPLOYEETYPE e
ON p.FEmployeetypeiD = e.FID
LEFT JOIN EAS86.T_ORG_Admin d
ON p.CFNewAdminOrgID = d.fid
LEFT JOIN EAS86.T_ORG_Admin dt
ON p.CFNEWPOSITIONID = dt.fid
LEFT JOIN EAS86.T_ORG_Admin lo
ON d.FLevelOneGroupID = lo.FID
LEFT JOIN EAS86.T_ORG_Admin lot
ON dt.FLevelOneGroupID = lot.FID
LEFT JOIN EAS86.T_ORG_Admin o
ON d.FLevelTwoGroupID = o.FID
LEFT JOIN EAS86.T_ORG_Position zhiwei
ON p.CFNewPositionID = zhiwei.fid
LEFT JOIN EAS86.T_BD_HRWed hy
ON p.FWedID = hy.fid
LEFT JOIN EAS86.T_BD_HRPolitical zzmm
ON p.FPoliticalFaceID = zzmm.fid
LEFT JOIN EAS86.T_HR_BDTechnicalPost zc
ON p.FHighestTechPostID = zc.fid
LEFT JOIN EAS86.T_HR_BDCertifyCompetency zyzg
ON p.FHighestCompetencyID = zyzg.fid
WHERE p.CFASSIGNTYPETXT = '主要任职'
UNION
SELECT  p.FID                                  AS "FID"
       ,p.FNumber                              AS "员工编码"
       ,p.fname_l2                             AS "员工姓名"
       ,to_char(p.CFFilebirthday,'yyyy-mm-dd') AS "生日"
       ,e.fname_l2                             AS "用工状态"
       ,hy.fname_l2                            AS "婚姻状态"
       ,zzmm.fname_l2                          AS "政治面貌"
       ,p.fcreatetime                          AS "shr创建时间"
       ,p.cfspecialty                          AS "专业"
       ,p.FCell                                AS "手机号码"
       ,p.fbackupemail                         AS "邮箱"
       ,p.FHomeplace                           AS "出生地"
       ,p.FIDCardNO                            AS "身份证号"
       ,zhiwei.fname_l2                        AS "职位"
       ,zc.fname_l2                            AS "职称"
       ,zyzg.fname_l2                          AS "职业资格"
       ,p.FAddress_l2                          AS "地址"
       ,lo.fnumber                             AS "一级组织编码"
       ,lo.FName_l2                            AS "一级组织名称"
       ,o.fnumber                              AS "二级组织编码"
       ,o.FName_l2                             AS "二级组织名称"
       ,d.fnumber                              AS "未级组织编码"
       ,d.FName_l2                             AS "末级组织名称"
       ,d.FDisplayName_l2                      AS "完整组织名称"
FROM EAS86.T_BD_PERSON p
LEFT JOIN EAS86.T_HR_BDEMPLOYEETYPE e
ON p.FEmployeetypeiD = e.FID
LEFT JOIN EAS86.T_ORG_Admin d
ON p.CFNewAdminOrgID = d.fid
LEFT JOIN EAS86.T_ORG_Admin dt
ON p.CFNEWPOSITIONID = dt.fid
LEFT JOIN EAS86.T_ORG_Admin lo
ON d.FLevelOneGroupID = lo.FID
LEFT JOIN EAS86.T_ORG_Admin lot
ON dt.FLevelOneGroupID = lot.FID
LEFT JOIN EAS86.T_ORG_Admin o
ON d.FLevelTwoGroupID = o.FID
LEFT JOIN EAS86.T_ORG_Position zhiwei
ON p.CFNewPositionID = zhiwei.fid
LEFT JOIN EAS86.T_BD_HRWed hy
ON p.FWedID = hy.fid
LEFT JOIN EAS86.T_BD_HRPolitical zzmm
ON p.FPoliticalFaceID = zzmm.fid
LEFT JOIN EAS86.T_HR_BDTechnicalPost zc
ON p.FHighestTechPostID = zc.fid
LEFT JOIN EAS86.T_HR_BDCertifyCompetency zyzg
ON p.FHighestCompetencyID = zyzg.fid
WHERE p.CFASSIGNTYPETXT = '主要任职'