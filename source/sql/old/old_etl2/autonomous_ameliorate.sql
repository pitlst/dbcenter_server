SELECT  f.fk_crrc_proposal_no                                                                                              AS "改善编码"
       ,f.fk_crrc_proposal_name                                                                                            AS "题案名称"
       ,case f.fk_crrc_proposal_status WHEN 'A' THEN '暂存' WHEN 'B' THEN '已提交' WHEN 'C' THEN '已审核' WHEN 'D' THEN '提前终止' end AS "提案状态"
       ,f.fk_crrc_proposal_type                                                                                            AS "改善类型"
       ,participants1.FTRUENAME                                                                                            AS "改善参与人1"
       ,participants1.FNUMBER                                                                                              AS "改善参与人1工号"
       ,participants2.FTRUENAME                                                                                            AS "改善参与人2"
       ,participants2.FNUMBER                                                                                              AS "改善参与人2工号"
       ,participants3.FTRUENAME                                                                                            AS "改善参与人3"
       ,participants3.FNUMBER                                                                                              AS "改善参与人3工号"
       ,f.fcreatetime                                                                                                      AS "创建时间"
       ,f.fk_crrc_final_grade                                                                                              AS "最终等级分"
       ,org1.FNAME                                                                                                         AS "参与部门1"
       ,org2.FNAME                                                                                                         AS "参与部门2"
       ,org3.FNAME                                                                                                         AS "参与部门3"
       ,f.fk_crrc_in_org1_g                                                                                                AS "参与部门1贡献度"
       ,f.fk_crrc_in_org2_g                                                                                                AS "参与部门2贡献度"
       ,f.fk_crrc_in_org3_g                                                                                                AS "参与部门3贡献度"
       ,org.FNAME                                                                                                          AS "班组信息"
       ,case f.fk_crrc_prize WHEN 'A' THEN '一等奖' WHEN 'B' THEN '二等奖' WHEN 'C' THEN '三等奖' end                               AS "奖项"
       ,auditor.FTRUENAME                                                                                                  AS "审核人"
       ,auditor.FNUMBER                                                                                                    AS "审核人工号"
       ,f.fauditdate                                                                                                       AS "审核日期"
       ,f.fk_crrc_currentapprover                                                                                          AS "流程当前处理人"
       ,case f.fk_crrc_is_recommened WHEN '0' THEN '否' WHEN '1' THEN '是' end                                               AS "是否推优"
       ,f.fk_crrc_submit_date                                                                                              AS "提交日期"
       ,submit_user.FTRUENAME                                                                                              AS "提交用户"
       ,submit_user.FNUMBER                                                                                                AS "提交用户工号"
       ,f.fk_crrc_end_date                                                                                                 AS "提前终止日期"
       ,end_user.FTRUENAME                                                                                                 AS "提前终止用户"
       ,end_user.FNUMBER                                                                                                   AS "提前终止用户工号"
FROM crrc_secd.tk_crrc_jygs_self AS f
LEFT JOIN crrc_sys.t_sec_user participants1
ON f.fk_crrc_participants1 = participants1.FID
LEFT JOIN crrc_sys.t_sec_user participants2
ON f.fk_crrc_participants2 = participants2.FID
LEFT JOIN crrc_sys.t_sec_user participants3
ON f.fk_crrc_participants3 = participants3.FID
LEFT JOIN crrc_sys.t_org_org org
ON f.fk_crrc_last_org = org.FID
LEFT JOIN crrc_sys.t_org_org org1
ON f.fk_crrc_in_org1 = org1.FID
LEFT JOIN crrc_sys.t_org_org org2
ON f.fk_crrc_in_org2 = org2.FID
LEFT JOIN crrc_sys.t_org_org org3
ON f.fk_crrc_in_org3 = org3.FID
LEFT JOIN crrc_sys.t_sec_user auditor
ON f.fk_crrc_auditor = auditor.FID
LEFT JOIN crrc_sys.t_sec_user submit_user
ON f.fk_crrc_submit_user = submit_user.FID
LEFT JOIN crrc_sys.t_sec_user end_user
ON f.fk_crrc_end_user = end_user.FID