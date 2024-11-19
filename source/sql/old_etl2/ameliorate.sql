SELECT  f.fk_crrc_proposal_no                                                                                                               AS "提案编号"
       ,f.fk_crrc_proposal_name                                                                                                             AS "题案名称"
       ,( CASE f.fk_crrc_proposal_status WHEN "A" THEN "暂存" WHEN "B" THEN "已提交" WHEN "C" THEN "已审核" WHEN "D" THEN "提前终止" ELSE "未知的状态" END ) AS "题案状态"
       ,f.fk_crrc_proposal_type                                                                                                             AS "改善类型"
       ,creator_user.FTRUENAME                                                                                                              AS "第一题案人"
       ,creator_user.FNUMBER                                                                                                                AS "第一题案人工号"
       ,f2nd_proposer_user.FTRUENAME                                                                                                        AS "第二申报人"
       ,f2nd_proposer_user.FNUMBER                                                                                                          AS "第二申报人工号"
       ,f.fcreatetime                                                                                                                       AS "创建时间"
       ,f.fk_crrc_final_grade                                                                                                               AS "最终等级分"
       ,org1.FNAME                                                                                                                          AS "提案单位一级"
       ,org2.FNAME                                                                                                                          AS "提案单位二级"
       ,last_org.FNAME                                                                                                                      AS "班组"
       ,case f.fk_crrc_prize WHEN 'A' THEN '一等奖' WHEN 'B' THEN '二等奖' WHEN 'C' THEN '三等奖' end                                       AS "奖项"
       ,auditor_user.FTRUENAME                                                                                                              AS "审核人"
       ,auditor_user.FNUMBER                                                                                                                AS "审核人工号"
       ,f.fauditdate                                                                                                                        AS "审核日期"
       ,f.fk_crrc_currentapprover                                                                                                           AS "流程当前处理人"
       ,f.fk_crrc_is_recommened                                                                                                             AS "是否推优"
       ,f1st_unit_auditor_user.FTRUENAME                                                                                                    AS "一级单位改善专员"
       ,f1st_unit_auditor_user.FNUMBER                                                                                                      AS "一级单位改善专员工号"
       ,f2nd_unit_auditor_user.FTRUENAME                                                                                                    AS "二级单位改善专员"
       ,f2nd_unit_auditor_user.FNUMBER                                                                                                      AS "二级单位改善专员工号"
       ,f.fk_crrc_submit_date                                                                                                               AS "提交日期"
       ,submit_user.FTRUENAME                                                                                                               AS "提交用户"
       ,submit_user.FNUMBER                                                                                                                 AS "提交用户工号"
       ,f.fk_crrc_end_date                                                                                                                  AS "提前终止日期"
       ,end_user.FTRUENAME                                                                                                                  AS "提前终止用户"
       ,end_user.FNUMBER                                                                                                                    AS "提前终止用户工号"
FROM crrc_secd.tk_crrc_jygs_evo_apl f
LEFT JOIN crrc_sys.t_sec_user creator_user
ON f.fk_crrc_creator = creator_user.FID
LEFT JOIN crrc_sys.t_sec_user f2nd_proposer_user
ON f.fk_crrc_2nd_proposer = f2nd_proposer_user.FID
LEFT JOIN crrc_sys.t_sec_user auditor_user
ON f.fk_crrc_auditor = auditor_user.FID
LEFT JOIN crrc_sys.t_sec_user f1st_unit_auditor_user
ON f.fk_crrc_1st_unit_auditor = f1st_unit_auditor_user.FID
LEFT JOIN crrc_sys.t_sec_user f2nd_unit_auditor_user
ON f.fk_crrc_2nd_unit_auditor = f2nd_unit_auditor_user.FID
LEFT JOIN crrc_sys.t_sec_user submit_user
ON f.fk_crrc_submit_user = submit_user.FID
LEFT JOIN crrc_sys.t_sec_user end_user
ON f.fk_crrc_end_user = end_user.FID
LEFT JOIN crrc_sys.t_org_org org1
ON f.fk_crrc_org1 = org1.FID
LEFT JOIN crrc_sys.t_org_org org2
ON f.fk_crrc_org2 = org2.FID
LEFT JOIN crrc_sys.t_org_org last_org
ON f.fk_crrc_last_org = last_org.FID