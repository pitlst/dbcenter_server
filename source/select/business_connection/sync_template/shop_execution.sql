SELECT  bill.fid                                                                                                                  AS "id"
       ,bill.fbillno                                                                                                              AS "单据编号"
       ,project.fname                                                                                                             AS "项目名称"
       ,bill.fk_crrc_desctitle                                                                                                    AS "标题"
       ,bill.fk_crrc_attachmentnums                                                                                               AS "附件数"
       ,bill.fk_crrc_reportbill                                                                                                   AS "关联报告编号"
       ,case bill.fbillstatus WHEN 'A' THEN '暂存' WHEN 'B' THEN '已提交' WHEN 'C' THEN '已审核' WHEN 'D' THEN '已完成' end                  AS "单据状态"
       ,case fk_crrc_isfirstconfirm WHEN 0 THEN '是' WHEN 1 THEN '否' end                                                           AS "是否完成首次确认"
       ,org.FNAME                                                                                                                 AS "所属组织"
       ,class.FNAME                                                                                                               AS "室组"
       ,bill.fk_crrc_ncreasid                                                                                                     AS "EAS-NCRid"
       ,bill.fk_crrc_waittaskid                                                                                                   AS "待办任务id"
       ,case bill.fk_crrc_taskcategory WHEN 'A' THEN 'NCR返工方案' WHEN 'B' THEN '打包方案' WHEN 'C' THEN '返工方案' WHEN 'D' THEN '临时任务' end AS "任务类别"
       ,bill.fk_crrc_backcause                                                                                                    AS "下发原因"
       ,bill.fk_crrc_title                                                                                                        AS "主题"
       ,bill.fk_crrc_bmgz                                                                                                         AS "编码规则取号"
       ,case bill.fk_crrc_iszhuanjiao WHEN 0 THEN '是' WHEN 1 THEN '否' end                                                         AS "是否转交编辑"
       ,auditor_user.FNUMBER                                                                                                      AS "批准人工号"
       ,auditor_user.FTRUENAME                                                                                                    AS "批准人姓名"
       ,bill.fauditdate                                                                                                           AS "批准日期"
       ,creator_user.FNUMBER                                                                                                      AS "创建人工号"
       ,creator_user.FTRUENAME                                                                                                    AS "创建人姓名"
       ,bill.fcreatetime                                                                                                          AS "创建时间"
       ,modifier_user.FNUMBER                                                                                                     AS "修改人工号"
       ,modifier_user.FTRUENAME                                                                                                   AS "修改人姓名"
       ,bill.fmodifytime                                                                                                          AS "修改时间"
FROM crrc_secd.tk_crrc_backworkexebill bill
LEFT JOIN crrc_secd.tk_crrc_projectmanager project
ON bill.fk_crrc_project = project.FID
LEFT JOIN crrc_sys.t_sec_user auditor_user
ON bill.fauditorid = auditor_user.FID
LEFT JOIN crrc_sys.t_sec_user creator_user
ON bill.fcreatorid = creator_user.FID
LEFT JOIN crrc_sys.t_sec_user modifier_user
ON bill.fmodifierid = modifier_user.FID
LEFT JOIN crrc_sys.t_org_org org
ON bill.fk_crrc_orgfield = org.FID
LEFT JOIN crrc_sys.t_org_org class
ON bill.fk_crrc_curentorg = class.FID