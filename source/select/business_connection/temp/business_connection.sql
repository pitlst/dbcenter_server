SELECT  bill.FId                                                                                                 AS "id"
       ,bill.fbillno                                                                                             AS "单据编号"
       ,project.fnumber                                                                                          AS "项目编码"
       ,project.fname                                                                                            AS "项目名称"
       ,bill.fk_crrc_chgname                                                                                     AS "业务联系书标题"
       ,bill.fk_crrc_attachmentcount                                                                             AS "附件数"
       ,case bill.fbillstatus WHEN 'A' THEN '暂存' WHEN 'B' THEN '已提交' WHEN 'C' THEN '已审核' WHEN 'D' THEN '已完成' end AS "单据状态"
       ,bill.fk_crrc_craftid                                                                                     AS "关联工艺流程id"
       ,bill.fk_crrc_craftno                                                                                     AS "关联工艺流程单号"
       ,org.FNAME                                                                                                AS "所属组织名称"
       ,_group.FNAME                                                                                             AS "所属室组名称"
       ,bill.fk_crrc_waittaskid                                                                                  AS "待办任务id"
       ,bill.fk_crrc_otheredituser                                                                               AS "其他编制人信息"
       ,bill.fk_crrc_textfield                                                                                   AS "事业部相关部门信息"
       ,bill.fk_crrc_textareafield                                                                               AS "业务联系书内容"
       ,bill.fk_crrc_bmgz                                                                                        AS "编码规则取号"
       ,create_user.fnumber                                                                                      AS "创建人工号"
       ,create_user.FTRUENAME                                                                                    AS "创建人姓名"
       ,bill.fcreatetime                                                                                         AS "创建时间"
       ,modifier_user.fnumber                                                                                    AS "修改人工号"
       ,modifier_user.FTRUENAME                                                                                  AS "修改人姓名"
       ,bill.fmodifytime                                                                                         AS "修改时间"
       ,check_user.fnumber                                                                                       AS "审核人工号"
       ,check_user.FTRUENAME                                                                                     AS "审核人姓名"
       ,bill.fk_crrc_issuedate                                                                                   AS "审核时间"
       ,auditor_user.fnumber                                                                                     AS "签发人工号"
       ,auditor_user.FTRUENAME                                                                                   AS "签发人姓名"
       ,bill.fauditdate                                                                                          AS "签发时间"
FROM crrc_secd.tk_crrc_bizcontactbook bill
LEFT JOIN crrc_secd.tk_crrc_projectmanager project
ON bill.fk_crrc_projectid = project.fid
LEFT JOIN crrc_sys.t_org_org org
ON bill.fk_crrc_orgfield = org.FID
LEFT JOIN crrc_sys.t_org_org _group
ON bill.fk_crrc_curentorg = _group.FID
LEFT JOIN crrc_sys.t_sec_user create_user
ON bill.fcreatorid = create_user.FID
LEFT JOIN crrc_sys.t_sec_user modifier_user
ON bill.fmodifierid = modifier_user.FID
LEFT JOIN crrc_sys.t_sec_user check_user
ON bill.fk_crrc_issuer = check_user.FID
LEFT JOIN crrc_sys.t_sec_user auditor_user
ON bill.fauditorid = auditor_user.FID