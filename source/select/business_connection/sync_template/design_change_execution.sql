SELECT  bill.FId                                                                                                 AS "id"
       ,bill.fbillno                                                                                             AS "单据编号"
       ,project.fnumber                                                                                          AS "项目编码"
       ,project.fname                                                                                            AS "项目名称"
       ,bill.fk_crrc_attanum                                                                                     AS "附件数"
       ,bill.fk_crrc_chgname                                                                                     AS "变更单名称"
       ,bill.fk_crrc_relationchgbill                                                                             AS "关联工艺流程单号"
       ,bill.fk_crrc_designbillnum                                                                               AS "关联设计变更单编码"
       ,case bill.fbillstatus WHEN 'A' THEN '暂存' WHEN 'B' THEN '已提交' WHEN 'C' THEN '已审核' WHEN 'D' THEN '已完成' end AS "单据状态"
       ,bill.fk_crrc_waittaskid                                                                                  AS "待办任务id"
       ,bill.fk_crrc_craftid                                                                                     AS "工艺流程id"
       ,case bill.fk_crrc_isrework WHEN 0 THEN '是' WHEN 1 THEN '否' end                                           AS "是否返工"
       ,case bill.fk_crrc_isfirstconfirm WHEN 0 THEN '是' WHEN 1 THEN '否' end                                     AS "是否完成首次确认"
       ,org.FNAME                                                                                                AS "所属组织"
       ,_group.fname                                                                                             AS "室组"
       ,case bill.fk_crrc_quotacy WHEN 0 THEN '是' WHEN 1 THEN '否' end                                            AS "定额是否差异"
       ,case bill.fk_crrc_productionstage WHEN 'A' THEN '试制' WHEN 'B' THEN '批量' end                              AS "生产阶段"
       ,bill.fk_crrc_chgcause                                                                                    AS "变更原因"
       ,bill.fk_crrc_description                                                                                 AS "说明"
       ,bill.fk_crrc_title                                                                                       AS "主题"
       ,bill.fk_crrc_bmgz                                                                                        AS "编码规则取号"
       ,case bill.fk_crrc_iszhuanjiao WHEN 0 THEN '是' WHEN 1 THEN '否' end                                        AS "是否转交编辑"
       ,create_user.fnumber                                                                                      AS "创建人工号"
       ,create_user.FTRUENAME                                                                                    AS "创建人姓名"
       ,bill.fcreatetime                                                                                         AS "创建时间"
       ,modifier_user.fnumber                                                                                    AS "修改人工号"
       ,modifier_user.FTRUENAME                                                                                  AS "修改人姓名"
       ,bill.fmodifytime                                                                                         AS "修改时间"
       ,auditor_user.fnumber                                                                                     AS "批准人工号"
       ,auditor_user.FTRUENAME                                                                                   AS "批准人姓名"
       ,bill.fauditdate                                                                                          AS "批准时间"
FROM crrc_secd.tk_crrc_chgbill bill
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
LEFT JOIN crrc_sys.t_sec_user auditor_user
ON bill.fauditorid = auditor_user.FID