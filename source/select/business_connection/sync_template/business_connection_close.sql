SELECT
       bill.fid AS "id",
       bill.fk_crrc_billno AS "单据编号",
       bill.fk_crrc_chgbillnum AS "变更单编码",
       bill.fk_crrc_executebillno AS "执行单编号",
       bill.fk_crrc_chgbillname AS "变更单名称",
       org.FNAME AS "部门",
       bill.fk_crrc_deptname AS "部门名称",
       bill.fk_crrc_maindept AS "主送单位",
       bill.fk_crrc_maindeptname AS "主送单位名称",
       bill.fk_crrc_copyunit AS "抄送单位",
       bill.fk_crrc_copyunitname AS "抄送单位名称",
       bill.fk_crrc_begincarno AS "车号",
       bill.fk_crrc_projectjch AS "节车号",
       bill.fk_crrc_serialno AS "序列号",
       project.fnumber AS "项目编号",
       project.fname AS "项目基础资料名称",
       org1.FNAME AS "执行部门",
       bill.fk_crrc_exegroupcode AS "执行班组",
       bill.fk_crrc_exegroupname AS "执行班组名称",
       CASE bill.fk_crrc_isfirstcaryes
              WHEN 0 THEN '是'
              WHEN 1 THEN '否'
       END AS "是否完成首次确认",
       bill.fk_crrc_projectnum AS "项目号",
       bill.fk_crrc_projectname AS "项目名称",
       create_user.fnumber AS "创建人工号",
       create_user.FTRUENAME AS "创建人姓名",
       bill.fk_crrc_createtime AS "创建时间",
       CASE bill.fk_crrc_jobstatus
              WHEN 'A' THEN '发送中'
              WHEN 'B' THEN '已下发'
              WHEN 'C' THEN '执行完工'
              WHEN 'D' THEN '质检合格'
              WHEN 'E' THEN '质检不合格'
              WHEN 'F' THEN '作废中'
              WHEN 'G' THEN '作废'
              WHEN 'H' THEN '转交中'
       END AS "任务状态",
       CASE bill.fk_crrc_jobstatus
              WHEN 'change' THEN '变更执行单'
              WHEN 'back' THEN '返工执行单'
       END AS "任务来源",
       bill.fk_crrc_reportbill AS "关联报告编号",
       CASE bill.fk_crrc_billstatus
              WHEN 15 THEN '评审完毕'
              WHEN 14 THEN '关闭'
       END AS "状态",
       CASE bill.fk_crrc_transmit
              WHEN 'A' THEN '转交完成'
       END AS "转交",
       bill.fk_crrc_chgcause AS "变更原因",
       bill.fk_crrc_craftnext AS "工艺步骤",
       bill.fk_crrc_worktime AS "工时",
       bill.fk_crrc_finishdate AS "要求完成时间",
       bill.fk_crrc_finishnode AS "完成时间节点",
       CASE bill.fk_crrc_ischeck
              WHEN 0 THEN '是'
              WHEN 1 THEN '否'
       END AS "是否需要质检",
       CASE bill.fk_crrc_locksending
              WHEN 0 THEN '是'
              WHEN 1 THEN '否'
       END AS "锁定发送中",
       CASE bill.fk_crrc_issubmaterial
              WHEN 0 THEN '是'
              WHEN 1 THEN '否'
       END AS "是否退料",
       org2.FNAME AS "所属组织",
       execute_user.FNUMBER AS "执行人工号",
       execute_user.FTRUENAME AS "执行人名称",
       bill.fk_crrc_feedbackdate AS "执行时间",
       bill.fk_crrc_finishdesc AS "完工描述",
       quality_user.FNUMBER AS "质检员工号",
       quality_user.FTRUENAME AS "质检员名称",
       bill.fk_crrc_closedate AS "质检时间",
       bill.fk_crrc_closecause AS "质检说明"
FROM
       crrc_secd.tk_crrc_mesjob bill
       LEFT JOIN crrc_sys.t_org_org org ON bill.fk_crrc_deptid = org.FID
       LEFT JOIN crrc_secd.tk_crrc_projectmanager project ON bill.fk_crrc_projectid = project.fid
       LEFT JOIN crrc_sys.t_org_org org1 ON bill.fk_crrc_departmentdpt = org1.FID
       LEFT JOIN crrc_sys.t_sec_user create_user ON bill.fk_crrc_createperson = create_user.FID
       LEFT JOIN crrc_sys.t_org_org org2 ON bill.fk_crrc_orgfield = org2.FID
       LEFT JOIN crrc_sys.t_sec_user execute_user ON bill.fk_crrc_feedbackperson = execute_user.FID
       LEFT JOIN crrc_sys.t_sec_user quality_user ON bill.fk_crrc_closeuser = quality_user.FID
WHERE