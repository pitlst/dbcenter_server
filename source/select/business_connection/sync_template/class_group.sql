SELECT
       bill.FID AS "id",
       bill.fnumber AS "编码",
       bill.fname AS "班组",
       org.FNAME AS "库存组织",
       depart.FNAME AS "所属部门",
       user_user.FNUMBER AS "负责人工号",
       user_user.FTRUENAME AS "负责人姓名",
       CASE bill.fstatus
              WHEN 'A' THEN '暂存'
              WHEN 'B' THEN '已提交'
              WHEN 'C' THEN '已审核'
       END AS "单据状态",
       CASE bill.fenable
              WHEN 0 THEN '禁用'
              WHEN 1 THEN '可用'
       END AS "使用状态",
       creator_user.fnumber AS "创建人工号",
       creator_user.FTRUENAME AS "创建人姓名",
       bill.fcreatetime AS "创建时间",
       modifier_user.fnumber AS "修改人工号",
       modifier_user.FTRUENAME AS "修改人姓名",
       bill.fmodifytime AS "修改时间",
       CASE bill.fk_crrc_ischeck
              WHEN 0 THEN '是'
              WHEN 1 THEN '否'
       END AS "是否需要质检"
FROM
       crrc_secd.tk_crrc_classgroup bill
       LEFT JOIN crrc_sys.t_sec_user user_user ON bill.fk_crrc_userfield = user_user.FID
       LEFT JOIN crrc_sys.t_sec_user creator_user ON bill.fcreatorid = creator_user.FID
       LEFT JOIN crrc_sys.t_sec_user modifier_user ON bill.fmodifierid = modifier_user.FID
       LEFT JOIN crrc_sys.t_org_org org ON bill.fk_crrc_orgdeptid = org.FID
       LEFT JOIN crrc_sys.t_org_org depart ON bill.fk_crrc_department = org.FID
WHERE