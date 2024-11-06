SELECT  job.fk_crrc_billno        AS "单据编号"
       ,job.fk_crrc_chgbillnum    AS "变更单编号"
       ,job.fk_crrc_executebillno AS "执行单编号"
       ,job.fk_crrc_jobstatus     AS "单据状态"
       ,job.fk_crrc_chgbillname   AS "变更单名称"
       ,job.fk_crrc_deptid        AS "部门(库存组织)"
       ,job.fk_crrc_deptname      AS "部门名称"
       ,_user.fnumber             AS "创建人工号"
       ,_user.Ftruename           AS "创建人姓名"
       ,job.fk_crrc_maindept      AS "主送单位"
       ,job.fk_crrc_maindeptname  AS "主送单位名称"
       ,job.fk_crrc_copyunit      AS "抄送单位"
       ,job.fk_crrc_copyunitname  AS "抄送单位名称"
       ,job.fk_crrc_projectnum    AS "项目号"
       ,job.fk_crrc_projectname   AS "项目名称"
       ,job.fk_crrc_begincarno    AS "车号"
       ,job.fk_crrc_projectjch    AS "节车号"
       ,classgroup.fnumber        AS "执行班组编号"
       ,classgroup.fname          AS "执行班组名称"
       ,job.fk_crrc_ischeck       AS "是否需要质检"
       ,job.fk_crrc_createtime    AS "创建时间"
FROM crrc_secd.tk_crrc_mesjob job
LEFT JOIN crrc_secd.tk_crrc_classgroup classgroup
ON job.fk_crrc_exegroupcode = classgroup.fnumber
LEFT JOIN crrc_sys.t_sec_user _user
ON job.fk_crrc_createperson = _user.FID