SELECT  bill.fid         AS "id"
       ,bill.fauditdate  AS "批准日期"
       ,bill.fmodifytime AS "修改时间"
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