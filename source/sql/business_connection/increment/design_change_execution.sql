SELECT  bill.FId         AS "id"
       ,bill.fmodifytime AS "修改时间"
       ,bill.fauditdate  AS "批准时间"
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