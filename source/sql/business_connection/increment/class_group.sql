SELECT  bill.FID         AS "id"
       ,bill.fmodifytime AS "修改时间"
FROM crrc_secd.tk_crrc_classgroup bill
LEFT JOIN crrc_sys.t_sec_user user_user
ON bill.fk_crrc_userfield = user_user.FID
LEFT JOIN crrc_sys.t_sec_user creator_user
ON bill.fcreatorid = creator_user.FID
LEFT JOIN crrc_sys.t_sec_user modifier_user
ON bill.fmodifierid = modifier_user.FID
LEFT JOIN crrc_sys.t_org_org org
ON bill.fk_crrc_orgdeptid = org.FID
LEFT JOIN crrc_sys.t_org_org depart
ON bill.fk_crrc_department = org.FID