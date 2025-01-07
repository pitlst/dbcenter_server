SELECT  bill.fid         AS "id"
       ,bill.fmodifytime AS "修改时间"
FROM crrc_secd.tk_crrc_craftchangebill bill
LEFT JOIN crrc_sys.t_sec_user _user
ON bill.fk_crrc_senduser = _user.FID