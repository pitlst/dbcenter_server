SELECT  bill.FID                 AS "id"
       ,bill.FEntryId            AS "子单据id"
       ,bill.FSeq                AS "排序"
       ,_user.fnumber            AS "审核人工号"
       ,_user.FTRUENAME          AS "审核人姓名"
       ,bill.fk_crrc_enauditdate AS "审核日期"
FROM crrc_secd.tk_crrc_auditentry bill
LEFT JOIN crrc_sys.t_sec_user _user
ON bill.fk_crrc_enauditor = _user.FID