SELECT
       bill.FID AS "id",
       bill.FEntryId AS "子单据id",
       bill.FSeq AS "排序",
       _user.fnumber AS "经办人工号",
       _user.FTRUENAME AS "经办人姓名",
       bill.fk_crrc_enhandlerdate AS "经办日期",
       bill.fk_crrc_enhandlerphone AS "联系方式"
FROM
       crrc_secd.tk_crrc_back_handleentry bill
       LEFT JOIN crrc_sys.t_sec_user _user ON bill.fk_crrc_enhandler = _user.FID
WHERE