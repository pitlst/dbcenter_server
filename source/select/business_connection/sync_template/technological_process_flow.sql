SELECT
       bill.fid AS "id",
       bill.FEntryId AS "子单据id",
       _user.fnumber AS "操作人工号",
       _user.FTRUENAME AS "操作人姓名",
       bill.fk_crrc_flowtime AS "操作时间",
       bill.fk_crrc_flowact AS "操作动作",
       bill.fk_crrc_fpusernums AS "被分配人组编码",
       bill.fk_crrc_fpusernames AS "被分配人组名称",
       bill.fk_crrc_options AS "意见"
FROM
       crrc_secd.tk_crrc_flowentry bill
       LEFT JOIN crrc_sys.t_sec_user _user ON bill.fk_crrc_flowuser = _user.FID
WHERE