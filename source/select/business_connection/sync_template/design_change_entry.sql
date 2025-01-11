SELECT
       bill.fid AS "id",
       bill.FEntryId AS "子单据id",
       bill.fk_crrc_owning_user AS "用户",
       bill.fk_crrc_material AS "零件号",
       bill.fk_crrc_materialname AS "零件名称",
       bill.fk_crrc_itemversion AS "物料版本号",
       bill.fk_crrc_entry_project AS "项目",
       bill.fk_crrc_datereleased AS "发放日期",
       bill.fk_crrc_textfield5 AS "tcmid"
FROM
       crrc_secd.tk_crrc_designchgentry bill
WHERE