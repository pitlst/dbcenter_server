SELECT
       bill.fid AS "id",
       bill.fbillno AS "变更单编码",
       bill.fk_crrc_chgname AS "变更单名称",
       bill.fk_crrc_senddate AS "发放日期",
       bill.fk_crrc_project AS "项目号",
       bill.fk_crrc_user AS "用户",
       bill.fk_crrc_chgtype AS "变更类型",
       bill.fk_crrc_sendunit AS "发送单位",
       bill.fk_crrc_remark AS "备注",
       bill.fk_crrc_issplit AS "是否拆分",
       bill.fmodifytime AS "修改时间"
FROM
       crrc_secd.tk_crrc_designchgcenter bill
WHERE