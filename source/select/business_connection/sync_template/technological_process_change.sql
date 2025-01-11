SELECT
       bill.fid AS "id",
       bill.FEntryId AS "子单据id",
       bill.fk_crrc_blmaterialnum AS "所属零部件编号",
       bill.fk_crrc_pversion AS "父版本",
       bill.fk_crrc_blmaterialname AS "所属零部件名称",
       bill.fk_crrc_materialnum AS "零部件编码",
       bill.fk_crrc_sversion AS "子版本",
       bill.fk_crrc_materialname AS "零部件名称",
       bill.fk_crrc_assseq AS "装配序号",
       bill.fk_crrc_quota AS "原定额",
       bill.fk_crrc_newquota AS "新定额",
       bill.fk_crrc_unit AS "单位",
       bill.fk_crrc_oldcraftflow AS "旧工艺流程",
       bill.fk_crrc_newcraftflow AS "新工艺流程",
       bill.fk_crrc_changecause AS "变更原因",
       bill.fk_crrc_description AS "更改描述"
FROM
       crrc_secd.tk_crrc_craftchangeentry bill
WHERE