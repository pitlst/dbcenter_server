SELECT  fid                    AS "id"
       ,FEntryId               AS "子单据id"
       ,fk_crrc_blmaterialnum  AS "所属零部件编号"
       ,fk_crrc_pversion       AS "父版本"
       ,fk_crrc_blmaterialname AS "所属零部件名称"
       ,fk_crrc_materialnum    AS "零部件编码"
       ,fk_crrc_sversion       AS "子版本"
       ,fk_crrc_materialname   AS "零部件名称"
       ,fk_crrc_assseq         AS "装配序号"
       ,fk_crrc_quota          AS "原定额"
       ,fk_crrc_newquota       AS "新定额"
       ,fk_crrc_unit           AS "单位"
       ,fk_crrc_oldcraftflow   AS "旧工艺流程"
       ,fk_crrc_newcraftflow   AS "新工艺流程"
       ,fk_crrc_changecause    AS "变更原因"
       ,fk_crrc_description    AS "更改描述"
FROM crrc_secd.tk_crrc_craftchangeentry