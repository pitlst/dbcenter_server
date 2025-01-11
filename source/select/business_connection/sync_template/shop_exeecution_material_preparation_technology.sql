SELECT
       bill.FID AS "id",
       bill.FEntryId AS "对应单据id",
       mis.fnumber AS "图号",
       mis.fname AS "名称",
       mis.fk_crrc_model AS "规格",
       mis.fk_crrc_cz AS "材质",
       bill.fk_crrc_workptext AS "工序步骤",
       bill.fk_crrc_banzutxt AS "班组(文本)",
       bill.fk_crrc_finishtime AS "完成时间",
       bill.fk_crrc_finishnode_e3 AS "完成时间节点",
       bill.fk_crrc_orgunit AS "收件单位",
       bill.fk_crrc_countqty AS "总数量"
FROM
       crrc_secd.tk_crrc_backprocess bill
       LEFT JOIN crrc_secd.tk_crrc_misproduct mis ON bill.fk_crrc_materialid = mis.FID
WHERE