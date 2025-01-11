SELECT
       bill.fid AS "id",
       bill.FEntryId AS "单据分录id",
       bill.FSeq AS "单据分录序号",
       mis.fnumber AS "图号",
       mis.fk_crrc_cz AS "材质",
       mis.fname AS "名称",
       mis.fk_crrc_model AS "规格",
       bill.fk_crrc_workptext_e5 AS "工序步骤",
       bill.fk_crrc_banzutxt AS "班组",
       bill.fk_crrc_orgunit AS "收件单位",
       bill.fk_crrc_finishtime AS "完成时间",
       bill.fk_crrc_finishnode_e5 AS "完成时间节点",
       bill.fk_crrc_countqty AS "总数量"
FROM
       crrc_secd.tk_crrc_chgbillstockentry bill
       LEFT JOIN crrc_secd.tk_crrc_misproduct mis ON bill.fk_crrc_materialid = mis.FID
WHERE