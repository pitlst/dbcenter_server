SELECT
       bill.FID AS "id",
       bill.FEntryId AS "子单据id",
       bill.FSeq AS "排序",
       bill.fk_crrc_begintrack AS "开始车号",
       bill.fk_crrc_endtrack AS "结束车号",
       bill.fk_crrc_backbill_jch AS "节车号",
       bill.fk_crrc_changecar_e2 AS "执行车次",
       bill.fk_crrc_serialno AS "序列号",
       bill.fk_crrc_craftnext AS "任务描述",
       mater.fnumber AS "物料编码",
       mater.FNAME AS "物料名称",
       bill.fk_crrc_baseunit_e1 AS "计量单位",
       bill.fk_crrc_qty AS "物料数量",
       tools.fnumber AS "工具编码",
       tools.fname AS "工具名称",
       tools.fk_crrc_model AS "工具规格",
       bill.fk_crrc_worktime AS "返工工时（分钟）",
       bill.fk_crrc_finishdate AS "要求完成时间",
       bill.fk_crrc_finishnode AS "完成时间节点"
FROM
       crrc_secd.tk_crrc_backworkentry bill
       LEFT JOIN crrc_sys.t_bd_material mater ON bill.fk_crrc_materiel = mater.FID
       LEFT JOIN crrc_secd.tk_crrc_tools tools ON bill.fk_crrc_tools = tools.FID
WHERE