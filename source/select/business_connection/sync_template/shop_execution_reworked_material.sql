SELECT
       bill.FID AS "id",
       bill.FEntryId AS "对应单据id",
       mater.fnumber AS "物料编码",
       mater.FNAME AS "物料名称",
       mater.fmodel AS "规格型号",
       CASE sur.FPrecisionType
              WHEN 1 THEN '四舍五入'
              WHEN 2 THEN '舍位'
              WHEN 3 THEN '进位'
       END AS "精度处理",
       CASE sur.FConvertType
              WHEN 1 THEN '固定'
              WHEN 2 THEN '浮动'
       END AS "换算类型",
       sur.FPrecision AS "单位精度",
       bill.fk_crrc_qty AS "数量",
       bill.fk_crrc_remark_e2 AS "备注"
FROM
       crrc_secd.tk_crrc_backmaterial bill
       LEFT JOIN crrc_sys.t_bd_material mater ON bill.fk_crrc_material = mater.FID
       LEFT JOIN crrc_sys.T_bd_Measureunit sur ON bill.fk_crrc_unit = sur.FID
WHERE