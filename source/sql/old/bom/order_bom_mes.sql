	SELECT
		bom.gid AS "id",
		bom.ORDER_CODE AS "订单号",
		bom.MRL_GID AS "物料主键",
		bom.QANA AS "数量",
		bom.SUM_UNIT AS "计量单位",
		bom.OP_LINE_CODE AS "工艺路线编码",
		bom.OP_LINE_NAME AS "工艺路线名称",
		bom.PRO_MRL_CODE AS "部件产品编码",
		bom.PRO_MRL_NAME AS "部件产品名称",
		bom.BOM_CODE AS "bom编码",
		bom.BOM_NAME AS "bom名称",
		bom.PRO_CODE AS "项目编码",
		bom.PRO_NAME AS "项目名称",
		bom.OP_CODE AS "工序编码",
		bom.OP_NAME AS "工序名称",
		bom.MRL_CODE AS "物料编码",
		bom.MRL_NAME AS "物料名称",
		bom.DIS_TYPE AS "物料类别",
	CASE
			bom.IS_IMPORTANT 
			WHEN 0 THEN
			'是' 
			WHEN 1 THEN
			'否' 
		END AS "是否关重件",
		bom.CREATE_DATE AS "创建时间",
		bom.MODIFY_DATE AS "修改时间" 
	FROM
		UNIMAX_CG.MBB_ORDER_BOM bom
	WHERE
		bom.IS_DELETE = 0 AND bom.IS_ACTIVE = 0 