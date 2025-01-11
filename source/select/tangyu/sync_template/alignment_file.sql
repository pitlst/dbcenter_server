SELECT
	site.GID AS "id",
	to_char (site.MODIFY_DATE, 'yyyy-MM-dd') AS "修改时间",
	p.uda_pro_code AS "项目号",
	p.uda_car_code AS "车号",
	p.uda_singercar_code AS "节车号",
	rel_site.OP_ID AS "工序id",
	op.OP_NAME AS "工序名称",
	site.WIRE_NUM AS "线号",
	site.WIRE_DIAMETER AS "线径",
	site.COLOUR AS "颜色",
	site.LINE_TYPE AS "线型",
	site.SHEET_NAME AS "文件sheet名称",
	site.SHEET_INDEX AS "文件sheet序号",
	site.POSITION_ONE AS "起始位置",
	site.POSITION_TWO AS "终止位置",
	site.DEVICE_ONE AS "连接点1",
	site.DEVICE_TWO AS "连接点2",
	site.POINT_ONE AS "点位1",
	site.POINT_TWO AS "点位2",
	site.REMARKS_ONE AS "说明1",
	site.REMARKS_TWO AS "说明2",
	site.QUESTION_ONE AS "问题1",
	site.QUESTION_TWO AS "问题2",
	site.QUESTION_THR AS "问题3",
	site.QUESTION_FOUR AS "问题4",
	site.POSITION_ONE_USER AS "人员1",
	site.POSITION_TWO_USER AS "人员2",
	site.REMARKS AS "备注",
	site.UDA1 AS "返工确认",
	site.REMARK_TECHNOLOGY AS "工艺备注",
	site.EXP_RESULT AS "试验结果",
	CASE
		WHEN site.QUESTION_ONE IS NOT NULL THEN site.POSITION_ONE || site.DEVICE_ONE || site.QUESTION_ONE
		ELSE ''
	END || CASE
		WHEN site.QUESTION_TWO IS NOT NULL THEN ',' || site.POSITION_ONE || site.DEVICE_ONE || site.POINT_ONE || site.WIRE_NUM || site.QUESTION_TWO
		ELSE ''
	END || CASE
		WHEN site.QUESTION_THR IS NOT NULL THEN ',' || site.POSITION_TWO || site.DEVICE_TWO || site.POINT_TWO || site.WIRE_NUM || site.QUESTION_THR
		ELSE ''
	END || CASE
		WHEN site.QUESTION_FOUR IS NOT NULL THEN ',' || site.POSITION_TWO || site.DEVICE_TWO || site.QUESTION_FOUR
		ELSE ''
	END AS "异常描述",
	ROW_NUMBER() OVER (
		PARTITION BY
			p.uda_pro_code,
			p.uda_car_code,
			p.uda_singercar_code,
			rel_site.OP_ID,
			site.DEVICE_ONE,
			site.DEVICE_TWO
		ORDER BY
			site.EXP_RESULT
	) AS "排序"
FROM
	unimax_cg.alignment_file_site site
	LEFT JOIN unimax_cg.alignment_file_rel rel_site ON site.FILE_REL_ID = rel_site.GID
	LEFT OUTER JOIN mbf_route_operation op ON rel_site.op_id = op.gid
	AND op.is_delete = 0
	LEFT OUTER JOIN mbf_route_line l ON op.route_gid = l.gid
	AND l.is_delete = 0
	LEFT OUTER JOIN umpp_plan_order p ON l.plan_order_gid = p.gid
	AND p.is_delete = 0
WHERE
	site.IS_ACTIVE = 0
	AND site.IS_DELETE = 0
	AND (
		site.EXP_RESULT = 1
		OR site.EXP_RESULT IS NULL
	)
	AND to_char (site.MODIFY_DATE, 'yyyy-MM') >= to_char (SYSDATE, 'yyyy-MM')
	AND