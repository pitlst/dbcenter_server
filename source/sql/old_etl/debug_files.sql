SELECT
	site.GID as "现场ID",
	rel_site.OP_ID as "工序id",
	op.OP_NAME as "工序名称",
	site.WIRE_NUM AS "现场线号",
	site.WIRE_DIAMETER AS "现场线径",
	site.COLOUR AS "现场线颜色",
	site.LINE_TYPE AS "现场线型",
	site.SCHEMATIC_DIAGRAM AS "现场原理图",
	site.SHEET_NAME AS "现场文件sheet名称",
	site.SHEET_INDEX AS "现场文件sheet序号",
	site.POSITION_ONE AS "现场位置1",
	site.POSITION_TWO AS "现场位置2",
	site.DEVICE_ONE AS "现场器件1",
	site.DEVICE_TWO AS "现场器件2",
	site.POINT_ONE AS "现场点位1",
	site.POINT_TWO AS "现场点位2",
	site.REMARKS_ONE AS "现场备注1",
	site.REMARKS_TWO AS "现场备注2",
	site.EXP_USER_GID AS "现场试验人员",
	site.POSITION_ONE_USER AS "现场位置1人员",
	site.POSITION_TWO_USER AS "现场位置2人员",
	site.REMARKS AS "现场备注",
	site.EXP_RESULT AS "现场实验结果"
FROM
	unimax_cg.alignment_file_site site
	left join unimax_cg.alignment_file_rel rel_site on site.FILE_REL_ID = rel_site.GID
	left join unimax_cg.mbf_route_operation op on rel_site.OP_ID = op.gid
where
	site.CREATE_DATE >= (SYSDATE-1) -- 最近