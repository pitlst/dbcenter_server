SELECT
	t.fid AS fid,
	p.fname_l2 AS "项目名称",
	p.fnumber AS "项目号",
	p_gzh.fnumber AS "跟踪号",
	p_jch.fnumber AS "节车号",
	p_ent.cfcostcentername AS "成本中心名称",
	p_bz.fname_l2 AS "班组名称",
	p_gx.fnumber AS "工序号",
	p_gx.fname_l2 AS "工序名称",
-- 	p_ents.CFBomDeSum AS "数量",
	floor(SUM(p_ents.CFQuateTime)) AS "工序定额工时" 
FROM
	ods_ct_bcp_prostdtime t
	LEFT JOIN ods_t_mm_project p ON t.cfprojectid = p.fid
	LEFT JOIN ods_t_pro_projectjch p_jch ON p_jch.fid = t.cfprojectjchid
	LEFT JOIN ods_ct_bcp_prostdtimeentry p_ent ON t.fid = p_ent.fparentid
	LEFT JOIN ods_ct_bcp_prostdtimeopentrys p_ents ON t.fid = p_ents.fparentid
	LEFT JOIN ods_t_mm_workcenter p_bz ON p_ents.CFWorkCenterID = p_bz.fid
	LEFT JOIN ods_t_mm_operation p_gx ON p_ents.cfoperationid = p_gx.fid
	LEFT JOIN ods_t_mm_tracknumber p_gzh ON t.cfstarttrackid = p_gzh.fid 
WHERE
	t.cfstorageorgunitid = "nXnw4GGNRh+AxZ/cMEMFpcznrtQ=" 
	AND p_ent.cfcostcentername = "城轨总成车间" 
-- 	AND p_bz.fname_l2 NOT LIKE 'P%' 
	AND t.cfprojectjchid IS NOT NULL 
	AND p_ents.CFBomDeSum != "0"
GROUP BY
	t.fid,
	p.fname_l2,
	p.fnumber,
	p_gzh.fnumber,
	p_jch.fnumber,
	p_ent.cfcostcentername,
	p_bz.fname_l2,
	p_gx.fnumber,
	p_gx.fname_l2