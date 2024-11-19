SELECT
	p.fnumber AS 项目号,
	p_gz.fnumber AS 跟踪号,
	p_jc.fnumber AS 节车号,
	o.fnumber AS 工序号,
	t_ens.CFOperationName AS 工序名称,
	w.fname_l2 AS 工作中心,
	SUM(t_ens.CFQuateTime) AS 定额工时合并,
	c.fname_l2 AS 成本中心
FROM
	ZJEAS7.ct_bcp_ProStdTime t
LEFT JOIN ZJEAS7.T_MM_Project p ON t.CFProjectID = p.fid
LEFT JOIN ZJEAS7.T_MM_TrackNumber p_gz ON t.CFStartTrackID = p_gz.fid
LEFT JOIN ZJEAS7.T_PRO_ProjectJCH p_jc ON t.CFProjectJCHID = p_jc.fid
LEFT JOIN ZJEAS7.CT_BCP_ProStdTimeOpEntrys t_ens ON t.fid = t_ens.fparentid
LEFT JOIN ZJEAS7.T_ORG_CostCenter c ON t_ens.CFCostCenterID = c.fid
LEFT JOIN ZJEAS7.T_MM_WorkCenter w ON t_ens.CFWorkCenterID = w.fid 
LEFT JOIN ZJEAS7.T_MM_Operation o ON t_ens.CFOperationID = o.fid
WHERE
	c.fname_l2 = '城轨总成车间'
	AND p_jc.fnumber IS NOT NULL
GROUP BY
	p.fnumber,
	p_gz.fnumber,
	p_jc.fnumber,
	o.fnumber,
	t_ens.CFOperationName,
	w.fname_l2,
	c.fname_l2