SELECT  t.gid                 AS "id"
       ,t.exception_type_name AS "异常类型名称"
       ,t.exception_type_code AS "异常类型编码"
       ,t.workcenter_code     AS "工作中心编码"
       ,t.workcenter_name     AS "工作中心"
       ,t.workcell_code       AS "工位编码"
       ,t.workcell_name       AS "工位"
       ,e.uda104              AS "班组名称"
       ,p.name                AS "车间名称"
       ,( CASE WHEN t.state = 0 THEN to_char ('待响应') WHEN t.state = 2 THEN to_char ('待处理') WHEN t.state = 4 THEN to_char ('待关闭') WHEN t.state = 8 THEN to_char ('已关闭') ELSE to_char ('未知') END ) AS "异常状态分类"
       ,t.state               AS "异常状态代号"
       ,t.exception_remark    AS "异常描述"
       ,t.uda5                AS "异常备注"
       ,t.op_name             AS "工序名称"
       ,t.op_code             AS "工序代号"
       ,t.car_code            AS "车号"
       ,t.singer_car_code     AS "节车号"
       ,t.plan_process_date   AS "计划处理时间/分钟"
       ,t.create_id           AS "创建人"
       ,t.create_date         AS "创建日期"
       ,t.launch_id           AS "发起人"
       ,t.launch_date         AS "发起日期"
       ,t.modify_id           AS "修改人"
       ,t.modify_date         AS "修改日期"
       ,t.response_id         AS "响应人"
       ,t.uda1c               AS "指定响应人"
       ,t.response_date       AS "响应时间"
       ,t.handl_id            AS "处理人"
       ,t.handl_date          AS "处理时间"
       ,t.close_id            AS "关闭人"
       ,t.close_date          AS "关闭时间"
       ,t.mrl_code            AS "产品编码"
       ,t.mrl_Name            AS "产品名称"
       ,t.workorder_code      AS "工单编码"
       ,t.code                AS "异常记录编码"
FROM unimax_cg.usm_exception_bill t
LEFT JOIN unimax_cg.PMBF_WORK_CELL wc
ON t.workcell_gid = wc.gid AND t.is_delete = 0 AND wc.is_delete = 0
LEFT JOIN unimax_cg.PMBF_WORK_CENTER pwc
ON wc.work_center_gid = pwc.gid AND pwc.is_delete = 0
LEFT JOIN unimax_cg.pmbf_Location p
ON pwc.location_gid = p.gid AND p.is_delete = 0
LEFT JOIN unimax_cg.pmbb_employee e
ON t.launch_id = e.code AND e.is_delete = 0 AND t.data_role = e.data_role
LEFT JOIN unimax_cg.MES_APS_EMPLOYEE m
ON t.uda1c = m.code AND e.is_delete = 0 AND t.data_role = m.data_role
WHERE t.IS_DELETE <> 1
AND t.class_flag = 0
AND SYSDATE - t.create_date <= 90