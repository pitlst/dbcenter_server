SELECT  v.project || v.car_code || v.singer_car_code || v.op_code || v.order_type || o.op_type || to_char (v.planned_start_time,'yyyy-mm-ddhh24:mi:ss') || to_char (v.planned_finish_time,'yyyy-mm-ddhh24:mi:ss') AS "id"
       ,MAX(v.DISPATCH_CODE)         AS "派工单id"
       ,MAX(v.WORK_CELL_GID)         AS "WORK_CELL_GID"
       ,MAX(v.singer_car_code)       AS "节车号"
       ,MAX(v.project)               AS "项目号"
       ,MAX(v.car_code)              AS "车号"
       ,COUNT(DISTINCT v.MRL_CODE)   AS "物料编码计数"
       ,MAX(v.MRL_CODE)              AS "产品编码"
       ,MAX(v.PRODU_NAME)            AS "产品名称"
       ,MAX(v.op_code)               AS "工序编码"
       ,MAX(o.op_name)               AS "工序名称"
       ,MAX(g.group_code)            AS "组编码"
       ,MAX(g.group_name)            AS "班组"
       ,MAX(v.order_type)            AS "订单类型"
       ,MAX(o.op_type)               AS "工序类型"
       ,MAX(v.planned_start_time)    AS "计划开始时间"
       ,MAX(v.planned_finish_time)   AS "计划结束时间"
       ,MAX(v.ACTUAL_BEGIN_DATE)     AS "实际开始时间"
       ,MAX(v.ACTUAL_END_DATE)       AS "实际结束时间"
       ,MAX(v.produ_place)           AS "produPlace"
       ,MAX(v.stage_position)        AS "台位"
       ,MAX(v.work_persion)          AS "工作人ID"
       ,MAX(pe.name)                 AS "项目名称"
       ,MAX(v.ISSUED_CHENK_POSITION) AS "issuedChenkPosition"
       ,MAX(v.ISSUED_CHENK_DATE)     AS "issuedChenkDate"
       ,MAX(v.ISSUED_CHENK_STATE)    AS "issuedChenkState"
       ,MAX(v.SEND_REQ_MRL)          AS "物料需求"
       ,MAX(v.DIS_CODE_STATE)        AS "派工单状态"
       ,MAX(v.uda_transmit)          AS "下达状态"
       ,MAX(pl.NAME)                 AS "车间"
       ,MAX(wc.NAME)                 AS "产线"
       ,MAX(cell.code)               AS "cellCode"
       ,MAX(cell.name)               AS "工段"
       ,MAX(s.std_time)              AS "工序工时时长"
       ,v.is_man_hour_rpt            AS "是否消耗工时"
       ,COUNT(f.state)               AS "有关齐套统计总数"
       ,MAX(v.REMARK)                AS "备注"
       ,MAX(v.UDA_OUT_STATE)         AS "委外状态"
FROM unimax_cg.uex_vtrack_record v
INNER JOIN unimax_cg.mbf_route_operation o
ON v.op_gid = o.gid AND o.is_delete = 0
LEFT JOIN unimax_cg.mbf_labour_group g
ON v.labour_group_gid = g.gid AND g.is_delete = 0
LEFT JOIN unimax_cg.uda_project_edit pe
ON v.project = pe.code AND pe.is_delete = 0
LEFT JOIN unimax_cg.PMBF_WORK_CELL cell
ON o.uda_cute_sta = cell.gid AND cell.is_delete = 0
INNER JOIN unimax_cg.pmbf_work_center wc
ON cell.WORK_CENTER_GID = wc.gid AND wc.is_delete = 0
LEFT JOIN unimax_cg.PMBF_LOCATION pl
ON wc.LOCATION_GID = pl.GID AND pl.IS_DELETE = 0
LEFT JOIN unimax_cg.EAS_MRL_FULLY_DATA f
ON f.order_code = v.order_code AND f.op_code = v.op_code AND f.IS_DELETE = 0
LEFT JOIN unimax_cg.uex_standard_man_hour s
ON v.project = s.pro_code AND v.car_code = s.car_code AND nvl (v.singer_car_code, '#singer_car_code') = nvl (s.single_car_code, '#singer_car_code') AND v.op_code = s.op_code AND s.is_delete = 0
WHERE v.is_delete <> 1
AND v.dis_code_state <> 5
AND v.repeat_id is null
AND v.program_name is null
AND v.uda_out_state <> 1
AND SYSDATE - planned_start_time <= 90
-- AND v.planned_start_time >= to_date (
-- to_char (sysdate, 'yyyy-mm-dd') || '00:00:01',
-- 'YYYY-MM-DD HH24:MI:SS'
-- )
-- AND v.planned_start_time <= to_date (
-- to_char (sysdate, 'yyyy-mm-dd') || '23:59:59',
-- 'YYYY-MM-DD HH24:MI:SS'
-- ) 
GROUP BY  v.project
         ,v.car_code
         ,v.singer_car_code
         ,v.op_code
         ,v.order_type
         ,v.is_man_hour_rpt
         ,o.op_type
         ,to_char (v.planned_start_time,'yyyy-mm-ddhh24:mi:ss')
         ,to_char (v.planned_finish_time,'yyyy-mm-ddhh24:mi:ss')