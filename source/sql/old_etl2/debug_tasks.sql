SELECT
    u.uda_pro_code AS "项目号",
    u.uda_car_code AS "车号",
    u.code AS "订单号",
    '调试' AS "文件类型",
    vt.planned_start_time AS "计划开始时间",
    vt.planned_finish_time AS "计划结束时间",
    vt.gid AS "id",
    vt.dispatch_code AS "派工单号",
    g.group_code as "班组编码",
    g.group_name as "班组名称",
    ml.code AS "工艺路线",
    ml.name AS "工艺名称",
    mo.op_code AS "工序编码",
    mo.op_name AS "工序名称",
    dis.gid as "调试项点的id",
    case
        when dis.chk_result = 7 then '合格'
        when dis.chk_result != 7
        and dis.IS_INDEPENDENCE_STEP = 1 then '合格'
        else '不合格'
    end as "调试项点状态"
FROM
    unimax_cg.umpp_plan_order u
    INNER JOIN unimax_cg.uex_vtrack_record vt ON vt.is_delete = 0
    AND vt.order_code = u.code
    LEFT JOIN unimax_cg.mbf_labour_group g ON vt.labour_group_gid = g.gid
    AND g.is_delete = 0
    LEFT JOIN unimax_cg.mbf_route_operation mo ON mo.gid = vt.op_gid
    AND mo.is_delete = 0
    LEFT JOIN unimax_cg.mbf_route_line ml ON ml.is_delete = 0
    AND ml.gid = mo.route_gid
    INNER JOIN unimax_cg.debug_file_operation d ON u.gid = d.order_gid
    AND d.op_id = vt.op_gid
    AND d.is_delete = 0
    LEFT JOIN unimax_cg.debugging_file_operation dd ON dd.is_delete = 0
    AND dd.order_file_id = d.gid
    LEFT JOIN unimax_cg.debugging_item di ON di.file_id = dd.gid
    AND di.is_delete = 0
    LEFT JOIN unimax_cg.debugging_item_step dis ON dis.item_id = di.gid
    AND dis.is_delete = 0
WHERE
    u.is_delete = 0
    AND vt.planned_start_time >= SYSDATE - 90
UNION ALL
SELECT
    u.uda_pro_code AS "项目号",
    u.uda_car_code AS "车号",
    u.CODE AS "订单号",
    '校线' AS "文件类型",
    vt.planned_start_time AS "计划开始时间",
    vt.planned_finish_time AS "计划结束时间",
    vt.gid AS "id",
    vt.dispatch_code AS "派工单号",
    g.group_code as "班组编码",
    g.group_name as "班组名称",
    ml.CODE AS "工艺路线",
    ml.NAME AS "工艺名称",
    mo.op_code AS "工序编码",
    mo.op_name AS "工序名称",
    fs.gid as "调试项点的id",
    case fs.EXP_RESULT
        when '7' then '合格'
        else '不合格'
    end as "调试项点状态"
FROM
    unimax_cg.umpp_plan_order u
    INNER JOIN unimax_cg.uex_vtrack_record vt ON vt.IS_DELETE = 0
    AND vt.order_code = u.code
    LEFT JOIN unimax_cg.mbf_labour_group g ON vt.labour_group_gid = g.gid
    AND g.is_delete = 0
    LEFT JOIN unimax_cg.MBF_ROUTE_LINE ml ON ml.gid = vt.ROUTE_GID
    LEFT JOIN unimax_cg.mbf_route_operation mo ON mo.gid = vt.OP_GID
    AND mo.IS_DELETE = 0
    INNER JOIN unimax_cg.ALIGNMENT_FILE_REL frs ON vt.ROUTE_GID = frs.ROUTE_LINE_GID
    AND frs.is_delete = 0
    AND frs.FLAG = '2'
    AND vt.OP_GID = frs.OP_ID
    INNER JOIN unimax_cg.ALIGNMENT_FILE_SITE fs ON fs.is_delete = 0
    AND fs.FILE_REL_ID = frs.gid
WHERE
    u.is_delete = 0
    AND vt.planned_start_time >= SYSDATE - 90