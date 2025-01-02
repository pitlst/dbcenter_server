SELECT mysql.udaProCode        AS "项目号",
       mysql.udaCarCode        AS "车号",
       '调试'                  AS "文件类型",
       mysql.orderCode         AS "订单号",
       mysql.dispatchCode      AS "派工单号",
       mysql.id                AS "id",
       mysql.plannedStartTime  AS "计划开始时间",
       mysql.plannedFinishTime AS "计划结束时间",
       mysql.lineCode          AS "工艺路线",
       mysql.lineName          AS "工艺名称",
       mysql.opCode            AS "工序编码",
       mysql.opName            AS "工序名称",
       mysql.fileCode          AS "文件编码",
       mysql.fileName          AS "文件名称",
       mysql.carTypeCode       AS "carTypeCode",
       mysql.publishGid        AS "publishGid",
       CASE
           WHEN mysql.completePercent = 0 THEN
               0
           WHEN mysql.completePercent = 100 THEN
               100
           WHEN mysql.completePercent
               BETWEEN 99
               AND 100 THEN
               99
           WHEN mysql.completePercent
               BETWEEN 0
               AND 1 THEN
               1
           ELSE to_number(to_char(mysql.completePercent, 'fm99'))
           END                 AS "完成度"
FROM (SELECT u.uda_pro_code                              AS udaProCode,
             u.uda_car_code                              AS udaCarCode,
             u.code                                      AS orderCode,
             vt.planned_start_time                       AS plannedStartTime,
             vt.planned_finish_time                      AS plannedFinishTime,
             vt.gid                                      AS id,
             vt.dispatch_code                            AS dispatchCode,
             ml.code                                     AS lineCode,
             ml.name                                     AS lineName,
             mo.op_code                                  AS opCode,
             mo.op_name                                  AS opName,
             d.file_code                                 AS fileCode,
             d.file_name                                 AS fileName,
             d.car_type_code                             AS carTypeCode,
             dd.gid                                      AS publishGid,
             decode(count(dis.gid),
                    0,
                    0,
                    count(
                            CASE
                                WHEN dis.chk_result = 7 THEN
                                    1
                                WHEN dis.chk_result != 7
                                AND dis.IS_INDEPENDENCE_STEP = 1 THEN
                            1
                            ELSE NULL
                            END) / count(dis.gid)) * 100 AS completePercent
      FROM unimax_cg.umpp_plan_order u
               INNER JOIN unimax_cg.uex_vtrack_record vt
                          ON vt.is_delete = 0
                              AND vt.order_code = u.code

               LEFT JOIN unimax_cg.mbf_route_operation mo
                         ON mo.gid = vt.op_gid
                             AND mo.is_delete = 0
               LEFT JOIN unimax_cg.mbf_route_line ml
                         ON ml.is_delete = 0
                             AND ml.gid = mo.route_gid
               INNER JOIN unimax_cg.debug_file_operation d
                          ON u.gid = d.order_gid
                              AND d.op_id = vt.op_gid
                              AND d.is_delete = 0
               LEFT JOIN unimax_cg.debugging_file_operation dd
                         ON dd.is_delete = 0
                             AND dd.order_file_id = d.gid
               LEFT JOIN unimax_cg.debugging_item di
                         ON di.file_id = dd.gid
                             AND di.is_delete = 0
               LEFT JOIN unimax_cg.debugging_item_step dis
                         ON dis.item_id = di.gid
                             AND dis.is_delete = 0
      WHERE u.is_delete = 0
        AND vt.planned_start_time >= SYSDATE - 180

      GROUP BY u.uda_pro_code, u.uda_car_code, u.code, vt.planned_start_time, vt.planned_finish_time, vt.gid, vt.dispatch_code, ml.code, ml.name,
               mo.op_code, mo.op_name, d.file_code, d.file_name, d.gid, d.car_type_code, dd.gid) mysql
UNION ALL

SELECT mysql.udaProCode        AS "udaProCode",
       mysql.udaCarCode        AS "udaCarCode",
       '校线'                  AS "type",
       mysql.orderCode         AS "orderCode",
       mysql.dispatchCode      AS "dispatchCode",
       mysql.id                AS "id",
       mysql.plannedStartTime  AS "plannedStartTime",
       mysql.plannedFinishTime AS "plannedFinishTime",
       mysql.lineCode          AS "lineCode",
       mysql.lineName          AS "lineName",
       mysql.opCode            AS "opCode",
       mysql.opName            AS "opName",
       ''                      AS "fileCode",
       ''                      AS "fileName",
       ''                      AS "carTypeCode",
       mysql.publishGid        AS "publishGid",
       CASE
           WHEN mysql.completePercent = 0 THEN
               0
           WHEN mysql.completePercent = 100 THEN
               100
           WHEN mysql.completePercent
               BETWEEN 99
               AND 100 THEN
               99
           WHEN mysql.completePercent
               BETWEEN 0
               AND 1 THEN
               1
           ELSE to_number(to_char(mysql.completePercent, 'fm99'))
           END                 AS "prasePercent"
FROM (SELECT u.uda_pro_code                                 AS udaProCode,
             u.uda_car_code                                 AS udaCarCode,
             u.CODE                                         AS orderCode,
             vt.planned_start_time                          AS plannedStartTime,
             vt.planned_finish_time                         AS plannedFinishTime,
             vt.gid                                         AS id,
             vt.dispatch_code                               AS dispatchCode,
             ml.CODE                                        AS lineCode,
             ml.NAME                                        AS lineName,
             mo.op_code                                     AS opCode,
             mo.op_name                                     AS opName,
             frs.gid                                        AS publishGid,
             decode(count(fs.gid),
                    0,
                    0,
                    count(
                            CASE
                                WHEN fs.EXP_RESULT = '7' THEN
                                    1
                                ELSE NULL
                                END) / count(fs.gid)) * 100 AS completePercent
      FROM unimax_cg.umpp_plan_order u
               INNER JOIN unimax_cg.uex_vtrack_record vt
                          ON vt.IS_DELETE = 0
                              AND vt.order_code = u.code
               LEFT JOIN unimax_cg.MBF_ROUTE_LINE ml
                         ON ml.gid = vt.ROUTE_GID
               LEFT JOIN unimax_cg.mbf_route_operation mo
                         ON mo.gid = vt.OP_GID
                             AND mo.IS_DELETE = 0
               INNER JOIN unimax_cg.ALIGNMENT_FILE_REL frs
                          ON vt.ROUTE_GID = frs.ROUTE_LINE_GID
                              AND frs.is_delete = 0
                              AND frs.FLAG = '2'
                              AND vt.OP_GID = frs.OP_ID
               INNER JOIN unimax_cg.ALIGNMENT_FILE_SITE fs
                          ON fs.is_delete = 0
                              AND fs.FILE_REL_ID = frs.gid
      WHERE u.is_delete = 0
        AND vt.planned_start_time >= SYSDATE - 180


      GROUP BY u.uda_pro_code, u.uda_car_code, u.code, vt.planned_start_time, vt.planned_finish_time, vt.gid, vt.dispatch_code, ml.code, ml.name,
               mo.op_code, mo.op_name, frs.gid) mysql
ORDER BY "工序编码" ASC