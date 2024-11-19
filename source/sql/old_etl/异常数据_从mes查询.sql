SELECT
  p.exception_type_name AS "异常类型",
  p.EXCEPTION_TYPE_CODE AS "异常类型编码",
  -- p.exception_content_name AS "异常内容名称",
  p.exception_remark AS "异常描述",
  -- p.launch_id AS "发起人",
  -- p.state AS "异常状态",
  -- q."real_create_date" AS "创建日期_deal",
  -- p.workcenter_name AS "工作中心",
  -- p.workcell_name AS "工位",
  -- q."real_response_date" AS "响应日期_deal",
  -- p.plan_process_date AS "预计处理时间",
  -- p.create_id AS "创建人",
  -- p.modify_id AS "修改人",
  -- p.close_id AS "关闭人",
  -- p.plan_process_date * 60 AS "预计处理时间",
  p.project AS "项目",
  p.singer_car_code AS "节车号",
  p.car_code AS "车号",
  p.workcenter_name AS "工作中心",
  p.workcell_name AS "工位",
  p.op_name AS "工序名称",
  p.op_code as "工序编码",
  p.exception_remark AS "异常内容描述",
  p.uda1c AS "指定响应人",
  p.workcell_gid AS "工位主键",
  CASE
    WHEN p.state = 0 THEN to_char ('待响应')
    WHEN p.state = 2 THEN to_char ('待处理')
    WHEN p.state = 4 THEN to_char ('待关闭')
    WHEN p.state = 8 THEN to_char ('已关闭')
    ELSE to_char ('未知')
  END AS "异常状态分类",
  p.launch_id AS "发起人",
  p.launch_date AS "发起日期",
  p.create_date AS "创建日期",
  p.modify_date AS "修改日期",
  p.response_id AS "响应人",
  p.response_date AS "响应日期",
  p.handl_id AS "处理人",
  p.handl_date AS "处理日期",
  p.close_id AS "关闭人",
  p.close_date AS "关闭日期",
  t."未响应时长-秒" AS "未响应时长-秒",
  w."未处理时长-秒" AS "未处理时长-秒",
  e."未关闭时长-秒" AS "未关闭时长-秒",
  CASE
    WHEN trim(
      translate(nvl (p.plan_process_date, 'x'), '0123456789', ' ')
    ) is NULL THEN to_number (p.plan_process_date) * 60
    ELSE -1
  END AS "预计处理时间",
  CASE
    WHEN p.response_date IS NOT NULL
    AND least (t."未响应时长-秒", 2 * 60 * 60) = t."未响应时长-秒" THEN to_char ('及时')
    WHEN p.response_date IS NULL
    AND least (t."未响应时长-秒", 2 * 60 * 60) = t."未响应时长-秒" THEN to_char ('待响应')
    WHEN least (t."未响应时长-秒", 2 * 60 * 60) <> t."未响应时长-秒" THEN to_char ('未及时')
    ELSE to_char ('未知')
  END AS "响应状态",
  CASE
    WHEN p.handl_date IS NOT NULL
    AND least (w."未处理时长-秒", 2 * 60 * 60 + y."plan_process_date") = w."未处理时长-秒" THEN to_char ('及时')
    WHEN p.handl_date IS NULL
    AND least (w."未处理时长-秒", 2 * 60 * 60 + y."plan_process_date") = w."未处理时长-秒" THEN to_char ('待处理')
    WHEN least (w."未处理时长-秒", 2 * 60 * 60) <> w."未处理时长-秒" THEN to_char ('未及时')
    ELSE to_char ('未知')
  END AS "处理状态",
  CASE
    WHEN p.close_date IS NOT NULL
    AND least (e."未关闭时长-秒", 2 * 60 * 60) = e."未关闭时长-秒" THEN to_char ('及时')
    WHEN p.close_date IS NULL
    AND least (e."未关闭时长-秒", 2 * 60 * 60) = e."未关闭时长-秒" THEN to_char ('待关闭')
    WHEN least (e."未关闭时长-秒", 2 * 60 * 60) <> e."未关闭时长-秒" THEN to_char ('未及时')
    ELSE to_char ('未知')
  END AS "关闭状态"
FROM
  unimax_cg.usm_exception_bill p
  LEFT OUTER JOIN (
    SELECT
      p.gid AS "gid_new",
      CASE
        WHEN trim(
          translate(nvl (p.plan_process_date, 'x'), '0123456789', ' ')
        ) is NULL THEN to_number (p.plan_process_date) * 60
        ELSE -1
      END AS "plan_process_date"
    FROM
      unimax_cg.usm_exception_bill p
    WHERE
      IS_ACTIVE = 0
      AND IS_DELETE = 0
  ) y ON p.gid = y."gid_new"
  LEFT OUTER JOIN (
    SELECT
      q."gid_new" AS "gid_new",
      CASE
        WHEN (
          to_number (to_char (q."real_create_date", 'hh24')) BETWEEN 0 AND 12
        )
        AND (
          to_number (to_char (q."real_response_date", 'hh24')) BETWEEN 12 AND 24
        ) THEN (q."real_response_date" - q."real_create_date") * 24 * 60 * 60 - 90 * 60 - to_number (
          trunc (q."real_response_date", 'dd') - trunc (q."real_create_date", 'dd')
        ) * 16.5 * 60 * 60 - floor(
          to_number (
            trunc (q."real_response_date", 'dd') - trunc (q."real_create_date", 'dd') + to_number (to_char (q."real_create_date", 'D')) - 1
          ) / 7
        ) * 7.5 * 2 * 60 * 60
        WHEN (
          to_number (to_char (q."real_create_date", 'hh24')) BETWEEN 12 AND 24
        )
        AND (
          to_number (to_char (q."real_response_date", 'hh24')) BETWEEN 0 AND 12
        ) THEN (q."real_response_date" - q."real_create_date") * 24 * 60 * 60 + 90 * 60 - to_number (
          trunc (q."real_response_date", 'dd') - trunc (q."real_create_date", 'dd')
        ) * 16.5 * 60 * 60 - floor(
          to_number (
            trunc (q."real_response_date", 'dd') - trunc (q."real_create_date", 'dd') + to_number (to_char (q."real_create_date", 'D')) - 1
          ) / 7
        ) * 7.5 * 2 * 60 * 60
        ELSE (q."real_response_date" - q."real_create_date") * 24 * 60 * 60 - to_number (
          trunc (q."real_response_date", 'dd') - trunc (q."real_create_date", 'dd')
        ) * 16.5 * 60 * 60 - floor(
          to_number (
            trunc (q."real_response_date", 'dd') - trunc (q."real_create_date", 'dd') + to_number (to_char (q."real_create_date", 'D')) - 1
          ) / 7
        ) * 7.5 * 2 * 60 * 60
      END AS "未响应时长-秒"
    FROM
      unimax_cg.usm_exception_bill p
      LEFT OUTER JOIN (
        SELECT
          CASE
            WHEN to_number (to_char (p.create_date, 'D')) = 1 THEN to_date (
              to_char (p.create_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_number (to_char (p.create_date, 'D')) = 7 THEN to_date (
              to_char (p.create_date + 2, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.create_date, 'yyyymmddhh24miss') > (to_char (p.create_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.create_date, 'D')) = 6 THEN to_date (
              to_char (p.create_date + 3, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.create_date, 'yyyymmddhh24miss') > (to_char (p.create_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.create_date, 'D')) <> 6 THEN to_date (
              to_char (p.create_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.create_date, 'yyyymmddhh24miss') < (to_char (p.create_date, 'yyyyMMdd') || '083000') THEN to_date (
              to_char (p.create_date, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.create_date, 'yyyymmddhh24miss') > (to_char (p.create_date, 'yyyyMMdd') || '120000')
            AND to_char (p.create_date, 'yyyymmddhh24miss') < (to_char (p.create_date, 'yyyyMMdd') || '133000') THEN to_date (
              to_char (p.create_date, 'yyyyMMdd') || '133000',
              'yyyymmddhh24miss'
            )
            WHEN p.create_date IS NULL THEN to_date ('19360101133000', 'yyyymmddhh24miss')
            ELSE p.create_date
          END as "real_create_date",
          CASE
            WHEN to_number (to_char (p.response_date, 'D')) = 1 THEN to_date (
              to_char (p.response_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_number (to_char (p.response_date, 'D')) = 7 THEN to_date (
              to_char (p.response_date + 2, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.response_date, 'yyyymmddhh24miss') > (to_char (p.response_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.response_date, 'D')) = 6 THEN to_date (
              to_char (p.response_date + 3, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.response_date, 'yyyymmddhh24miss') > (to_char (p.response_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.response_date, 'D')) <> 6 THEN to_date (
              to_char (p.response_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.response_date, 'yyyymmddhh24miss') < (to_char (p.response_date, 'yyyyMMdd') || '083000') THEN to_date (
              to_char (p.response_date, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.response_date, 'yyyymmddhh24miss') > (to_char (p.response_date, 'yyyyMMdd') || '120000')
            AND to_char (p.response_date, 'yyyymmddhh24miss') < (to_char (p.response_date, 'yyyyMMdd') || '133000') THEN to_date (
              to_char (p.response_date, 'yyyyMMdd') || '133000',
              'yyyymmddhh24miss'
            )
            WHEN p.response_date IS NULL THEN (
              SELECT
                sysdate
              FROM
                dual
            )
            ELSE p.response_date
          END as "real_response_date",
          p.gid as "gid_new"
        FROM
          unimax_cg.usm_exception_bill p
        WHERE
          IS_ACTIVE = 0
          AND IS_DELETE = 0
      ) q ON p.gid = q."gid_new"
    WHERE
      IS_ACTIVE = 0
      AND IS_DELETE = 0
  ) t ON p.gid = t."gid_new"
  LEFT OUTER JOIN (
    SELECT
      q."gid_new" AS "gid_new",
      q."real_response_date" AS "real_response_date",
      CASE
        WHEN (
          to_number (to_char (q."real_response_date", 'hh24')) BETWEEN 0 AND 12
        )
        AND (
          to_number (to_char (q."real_handl_date", 'hh24')) BETWEEN 12 AND 24
        ) THEN (q."real_handl_date" - q."real_response_date") * 24 * 60 * 60 - 90 * 60 - to_number (
          trunc (q."real_handl_date", 'dd') - trunc (q."real_response_date", 'dd')
        ) * 16.5 * 60 * 60 - floor(
          to_number (
            trunc (q."real_handl_date", 'dd') - trunc (q."real_response_date", 'dd') + to_number (to_char (q."real_response_date", 'D')) - 1
          ) / 7
        ) * 7.5 * 2 * 60 * 60
        WHEN (
          to_number (to_char (q."real_response_date", 'hh24')) BETWEEN 12 AND 24
        )
        AND (
          to_number (to_char (q."real_handl_date", 'hh24')) BETWEEN 0 AND 12
        ) THEN (q."real_handl_date" - q."real_response_date") * 24 * 60 * 60 + 90 * 60 - to_number (
          trunc (q."real_handl_date", 'dd') - trunc (q."real_response_date", 'dd')
        ) * 16.5 * 60 * 60 - floor(
          to_number (
            trunc (q."real_handl_date", 'dd') - trunc (q."real_response_date", 'dd') + to_number (to_char (q."real_response_date", 'D')) - 1
          ) / 7
        ) * 7.5 * 2 * 60 * 60
        ELSE (q."real_handl_date" - q."real_response_date") * 24 * 60 * 60 - to_number (
          trunc (q."real_handl_date", 'dd') - trunc (q."real_response_date", 'dd')
        ) * 16.5 * 60 * 60 - floor(
          to_number (
            trunc (q."real_handl_date", 'dd') - trunc (q."real_response_date", 'dd') + to_number (to_char (q."real_response_date", 'D')) - 1
          ) / 7
        ) * 7.5 * 2 * 60 * 60
      END AS "未处理时长-秒"
    FROM
      unimax_cg.usm_exception_bill p,
      (
        SELECT
          CASE
            WHEN to_number (to_char (p.response_date, 'D')) = 1 THEN to_date (
              to_char (p.response_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_number (to_char (p.response_date, 'D')) = 7 THEN to_date (
              to_char (p.response_date + 2, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.response_date, 'yyyymmddhh24miss') > (to_char (p.response_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.response_date, 'D')) = 6 THEN to_date (
              to_char (p.response_date + 3, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.response_date, 'yyyymmddhh24miss') > (to_char (p.response_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.response_date, 'D')) <> 6 THEN to_date (
              to_char (p.response_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.response_date, 'yyyymmddhh24miss') < (to_char (p.response_date, 'yyyyMMdd') || '083000') THEN to_date (
              to_char (p.response_date, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.response_date, 'yyyymmddhh24miss') > (to_char (p.response_date, 'yyyyMMdd') || '120000')
            AND to_char (p.response_date, 'yyyymmddhh24miss') < (to_char (p.response_date, 'yyyyMMdd') || '133000') THEN to_date (
              to_char (p.response_date, 'yyyyMMdd') || '133000',
              'yyyymmddhh24miss'
            )
            ELSE p.response_date
          END as "real_response_date",
          CASE
            WHEN to_number (to_char (p.handl_date, 'D')) = 1 THEN to_date (
              to_char (p.handl_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_number (to_char (p.handl_date, 'D')) = 7 THEN to_date (
              to_char (p.handl_date + 2, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.handl_date, 'yyyymmddhh24miss') > (to_char (p.handl_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.handl_date, 'D')) = 6 THEN to_date (
              to_char (p.handl_date + 3, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.handl_date, 'yyyymmddhh24miss') > (to_char (p.handl_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.handl_date, 'D')) <> 6 THEN to_date (
              to_char (p.handl_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.handl_date, 'yyyymmddhh24miss') < (to_char (p.handl_date, 'yyyyMMdd') || '083000') THEN to_date (
              to_char (p.handl_date, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.handl_date, 'yyyymmddhh24miss') > (to_char (p.handl_date, 'yyyyMMdd') || '120000')
            AND to_char (p.handl_date, 'yyyymmddhh24miss') < (to_char (p.handl_date, 'yyyyMMdd') || '133000') THEN to_date (
              to_char (p.handl_date, 'yyyyMMdd') || '133000',
              'yyyymmddhh24miss'
            )
            WHEN p.handl_date IS NULL THEN (
              SELECT
                sysdate
              FROM
                dual
            )
            ELSE p.handl_date
          END as "real_handl_date",
          p.gid as "gid_new"
        FROM
          unimax_cg.usm_exception_bill p
        WHERE
          IS_ACTIVE = 0
          AND IS_DELETE = 0
      ) q
    WHERE
      p.gid = q."gid_new"
      AND IS_ACTIVE = 0
      AND IS_DELETE = 0
  ) w ON p.gid = w."gid_new"
  AND w."real_response_date" IS NOT NULL
  LEFT OUTER JOIN (
    SELECT
      q."gid_new" AS "gid_new",
      q."real_handl_date" AS "real_handl_date",
      CASE
        WHEN (
          to_number (to_char (q."real_handl_date", 'hh24')) BETWEEN 0 AND 12
        )
        AND (
          to_number (to_char (q."real_close_date", 'hh24')) BETWEEN 12 AND 24
        ) THEN (q."real_close_date" - q."real_handl_date") * 24 * 60 * 60 - 90 * 60 - to_number (
          trunc (q."real_close_date", 'dd') - trunc (q."real_handl_date", 'dd')
        ) * 16.5 * 60 * 60 - floor(
          to_number (
            trunc (q."real_close_date", 'dd') - trunc (q."real_handl_date", 'dd') + to_number (to_char (q."real_handl_date", 'D')) - 1
          ) / 7
        ) * 7.5 * 2 * 60 * 60
        WHEN (
          to_number (to_char (q."real_handl_date", 'hh24')) BETWEEN 12 AND 24
        )
        AND (
          to_number (to_char (q."real_close_date", 'hh24')) BETWEEN 0 AND 12
        ) THEN (q."real_close_date" - q."real_handl_date") * 24 * 60 * 60 + 90 * 60 - to_number (
          trunc (q."real_close_date", 'dd') - trunc (q."real_handl_date", 'dd')
        ) * 16.5 * 60 * 60 - floor(
          to_number (
            trunc (q."real_close_date", 'dd') - trunc (q."real_handl_date", 'dd') + to_number (to_char (q."real_handl_date", 'D')) - 1
          ) / 7
        ) * 7.5 * 2 * 60 * 60
        ELSE (q."real_close_date" - q."real_handl_date") * 24 * 60 * 60 - to_number (
          trunc (q."real_close_date", 'dd') - trunc (q."real_handl_date", 'dd')
        ) * 16.5 * 60 * 60 - floor(
          to_number (
            trunc (q."real_close_date", 'dd') - trunc (q."real_handl_date", 'dd') + to_number (to_char (q."real_handl_date", 'D')) - 1
          ) / 7
        ) * 7.5 * 2 * 60 * 60
      END AS "未关闭时长-秒"
    FROM
      unimax_cg.usm_exception_bill p,
      (
        SELECT
          CASE
            WHEN to_number (to_char (p.handl_date, 'D')) = 1 THEN to_date (
              to_char (p.handl_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_number (to_char (p.handl_date, 'D')) = 7 THEN to_date (
              to_char (p.handl_date + 2, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.handl_date, 'yyyymmddhh24miss') > (to_char (p.handl_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.handl_date, 'D')) = 6 THEN to_date (
              to_char (p.handl_date + 3, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.handl_date, 'yyyymmddhh24miss') > (to_char (p.handl_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.handl_date, 'D')) <> 6 THEN to_date (
              to_char (p.handl_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.handl_date, 'yyyymmddhh24miss') < (to_char (p.handl_date, 'yyyyMMdd') || '083000') THEN to_date (
              to_char (p.handl_date, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.handl_date, 'yyyymmddhh24miss') > (to_char (p.handl_date, 'yyyyMMdd') || '120000')
            AND to_char (p.handl_date, 'yyyymmddhh24miss') < (to_char (p.handl_date, 'yyyyMMdd') || '133000') THEN to_date (
              to_char (p.handl_date, 'yyyyMMdd') || '133000',
              'yyyymmddhh24miss'
            )
            ELSE p.handl_date
          END as "real_handl_date",
          CASE
            WHEN to_number (to_char (p.close_date, 'D')) = 1 THEN to_date (
              to_char (p.close_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_number (to_char (p.close_date, 'D')) = 7 THEN to_date (
              to_char (p.close_date + 2, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.close_date, 'yyyymmddhh24miss') > (to_char (p.close_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.close_date, 'D')) = 6 THEN to_date (
              to_char (p.close_date + 3, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.close_date, 'yyyymmddhh24miss') > (to_char (p.close_date, 'yyyyMMdd') || '173000')
            AND to_number (to_char (p.close_date, 'D')) <> 6 THEN to_date (
              to_char (p.close_date + 1, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.close_date, 'yyyymmddhh24miss') < (to_char (p.close_date, 'yyyyMMdd') || '083000') THEN to_date (
              to_char (p.close_date, 'yyyyMMdd') || '083000',
              'yyyymmddhh24miss'
            )
            WHEN to_char (p.close_date, 'yyyymmddhh24miss') > (to_char (p.close_date, 'yyyyMMdd') || '120000')
            AND to_char (p.close_date, 'yyyymmddhh24miss') < (to_char (p.close_date, 'yyyyMMdd') || '133000') THEN to_date (
              to_char (p.close_date, 'yyyyMMdd') || '133000',
              'yyyymmddhh24miss'
            )
            WHEN p.close_date IS NULL THEN (
              SELECT
                sysdate
              FROM
                dual
            )
            ELSE p.close_date
          END as "real_close_date",
          p.gid as "gid_new"
        FROM
          unimax_cg.usm_exception_bill p
        WHERE
          IS_ACTIVE = 0
          AND IS_DELETE = 0
      ) q
    WHERE
      p.gid = q."gid_new"
      AND IS_ACTIVE = 0
      AND IS_DELETE = 0
  ) e on p.gid = e."gid_new"
  AND e."real_handl_date" IS NOT NULL
WHERE
  DATA_ROLE = '8a98ec8b72bacea80172bb4f55840019'
  AND SYSDATE - create_date <= 720
  -- AND SYSDATE - create_date <= 1
  AND IS_ACTIVE = 0
  AND IS_DELETE = 0