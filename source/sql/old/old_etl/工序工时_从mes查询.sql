select (
           v.project | | v.car_code | | v.singer_car_code | | v.op_code | | o.op_name | | v.is_man_hour_rpt
           )                       as "id",
       v.project                   as "carProduct",
       v.car_code                  as "carCode",
       v.singer_car_code           as "everyCarCode",
       v.op_code                   as "opCode",
       max(g.group_name)           AS "班组",
       case max(pl.NAME)
           when '城轨交车车间（TCM12）' then '城轨交车车间'
           when '城轨总成车间（TCM12）' then '城轨总成车间'
           else max(pl.NAME) end   AS "车间",
       max(v.planned_start_time)      AS "实际结束时间",
       max(u1.name)                as "workHourCheckEmpName",
       max(v.WORK_HOUR_CHECK_DATE) as "workHourCheckDate",
       max(u2.name)                as "workHourDownEmpName",
       max(v.WORK_HOUR_DOWN_DATE)  as "workHourDownDate",
       max(v.WORK_HOUR_DOWN_CAUSE) as "workHourDownCause",
       o.op_name                   as "opName",
       max(s.std_time)             as "stdTime",
       v.is_man_hour_rpt           as "isManHourRpt",
       max(pn.name)                as "carProductName"
from unimax_cg.uex_vtrack_record v
         LEFT JOIN unimax_cg.mbf_labour_group g ON v.labour_group_gid = g.gid
    AND g.is_delete = 0
         inner join unimax_cg.mbf_route_operation o on v.op_gid = o.gid
    and o.is_delete = 0
         LEFT JOIN unimax_cg.PMBF_WORK_CELL cell ON o.uda_cute_sta = cell.gid
    AND cell.is_delete = 0
         INNER JOIN unimax_cg.pmbf_work_center wc ON cell.WORK_CENTER_GID = wc.gid
    AND wc.is_delete = 0
         LEFT JOIN unimax_cg.PMBF_LOCATION pl ON wc.LOCATION_GID = pl.GID
    AND pl.IS_DELETE = 0
         left join unimax_cg.uex_standard_man_hour s on v.project = s.pro_code
    and v.car_code = s.car_code
    and nvl(v.singer_car_code, '#singer_car_code') = nvl(s.single_car_code, '#singer_car_code')
    and v.op_code = s.op_code
    and s.is_delete = 0
         left join unimax_cg.uda_project_edit pn on v.project = pn.code
    and pn.is_delete = 0
         left join unimax_cg.MTS_user u1 on u1.login_name = v.WORK_HOUR_CHECK_EMP
    and u1.is_delete = 0
         left join unimax_cg.MTS_user u2 on u2.login_name = v.WORK_HOUR_DOWN_EMP
    and u2.is_delete = 0
where v.is_delete <> 1
  and v.order_type = 0
  and v.dis_code_state = 3
  and v.repeat_id is null
  and v.program_name is null
  and v.uda_out_state <> 1
  and TO_CHAR(v.ACTUAL_END_DATE, 'YYYY-MM') = TO_CHAR(SYSDATE, 'YYYY-MM')
-- AND SYSDATE -  <= 180
-- AND SYSDATE - v.ACTUAL_END_DATE <= 1
group by v.project,
         v.car_code,
         v.singer_car_code,
         v.op_code,
         o.op_name,
         v.is_man_hour_rpt