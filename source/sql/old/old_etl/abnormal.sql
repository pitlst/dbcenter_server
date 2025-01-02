select
    t.gid as "id",
    t.exception_type_name as "异常类型名称",
    t.exception_type_code as "异常类型编码",
    t.workcenter_code as "工作中心编码",
    t.workcenter_name as "工作中心",
    t.workcell_code as "工位编码",
    t.workcell_name as "工位",
    e.uda104 as "班组名称",
    p.name as "车间名称",
    (
        CASE
            WHEN t.state = 0 THEN to_char ('待响应')
            WHEN t.state = 2 THEN to_char ('待处理')
            WHEN t.state = 4 THEN to_char ('待关闭')
            WHEN t.state = 8 THEN to_char ('已关闭')
            ELSE to_char ('未知')
        END
    ) AS "异常状态分类",
    t.state as "异常状态代号",
    t.exception_remark as "异常描述",
    t.uda5 as "异常备注",
    t.op_name as "工序名称",
    t.op_code as "工序代号",
    t.car_code as "车号",
    t.singer_car_code as "节车号",
    t.plan_process_date as "计划处理时间/分钟",
    t.create_id AS "创建人",
    t.create_date AS "创建日期",
    t.launch_id as "发起人",
    t.launch_date as "发起日期",
    t.modify_id AS "修改人",
    t.modify_date AS "修改日期",
    t.response_id as "响应人",
    t.uda1c as "指定响应人",
    t.response_date as "响应时间",
    t.handl_id as "处理人",
    t.handl_date as "处理时间",
    t.close_id as "关闭人",
    t.close_date as "关闭时间",
    t.mrl_code as "产品编码",
    t.mrl_Name as "产品名称",
    t.workorder_code as "工单编码",
    t.code as "异常记录编码"
from
    unimax_cg.usm_exception_bill t
    left join unimax_cg.PMBF_WORK_CELL wc on t.workcell_gid = wc.gid
    and t.is_delete = 0
    and wc.is_delete = 0
    left join unimax_cg.PMBF_WORK_CENTER pwc on wc.work_center_gid = pwc.gid
    and pwc.is_delete = 0
    left join unimax_cg.pmbf_Location p on pwc.location_gid = p.gid
    and p.is_delete = 0
    left join unimax_cg.pmbb_employee e on t.launch_id = e.code
    and e.is_delete = 0
    and t.data_role = e.data_role
    left join unimax_cg.MES_APS_EMPLOYEE m on t.uda1c = m.code
    and e.is_delete = 0
    and t.data_role = m.data_role
where
    t.IS_DELETE <> 1
    and t.class_flag = 0
    and SYSDATE - t.create_date <= 720