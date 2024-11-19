select
    u.gid as "gid",
    u.code as "工号",
    u.name as "姓名",
    u.uda105 as "工种",
    u.uda104 as "班组名称",
    case u.is_work
        when 0 then '在岗'
        when 1 then '离岗'
    end as "在岗情况",
    u.DATA_ROLE
from
    unimax_cg.pmbb_employee u
where
    u.is_delete = 0
    and u.is_active = 0