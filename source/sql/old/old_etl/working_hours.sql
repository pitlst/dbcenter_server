SELECT
    p.fnumber 项目,
    t.fnumber 跟踪号,
    jch.fnumber 节车号,
    o.fnumber 工序,
    o.fname_l2 工序名称,
    s.fnumber 人员,
    s.fname_l2 人员名称,
    sum(rt.cfworkTimeHS) 工时,
    sum(rt.CFreplenishTime) 补报工时
FROM
    ZJEAS7.T_MM_CompletionReport r
    left join ZJEAS7.T_MM_CompletionRAT rt on r.fid = rt.fparentid
    left join ZJEAS7.T_MM_PROJECT p on p.fid = r.FPROJECTID
    left join ZJEAS7.T_MM_TRACKNUMBER t on t.fid = r.ftracknumberid
    left join ZJEAS7.T_PRO_ProjectJCH jch on jch.fid = r.cfProjectJCHid
    left join ZJEAS7.T_MM_Operation o on o.fid = r.cfoperationid
    left join ZJEAS7.t_bd_person s on s.fid = rt.FPERSONNUMBERID
where
    to_char(r.FBIZDATE, 'yyyy') = to_char(sysdate, 'yyyy') -- 这里@替换为需要查询的参数
    and rt.FPERSONNUMBERID is not null
    and (
        (
            rt.cfworkTimeHS is not null
            and rt.cfworkTimeHS <> 0
        )
        or (
            rt.CFreplenishTime <> 0
            and rt.CFreplenishTime is not null
        )
    )
group by
    p.fnumber,
    t.fnumber,
    jch.fnumber,
    o.fnumber,
    o.fname_l2,
    s.fnumber,
    s.fname_l2
order by
    p.fnumber,
    t.fnumber,
    jch.fnumber,
    o.fnumber,
    o.fname_l2,
    s.fnumber,
    s.fname_l2