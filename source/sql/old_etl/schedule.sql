SELECT
    f.fk_crrc_textfield as "设定日期",
    fe.fk_crrc_timefield as "开始时间",
    fe.fk_crrc_timefield1 as "结束时间"
FROM 
    crrc_secd.tk_crrc_schedule f
    left join crrc_secd.tk_crrc_schedule_entry fe on f.fid = fe.FId 