SELECT  EmployeeID     AS "员工考勤系统id"
       ,StatisticsDate AS "调整时间"
       ,TypeID         AS "考勤类型id"
       ,TimeLongXS     AS "时间长度(小时)"
       ,TimeLongFZ     AS "时间长度(分钟)"
       ,TimeLongT      AS "时间长度(百分比)"
FROM YQ_KQ_AS_MonthTotalDay
WHERE YEAR ( StatisticsDate ) = YEAR ( GETDATE( ) )