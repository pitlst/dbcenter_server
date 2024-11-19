-- 员工个人任务量 mes工序
SELECT
-- 	new.id,
-- 	COUNT(*) AS count,
	new.`项目号`,
	new.`跟踪号`, 
	new.`节车号`,
	new.`工序编码`,
	new.`工序名称`,
	new.`作业时长`,
	new.`工位名称`,
	new.`执行人工号`,
	new.`执行人姓名`,
	t.`定额工时合并`,
	new.`计划开工时间`
FROM
(
	SELECT 
		gg.id,
		gg.`项目号`,
		gg.`跟踪号`,
		gg.`节车号`,
		gg.`工序编码`,
		gg.`工序名称`,
		gg.`工位名称`,
		u.FNUMBER AS 执行人工号,
		u.FTRUENAME AS 执行人姓名,
		gg.`作业时长`,
		gg.`计划开工时间`,
		gg.`计划完工时间`,
		gg.`任务状态`,
		gg.`是否下发` 
	FROM
		(
		SELECT DISTINCT
			g.id,
			g.`项目号`,
			g.`跟踪号`,
			g.`节车号`,
			g.`工序编码`,
			g.`工序名称`,
			g.`工位名称`,
			g.`作业时长`,
			g.`计划开工时间`,
			g.`计划完工时间`,
			g.`任务状态`,
			SUBSTRING_INDEX( SUBSTRING_INDEX( g.`执行人员`, ',', b.help_topic_id + 1 ), ',',- 1 ) AS 执行人,
			g.`部门`,
			g.`是否下发` 
		FROM
			(
			SELECT
				gx.FID AS id,
				gx.FK_CRRC_TEXTFIELD AS 项目号,
				gx.FK_CRRC_TEXTFIELD1 AS 跟踪号,
				gx.FK_CRRC_TEXTFIELD2 AS 节车号,
				gx.FK_CRRC_TEXTFIELD5 AS 工序编码,
				gx.FK_CRRC_TEXTFIELD6 AS 工序名称,
				gx.FK_CRRC_GROUP_NAME AS 工位名称,
				gx.FK_CRRC_DURATION AS 作业时长,
				gx.FK_CRRC_PLAN_START_TIME AS 计划开工时间,
				gx.FK_CRRC_PLAN_FINISH_TIME AS 计划完工时间,
				gx.FK_CRRC_TEXTFIELD8 AS 任务状态,
				gx.FK_CRRC_WORKER AS 执行人员,
				gx.FK_CRRC_WORKSHOP AS 部门,
				gx.FK_CRRC_TEXTFIELD3 AS 是否下发 
			FROM
				dataframe_flow_v2.ods_tk_crrc_gxrwxf gx
				LEFT JOIN dataframe_flow_v2.ods_t_sec_user mes_pe ON gx.FK_CRRC_WORKER = mes_pe.FNUMBER
				
			WHERE
				FK_CRRC_WORKSHOP IN ( '城轨总成车间', '城轨总成车间（TCM12）' ) 
				
				
				AND FK_CRRC_TEXTFIELD3 = 1 
				
				AND FK_CRRC_TEXTFIELD8 IN ( '0', '1', '2', '3' ) 
			) g
			INNER JOIN mysql.help_topic b ON help_topic_id < (
			LENGTH( g.`执行人员` ) - LENGTH( REPLACE ( g.`执行人员`, ',', '' ) + 1 )) 
		) gg
	LEFT JOIN dataframe_flow_v2.ods_t_sec_user u ON gg.`执行人` = u.FNUMBER
) new
	LEFT JOIN dataframe_flow_v2.ods_df_worktime_new t ON new.`项目号` = t.项目号 
	AND new.`跟踪号` = t.跟踪号 
	AND new.`节车号` = t.节车号 
	AND new.`工序编码` = t.工序号
WHERE
	t.项目号 IS NOT NULL


	