SELECT 
job.fk_crrc_billno as "单据编号",
job.fk_crrc_chgbillnum as "变更单据编号",
job.fk_crrc_chgbillname as "单据名称",
classgroup.fnumber as "执行班组编号",
classgroup.fname as "执行班组名称"
FROM tk_crrc_mesjob job
left join tk_crrc_classgroup classgroup on job.fk_crrc_exegroupcode = classgroup.fnumber
where 
classgroup.fk_crrc_orgdeptid = '966412671396108288' and 
left(classgroup.fnumber, 4)='SHFW' and
(job.fk_crrc_jobstatus = 'B' or job.fk_crrc_jobstatus = 'E')