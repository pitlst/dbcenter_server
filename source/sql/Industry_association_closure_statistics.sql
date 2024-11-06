SELECT  job.fk_crrc_billno      AS "单据编号"
       ,job.fk_crrc_chgbillnum  AS "变更单据编号"
       ,job.fk_crrc_chgbillname AS "单据名称"
       ,classgroup.fnumber      AS "执行班组编号"
       ,classgroup.fname        AS "执行班组名称"
FROM tk_crrc_mesjob job
LEFT JOIN tk_crrc_classgroup classgroup
ON job.fk_crrc_exegroupcode = classgroup.fnumber
WHERE classgroup.fk_crrc_orgdeptid = '966412671396108288'
AND left(classgroup.fnumber, 4) = 'SHFW'
AND (job.fk_crrc_jobstatus = 'B' or job.fk_crrc_jobstatus = 'E')