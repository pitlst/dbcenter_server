SELECT
       bill.fid AS "id",
       CASE bill.fk_crrc_jobstatus
              WHEN 'A' THEN '发送中'
              WHEN 'B' THEN '已下发'
              WHEN 'C' THEN '执行完工'
              WHEN 'D' THEN '质检合格'
              WHEN 'E' THEN '质检不合格'
              WHEN 'F' THEN '作废中'
              WHEN 'G' THEN '作废'
              WHEN 'H' THEN '转交中'
       END AS "任务状态"
FROM
       crrc_secd.tk_crrc_mesjob bill