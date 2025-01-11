SELECT
       site_.GID AS "id",
       site_.MODIFY_DATE AS "修改时间"
FROM
       unimax_cg.alignment_file_site site_
WHERE
       site_.IS_ACTIVE = 0
       AND site_.IS_DELETE = 0
       AND (
              site_.EXP_RESULT = 1
              OR site_.EXP_RESULT IS NULL
       )
       AND to_char (site_.MODIFY_DATE, 'yyyy-MM') >= to_char (SYSDATE, 'yyyy-MM')