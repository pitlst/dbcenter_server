SELECT  f.fk_crrc_textfield  AS "车号"
       ,f.fk_crrc_combofield AS "调试产线"
       ,_user.fnumber        AS "负责人工号"
       ,_user.ftruename      AS "负责人姓名"
FROM crrc_secd.tk_crrc_jcgd f
LEFT JOIN crrc_sys.t_sec_user _user
ON f.fk_crrc_basedatafield = _user.FID