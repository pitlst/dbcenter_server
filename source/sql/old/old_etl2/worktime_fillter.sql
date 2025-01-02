SELECT  f.fbillno            AS "id"
       ,f.fk_crrc_datefield  AS "日期"
       ,_user.fnumber        AS "工号"
       ,_user.FTRUENAME      AS "姓名"
       ,f.fk_crrc_combofield AS "是否加入计算"
FROM crrc_secd.tk_crrc_ryxn f
LEFT JOIN crrc_sys.t_sec_user _user
ON f.fk_crrc_userfield = _user.FID