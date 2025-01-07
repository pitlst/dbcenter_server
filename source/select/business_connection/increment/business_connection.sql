SELECT  bill.FId               AS "id"
       ,bill.fmodifytime       AS "修改时间"
       ,bill.fk_crrc_issuedate AS "审核时间"
       ,bill.fauditdate        AS "签发时间"
FROM crrc_secd.tk_crrc_bizcontactbook bill