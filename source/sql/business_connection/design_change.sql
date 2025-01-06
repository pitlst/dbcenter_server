SELECT  fid              AS "id"
       ,fbillno          AS "变更单编码"
       ,fk_crrc_chgname  AS "变更单名称"
       ,fk_crrc_senddate AS "发放日期"
       ,fk_crrc_project  AS "项目号"
       ,fk_crrc_user     AS "用户"
       ,fk_crrc_chgtype  AS "变更类型"
       ,fk_crrc_sendunit AS "发送单位"
       ,fk_crrc_remark   AS "备注"
       ,fk_crrc_issplit  AS "是否拆分"
       ,fmodifytime      AS "修改时间"
FROM crrc_secd.tk_crrc_designchgcenter