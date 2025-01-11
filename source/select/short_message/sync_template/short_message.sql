SELECT
       msg.fid AS "id",
       msg.FTITLE AS "标题",
       msg.FSENTTIME AS "发送时间",
       msg.FCONTENT AS "短信详情",
       msg.FNOTIFY AS "是否通知消息",
       msg.FREVERTIBLE AS "是否可以回复",
       msg.FRESPONSED AS "是否已经回复",
       msg.FSENT AS "是否已经发送",
       msg.FSENDSUCCEED AS "是否发送成功",
       msg.FSENDFAIL AS "是否发送失败",
       msg.FISDELETED AS "删除标志位"
FROM
       zjeas7.T_MO_SENDMOMSG msg
where
       msg.FSENTTIME >= TRUNC (SYSDATE) - 180
       and