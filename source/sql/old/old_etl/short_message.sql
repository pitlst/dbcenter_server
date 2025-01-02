SELECT
	msg.fid as "id",
	msg.FTITLE as "标题",
	msg.FSENTTIME as "发送时间",
	msg.FCONTENT as "短信详情",
	-- 以下为标志位
	msg.FNOTIFY as "是否通知消息",
	msg.FREVERTIBLE as "是否可以回复",
	msg.FRESPONSED as "是否已经回复",
	msg.FSENT as "是否已经发送",
	msg.FSENDSUCCEED as "是否发送成功",
	msg.FSENDFAIL as "是否发送失败",
	msg.FISDELETED as "删除标志位"
FROM zjeas7.T_MO_SENDMOMSG msg
WHERE to_number( to_char( SYSDATE, 'yyyy' ) ) - to_number( to_char( msg.FSENTTIME, 'yyyy' ) ) < 2