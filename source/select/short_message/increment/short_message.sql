SELECT  msg.fid AS "id"
FROM zjeas7.T_MO_SENDMOMSG msg
WHERE to_number( to_char( SYSDATE, 'yyyy' ) ) - to_number( to_char( msg.FSENTTIME, 'yyyy' ) ) < 2