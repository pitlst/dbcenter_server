SELECT
    msg.fid AS "id"
FROM
    zjeas7.T_MO_SENDMOMSG msg
where
    msg.FSENTTIME >= TRUNC (SYSDATE) - 180