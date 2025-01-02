SELECT f.FID                      AS "id",
       f.fnumber                  AS "项目号",
       f.fname                    AS "项目名称",
       CASE
           f.fenable
           WHEN 1 THEN
               '可用'
           WHEN 0 THEN
               '禁用'
           END                    AS "使用状态",
       CASE
           f.fstatus
           WHEN 'A' THEN
               '暂存'
           WHEN 'B' THEN
               '已保存'
           WHEN 'C' THEN
               '已审核'
           END                    AS "单据状态",
       f.fk_crrc_simplename       AS "项目简称",
       f.fk_crrc_projectstartyear AS "项目启动年份",
       f.fk_crrc_projectjch       AS "节车号",
       f.fmodifytime              AS "更改时间",
       creator_user.fnumber       as "创建人",
       creator_user.FTRUENAME     as "创建人姓名",
       modify_user.fnumber        as "更改人",
       modify_user.FTRUENAME      as "更改人姓名",
       crafts_user.fnumber        as "工艺经理",
       crafts_user.FTRUENAME      as "工艺经理姓名"
FROM crrc_secd.tk_crrc_projectmanager f
         left JOIN crrc_sys.t_sec_user creator_user on f.fcreatorid = creator_user.FID
         left JOIN crrc_sys.t_sec_user modify_user on f.fmodifierid = modify_user.FID
         left JOIN crrc_sys.t_sec_user crafts_user on f.fk_crrc_managerid = crafts_user.FID