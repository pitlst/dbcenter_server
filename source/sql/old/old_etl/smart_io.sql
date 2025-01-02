SELECT
    rig_project_code as "项目编码",
    rig_project_name AS "项目名称",
    (
        case rig_tipoop
            when 'P' then '出库'
            when 'V' then '入库'
            else '直接修改'
        end
    ) as "记录类别",
    rig_articolo AS "物料编码",
    rig_articolo_name AS "物料名称",
    rig_sub1 AS "节车",
    rig_sub2 AS "车号",
    rig_tipoconf AS "班组编码",
    rig_tipoconf_name AS "班组名称",
    location_no AS "仓位编码",
    location_name AS "仓位名称",
    rig_qtar AS "库存数量",
    create_time AS "出入库时间"
FROM
    imp_ordini_inventory
WHERE
    del_flag = '0'