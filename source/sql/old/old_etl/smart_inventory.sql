SELECT
    t.rig_project_code AS "项目编码",
    ANY_VALUE (t.rig_project_name) AS "项目名称",
    t.rig_articolo AS "物料编码",
    ANY_VALUE (t.rig_articolo_name) AS "物料名称",
    t.rig_sub1 AS "节车",
    t.rig_sub2 AS "车号" ,
    t.rig_tipoconf AS rigTipoconf,
    ANY_VALUE (t.rig_tipoconf_name) AS "工位名称",
    t.location_no AS "仓位编码" ,
    ANY_VALUE (t.location_name) AS "仓位名称" ,
    SUM(rig_qtar) AS "库存数量",
    MAX(IF (rig_tipoop = 'P', '', create_time)) AS "入库时间"
FROM 
    imp_ordini_inventory t
WHERE
    del_flag = '0'
GROUP BY
    rig_project_code,
    rig_articolo,
    rig_sub1,
    rig_tipoconf,
    location_no