SELECT
    tp.CODE AS "异常类型编码",
    tp.NAME AS "异常类型名称",
    tp.UDI_NAME AS "类型属性名称",
    tp.UDI_CODE AS "类型属性编码",
    tp.is_relevancy_track AS "是否关联派工单"
FROM
    unimax_cg.USM_EXCEPTION_TYPE tp
WHERE
    tp.IS_ACTIVE = 0
    AND tp.IS_DELETE = 0