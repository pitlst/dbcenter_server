SELECT  CODE     AS "异常类型编码"
       ,NAME     AS "异常类型名称"
       ,UDI_NAME AS "类型属性名称"
       ,UDI_CODE AS "类型属性编码"
FROM unimax_cg.USM_EXCEPTION_TYPE
WHERE DATA_ROLE = '8a98ec8b72bacea80172bb4f55840019'
AND IS_ACTIVE = 0
AND IS_DELETE = 0