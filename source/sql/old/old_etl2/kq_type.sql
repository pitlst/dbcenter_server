SELECT  TypeID   AS "id"
       ,TypeCode AS "类型编号"
       ,TypeName AS "类型名称"
FROM YQ_KQ_TI_OTType
UNION ALL
SELECT  TypeID   AS "id"
       ,TypeCode AS "类型编号"
       ,TypeName AS "类型名称"
FROM YQ_KQ_TI_EarlyType
UNION ALL
SELECT  TypeID   AS "id"
       ,TypeCode AS "类型编号"
       ,TypeName AS "类型名称"
FROM YQ_KQ_TI_LateType
UNION ALL
SELECT  TypeID   AS "id"
       ,TypeCode AS "类型编号"
       ,TypeName AS "类型名称"
FROM YQ_KQ_TI_FixedType
UNION ALL
SELECT  TypeID   AS "id"
       ,TypeCode AS "类型编号"
       ,TypeName AS "类型名称"
FROM YQ_KQ_TI_LeaveType
UNION ALL
SELECT  TypeID   AS "id"
       ,TypeCode AS "类型编号"
       ,TypeName AS "类型名称"
FROM YQ_KQ_TI_ShiftType
UNION ALL
SELECT  TypeID   AS "id"
       ,TypeCode AS "类型编号"
       ,TypeName AS "类型名称"
FROM YQ_KQ_TI_CustomType