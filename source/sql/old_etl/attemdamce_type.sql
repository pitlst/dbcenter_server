select TypeID as 'ID', TypeCode as '代号', TypeName as '类型名称'
from [dbo].[YQ_KQ_TI_CostCenterType]
UNION ALL
select TypeID as 'ID', TypeCode as '代号', TypeName as '类型名称'
from [dbo].[YQ_KQ_TI_CustomType]
UNION ALL
select TypeID as 'ID', TypeCode as '代号', TypeName as '类型名称'
from [dbo].[YQ_KQ_TI_EarlyType]
UNION ALL
select TypeID as 'ID', TypeCode as '代号', TypeName as '类型名称'
from [dbo].[YQ_KQ_TI_FixedType]
UNION ALL
select TypeID as 'ID', TypeCode as '代号', TypeName as '类型名称'
from [dbo].[YQ_KQ_TI_LateType]
UNION ALL
select TypeID as 'ID', TypeCode as '代号', TypeName as '类型名称'
from [dbo].[YQ_KQ_TI_LeaveType]
UNION ALL
select TypeID as 'ID', TypeCode as '代号', TypeName as '类型名称'
from [dbo].[YQ_KQ_TI_OTType]
UNION ALL
select TypeID as 'ID', TypeCode as '代号', TypeName as '类型名称'
from [dbo].[YQ_KQ_TI_ShiftType]