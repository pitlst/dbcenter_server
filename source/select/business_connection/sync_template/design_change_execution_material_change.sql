SELECT
       bill.FId AS "id",
       bill.FEntryId AS "单据分录id",
       bill.FSeq AS "单据分录序号",
       case bill.fk_crrc_chgtype
              WHEN 'A' THEN '新增'
              WHEN 'B' THEN '增加'
              WHEN 'C' THEN '减少'
              WHEN 'D' THEN '补料'
              WHEN 'E' THEN '其他'
              WHEN 'F' THEN '替换'
              WHEN 'G' THEN '结构更改'
       end AS "变更类别",
       bill.fk_crrc_materialnum_e3 AS "物料编码",
       bill.fk_crrc_matname_e2 AS "物料名称",
       bill.fk_crrc_materialfrom AS "所属组件编码",
       bill.fk_crrc_oldqty_e3 AS "原定额",
       bill.fk_crrc_qty_e3 AS "新定额",
       bill.fk_crrc_qtychg AS "数量变化（列/台）",
       bill.fk_crrc_begintrack_e3 AS "开始车号",
       bill.fk_crrc_endtrack_e3 AS "结束车号",
       bill.fk_crrc_projectjch_e3 AS "节车号",
       bill.fk_crrc_changecar_e3 AS "执行列车次",
       bill.fk_crrc_carnum_e3 AS "执行车数",
       bill.fk_crrc_matcount_e3 AS "物料总量",
       bill.fk_crrc_unit AS "单位",
       bill.fk_crrc_flow_e3 AS "流程",
       mater.fname AS "物料处置",
       bill.fk_crrc_orgunit_e3 AS "责任单位",
       case bill.fk_crrc_chgcause
              WHEN 'JZa1' THEN 'JZa1类：设计经验不足和设计疏忽原因引起的变更'
              WHEN 'JZa2' THEN 'JZa2类：重复错误引起的变更'
              WHEN 'JZa3' THEN 'JZa3类：部门内部接口分析错误引起的变更'
              WHEN 'JZa4' THEN 'JZa4类：工程图借用错误引起的变更'
              WHEN 'JZa5' THEN 'JZa5类：其他部门接口错误或接口遗漏而引起的变更'
              WHEN 'JZb' THEN 'JZb：业主要求及外部不可抗拒因素引起的变更'
              WHEN 'JZc1' THEN 'JZc1类：设计自身调整要求引起的变更'
              WHEN 'JZc2' THEN 'JZc2类：采购要求引起的变更'
              WHEN 'JZc3' THEN 'JZc3类：为满足现场制造及工艺实现需求引起的变更'
              WHEN 'JZc4' THEN 'JZc4类：试验和运行要求引起的变更'
              WHEN 'JZc5' THEN 'JZc5类：其他部门设计调整要求引起的变更'
              WHEN 'JZd' THEN 'JZd类：成本优化要求引起的变更'
       end AS "变更原因",
       pro.fname AS "问题类别",
       bill.fk_crrc_chgafterdesc AS "更改描述",
       case bill.fk_crrc_system
              WHEN 'A' THEN '总体'
              WHEN 'B' THEN '牵引、辅助及控制'
              WHEN 'C' THEN '屏柜'
              WHEN 'D' THEN '电气'
              WHEN 'E' THEN '布线'
              WHEN 'F' THEN '照明'
              WHEN 'G' THEN '制动'
              WHEN 'H' THEN '车体'
              WHEN 'I' THEN '内装'
              WHEN 'J' THEN '车钩'
              WHEN 'K' THEN '贯通道'
              WHEN 'L' THEN '转向架'
              WHEN 'M' THEN '车门'
              WHEN 'N' THEN '空调及通风'
              WHEN 'O' THEN '乘客信息系统'
       END AS "所属系统",
       bill.fk_crrc_remark_e3 AS "备注"
FROM
       crrc_secd.tk_crrc_materialchgentry bill
       LEFT JOIN crrc_secd.tk_crrc_materialcztype mater ON mater.FID = bill.fk_crrc_materialczid
       LEFT JOIN crrc_secd.tk_crrc_problemtype pro ON pro.fid = bill.fk_crrc_problemtype
WHERE