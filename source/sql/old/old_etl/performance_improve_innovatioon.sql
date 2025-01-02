SELECT
    (
        case f.fbillstatus
            when "A" then "暂存"
            when "B" then "已提交"
            when "C" then "已审核"
            when "D" then "提前终止"
            else "未知的状态"
        end
    ) as "状态",
    create_user.FTRUENAME AS "创建人",
    f.fcreatetime AS "创建时间",
    f.fk_crrc_combofield AS "填报部门",
    f.fk_crrc_combofield11 AS "全员型改善指标是否完成",
    f.fk_crrc_combofield1 AS "自主型改善指标是否完成",
    f.fk_crrc_integerfield1 AS "全员型改善一等奖个数",
    f.fk_crrc_integerfield2 AS "全员型改善二等奖个数",
    f.fk_crrc_integerfield3 AS "全员型改善三等奖个数",
    f.fk_crrc_integerfield4 AS "推优到公司改善评奖数量",
    f.fk_crrc_integerfield5 AS "自主型改善一等奖个数",
    f.fk_crrc_integerfield6 AS "自住型改善二等奖个数",
    f.fk_crrc_integerfield7 AS "自主型改善三等奖个数",
    f.fk_crrc_integerfield8 AS "系统性改善个数",
    f.fk_crrc_integerfield9 AS "管理创新一等奖个数",
    f.fk_crrc_integerfield10 AS "管理创新二等奖个数",
    f.fk_crrc_integerfield AS "管理创新三等奖个数" 
FROM
    crrc_secd.tk_crrc_gscxtb f
    LEFT JOIN crrc_sys.t_sec_user create_user ON f.fcreatorid = create_user.FID