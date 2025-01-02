SELECT
        case 
        when (  CASE
                WHEN ogad3.FNAME_L2 = '城轨事业部' THEN ogad2.FNAME_L2 
                WHEN ogad2.FNAME_L2 = '城轨事业部' THEN ogad.FNAME_L2 
                WHEN ogad.FNAME_L2 = '城轨事业部' THEN ogad.FNAME_L2 
                ELSE ogad.FNAME_L2 
                END) = '城轨事业部'
        then (  case
                when bu.FNAME_L2 = '城轨事业部' then to_nchar('城轨事业部')
                else bu2.FNAME_L2
                end
        )
        else (  CASE
                WHEN ogad3.FNAME_L2 = '城轨事业部' THEN ogad2.FNAME_L2 
                WHEN ogad2.FNAME_L2 = '城轨事业部' THEN ogad.FNAME_L2 
                WHEN ogad.FNAME_L2 = '城轨事业部' THEN ogad.FNAME_L2 ELSE ogad.FNAME_L2 
                END)
        end AS "申请部门",
        constcenter_shenqing.FNAME_L2 AS "费用承担部门",
        shenqing.FCause AS "出差事由",
        traveltype_shenqing.FNAME_L2 AS "出差类型",
        project.FNAME_L2 AS "项目名称",
        project.FNUMBER AS "项目号",
        replace(WM_CONCAT ( shenqinge.FTo ),',',';') AS "出差目的地",
        bd_person.FNAME_L2 AS "员工姓名",
        bd_person.FIDNUM AS "员工编码",
        REGEXP_SUBSTR(B.FNAME_L2, '([A-Z]+[_]?[0-9]*)+') AS "报销级别",
        bd_person.Fpostname AS "工种\职位\职务",
        bu.FNAME_L2 AS "所属hr组织",
    CASE
            
            WHEN baoxiaoe.FStartDate IS NOT NULL THEN
            baoxiaoe.FStartDate ELSE shenqinge.FStartDate 
        END AS "出发时间",
    CASE
            
            WHEN baoxiaoe.FEndDate IS NOT NULL THEN
            baoxiaoe.FEndDate ELSE shenqinge.FEndDate 
        END AS "结束时间",
        (
        trunc( CASE WHEN baoxiaoe.FEndDate IS NOT NULL THEN baoxiaoe.FEndDate ELSE shenqinge.FEndDate END ) - trunc( CASE WHEN baoxiaoe.FStartDate IS NOT NULL THEN baoxiaoe.FStartDate ELSE shenqinge.FStartDate END ) + 1 
    ) AS "住宿天数",
    baoxiao.FAMOUNTAPPROVED - baoxiao.CFTAXAMOUNT AS "报销金额",
-- 是否基地
-- 是否国外项目
CASE
        shenqing.fstate 
        WHEN 10 THEN
        '制单' 
        WHEN 20 THEN
        '暂存' 
        WHEN 25 THEN
        '已提交' 
        WHEN 27 THEN
        '已废弃' 
        WHEN 30 THEN
        '审核中' 
        WHEN 40 THEN
        '审核未通过' 
        WHEN 43 THEN
        '已生成付款单' 
        WHEN 45 THEN
        '已生成凭证' 
        WHEN 46 THEN
        '已生成K/3凭证' 
        WHEN 47 THEN
        '已生成凭证未付款' 
        WHEN 50 THEN
        '取消' 
        WHEN 60 THEN
        '审核通过' 
        WHEN 70 THEN
        '已付款' 
        WHEN 65 THEN
        '等待付款' 
        WHEN 80 THEN
        '已关闭' 
        WHEN 90 THEN
        '已生成报销单' 
        WHEN 110 THEN
        '已挂账' 
        WHEN 115 THEN
        '暂挂账' 
        WHEN 120 THEN
        '已生成结算单' 
        WHEN 125 THEN
        '付款中' 
        WHEN 130 THEN
        '收款中' 
        WHEN 135 THEN
        '已收款' 
        WHEN 140 THEN
        '已生成收款单' ELSE to_char( shenqing.fstate ) 
    END AS "申请单状态",
CASE
        baoxiao.fstate 
        WHEN 10 THEN
        '制单' 
        WHEN 20 THEN
        '暂存' 
        WHEN 25 THEN
        '已提交' 
        WHEN 27 THEN
        '已废弃' 
        WHEN 30 THEN
        '审核中' 
        WHEN 40 THEN
        '审核未通过' 
        WHEN 43 THEN
        '已生成付款单' 
        WHEN 45 THEN
        '已生成凭证' 
        WHEN 46 THEN
        '已生成K/3凭证' 
        WHEN 47 THEN
        '已生成凭证未付款' 
        WHEN 50 THEN
        '取消' 
        WHEN 60 THEN
        '审核通过' 
        WHEN 70 THEN
        '已付款' 
        WHEN 65 THEN
        '等待付款' 
        WHEN 80 THEN
        '已关闭' 
        WHEN 90 THEN
        '已生成报销单' 
        WHEN 110 THEN
        '已挂账' 
        WHEN 115 THEN
        '暂挂账' 
        WHEN 120 THEN
        '已生成结算单' 
        WHEN 125 THEN
        '付款中' 
        WHEN 130 THEN
        '收款中' 
        WHEN 135 THEN
        '已收款' 
        WHEN 140 THEN
        '已生成收款单' ELSE to_char( baoxiao.fstate ) 
    END AS "报销单状态",
    5 AS "发生时间段",
    shenqing.FNumber AS "申请单单据编号",
    baoxiao.FNumber AS "报销单单据编号" 
FROM
    ZJEAS7.T_BC_EVECTIONLOANBILL shenqing
    LEFT JOIN ZJEAS7.T_BC_EvectionLoanBillEntry shenqinge ON shenqinge.FBillID = shenqing.FID
    LEFT JOIN ZJEAS7.T_BC_TravelABLCE temp_connrct ON shenqing.FID = temp_connrct.FSOURCEBILLID
    LEFT JOIN ZJEAS7.T_BC_TRAVELACCOUNTBILL baoxiao ON temp_connrct.FParentID = baoxiao.FID
    LEFT JOIN ZJEAS7.T_BC_TravelAccountBillEntry baoxiaoe ON baoxiao.FID = baoxiaoe.FBillID
    LEFT JOIN ZJEAS7.T_ORG_CostCenter constcenter_shenqing ON shenqing.FCostedDeptID = constcenter_shenqing.FID
    LEFT JOIN ZJEAS7.T_ORG_CostCenter constcenter_baoxiao ON baoxiao.FCostedDeptID = constcenter_baoxiao.FID
    LEFT JOIN ZJEAS7.T_BC_TravelType traveltype_shenqing ON shenqing.FTravelTypeId = traveltype_shenqing.FID
    LEFT JOIN ZJEAS7.T_BD_Project project ON shenqing.CFPROJECTID = project.FID
    LEFT JOIN ZJEAS7.T_BD_Person bd_person ON shenqing.FApplierID = bd_person.FID
    left join ZJEAS7.T_BC_STAFFREIMBURSELEVELSET post ON post.FPERSONID = bd_person.FID
    left join ZJEAS7.T_BC_REIMBURSELEVEL B ON post.FREIMBURSELEVELID = B.FID
    LEFT JOIN ZJEAS7.T_ORG_Positionmember pm ON bd_person.fid = pm.fpersonid
    LEFT JOIN ZJEAS7.T_ORG_Position pt ON pm.fpositionid = pt.fid
    LEFT JOIN ZJEAS7.T_ORG_baseunit bu ON pt.fadminorgunitid = bu.fid
    INNER JOIN ZJEAS7.T_ORG_baseunit bu2 ON bu.FParentID = bu2.fid
    INNER JOIN ZJEAS7.T_ORG_baseunit bu3 ON bu2.FParentID = bu3.fid
    LEFT JOIN ZJEAS7.T_ORG_admin ogad ON shenqing.forgunitid = ogad.fid
    INNER JOIN ZJEAS7.T_ORG_admin ogad2 ON ogad2.fid = ogad.fparentid
    INNER JOIN ZJEAS7.T_ORG_admin ogad3 ON ogad3.fid = ogad2.fparentid 
WHERE
    (
        constcenter_baoxiao.FNAME_L2 = '城轨事业部' 
        OR bu.FNAME_L2 = '城轨事业部' 
        OR bu.FParentID = 'kcYAAE9w5izM567U' 
        OR -- 质量技术部
        bu.FParentID = 'kcYAAAAytz3M567U' 
        OR -- 项目工程部
        bu.FParentID = '4bxquXOYSCyyT2ggSDaeKsznrtQ=' 
        OR -- 综合管理部
        bu.FParentID = 'vlLKlmk0SxO5ZzcPxHFFfMznrtQ=' 
        OR -- 总成车间
        bu.FParentID = 'UJHQgbgXRmyfMFeb0D6QTcznrtQ=' 
    ) --  交车车间
--    and baoxiao.FNumber is null
-- 查询最近两年的
    
    AND pm.FISPRIMARY = 1 -- 获取有效的职位信息
    
    AND to_number( to_char( SYSDATE, 'yyyy' ) ) - to_number( to_char( shenqing.FCREATETIME, 'yyyy' ) ) < 2 
    AND shenqing.fstate <> 27 
    AND shenqing.fstate <> 70 
    AND (baoxiao.FNumber is null or baoxiao.fstate <> 27) 
GROUP BY
        case 
        when (  CASE
                WHEN ogad3.FNAME_L2 = '城轨事业部' THEN ogad2.FNAME_L2 
                WHEN ogad2.FNAME_L2 = '城轨事业部' THEN ogad.FNAME_L2 
                WHEN ogad.FNAME_L2 = '城轨事业部' THEN ogad.FNAME_L2 
                ELSE ogad.FNAME_L2 
                END) = '城轨事业部'
        then (  case
                when bu.FNAME_L2 = '城轨事业部' then to_nchar('城轨事业部')
                else bu2.FNAME_L2
                end
        )
        else (  CASE
                WHEN ogad3.FNAME_L2 = '城轨事业部' THEN ogad2.FNAME_L2 
                WHEN ogad2.FNAME_L2 = '城轨事业部' THEN ogad.FNAME_L2 
                WHEN ogad.FNAME_L2 = '城轨事业部' THEN ogad.FNAME_L2 ELSE ogad.FNAME_L2 
                END)
        end,
        constcenter_shenqing.FNAME_L2,
        shenqing.FCause,
        traveltype_shenqing.FNAME_L2,
        project.FNAME_L2,
        project.FNUMBER,
        bd_person.FNAME_L2,
        bd_person.FIDNUM,
        bd_person.Fpostname,
        REGEXP_SUBSTR(B.FNAME_L2, '([A-Z]+[_]?[0-9]*)+'),
        bu.FNAME_L2 ,
    CASE
            
            WHEN baoxiaoe.FStartDate IS NOT NULL THEN
            baoxiaoe.FStartDate ELSE shenqinge.FStartDate 
        END,
    CASE
            
            WHEN baoxiaoe.FEndDate IS NOT NULL THEN
            baoxiaoe.FEndDate ELSE shenqinge.FEndDate 
        END,
        (
        trunc( CASE WHEN baoxiaoe.FEndDate IS NOT NULL THEN baoxiaoe.FEndDate ELSE shenqinge.FEndDate END ) - trunc( CASE WHEN baoxiaoe.FStartDate IS NOT NULL THEN baoxiaoe.FStartDate ELSE shenqinge.FStartDate END ) + 1 
    ) ,
    baoxiao.FAMOUNTAPPROVED - baoxiao.CFTAXAMOUNT,
-- 是否基地
-- 是否国外项目
CASE
        shenqing.fstate 
        WHEN 10 THEN
        '制单' 
        WHEN 20 THEN
        '暂存' 
        WHEN 25 THEN
        '已提交' 
        WHEN 27 THEN
        '已废弃' 
        WHEN 30 THEN
        '审核中' 
        WHEN 40 THEN
        '审核未通过' 
        WHEN 43 THEN
        '已生成付款单' 
        WHEN 45 THEN
        '已生成凭证' 
        WHEN 46 THEN
        '已生成K/3凭证' 
        WHEN 47 THEN
        '已生成凭证未付款' 
        WHEN 50 THEN
        '取消' 
        WHEN 60 THEN
        '审核通过' 
        WHEN 70 THEN
        '已付款' 
        WHEN 65 THEN
        '等待付款' 
        WHEN 80 THEN
        '已关闭' 
        WHEN 90 THEN
        '已生成报销单' 
        WHEN 110 THEN
        '已挂账' 
        WHEN 115 THEN
        '暂挂账' 
        WHEN 120 THEN
        '已生成结算单' 
        WHEN 125 THEN
        '付款中' 
        WHEN 130 THEN
        '收款中' 
        WHEN 135 THEN
        '已收款' 
        WHEN 140 THEN
        '已生成收款单' ELSE to_char( shenqing.fstate ) 
    END,
CASE
        baoxiao.fstate 
        WHEN 10 THEN
        '制单' 
        WHEN 20 THEN
        '暂存' 
        WHEN 25 THEN
        '已提交' 
        WHEN 27 THEN
        '已废弃' 
        WHEN 30 THEN
        '审核中' 
        WHEN 40 THEN
        '审核未通过' 
        WHEN 43 THEN
        '已生成付款单' 
        WHEN 45 THEN
        '已生成凭证' 
        WHEN 46 THEN
        '已生成K/3凭证' 
        WHEN 47 THEN
        '已生成凭证未付款' 
        WHEN 50 THEN
        '取消' 
        WHEN 60 THEN
        '审核通过' 
        WHEN 70 THEN
        '已付款' 
        WHEN 65 THEN
        '等待付款' 
        WHEN 80 THEN
        '已关闭' 
        WHEN 90 THEN
        '已生成报销单' 
        WHEN 110 THEN
        '已挂账' 
        WHEN 115 THEN
        '暂挂账' 
        WHEN 120 THEN
        '已生成结算单' 
        WHEN 125 THEN
        '付款中' 
        WHEN 130 THEN
        '收款中' 
        WHEN 135 THEN
        '已收款' 
        WHEN 140 THEN
        '已生成收款单' ELSE to_char( baoxiao.fstate ) 
    END,
    shenqing.FNumber,
    baoxiao.FNumber 