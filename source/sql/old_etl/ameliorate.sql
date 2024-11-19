SELECT
    f.fk_crrc_proposal_no as "提案编号",
    f.fk_crrc_proposal_name as "题案名称",
    -- f.fk_crrc_proposal_status as "题案状态代号",
    (
        case f.fk_crrc_proposal_status
            when "A" then "暂存"
            when "B" then "已提交"
            when "C" then "已审核"
            when "D" then "提前终止"
            else "未知的状态"
        end
    ) as "题案状态",
    f.fk_crrc_proposal_type as "改善类型",
    -- f.fk_crrc_creator as "第一题案人",
    creator_user.FTRUENAME as "第一题案人",
    -- f.fk_crrc_2nd_proposer as "第二申报人",
    f2nd_proposer_user.FTRUENAME as "第二申报人",
    f.fcreatetime as "创建时间",
    f.fk_crrc_final_grade as "最终等级分",
    -- f.fk_crrc_org1 as "提案单位一级",
    org1.FNAME as "提案单位一级",
    -- f.fk_crrc_org2 as "提案单位二级",
    org2.FNAME as "提案单位二级",
    -- f.fk_crrc_last_org as "班组",
    last_org.FNAME as "班组",
    f.fk_crrc_prize as "奖项",
    -- f.fk_crrc_auditor as "审核人",
    auditor_user.FTRUENAME as "审核人",
    f.fauditdate as "审核日期",
    f.fk_crrc_currentapprover as "流程当前处理人",
    f.fk_crrc_is_recommened as "是否推优",
    -- f.fk_crrc_1st_unit_auditor as "一级单位改善专员",
    f1st_unit_auditor_user.FTRUENAME as "一级单位改善专员",
    -- f.fk_crrc_2nd_unit_auditor as "二级单位改善专员",
    f2nd_unit_auditor_user.FTRUENAME as "二级单位改善专员",
    f.fk_crrc_submit_date as "提交日期",
    -- f.fk_crrc_submit_user as "提交用户",
    submit_user.FTRUENAME as "提交用户",
    f.fk_crrc_end_date as "提前终止日期",
    -- f.fk_crrc_end_user as "提前终止用户",
    end_user.FTRUENAME as "提前终止用户"
FROM
    crrc_secd.tk_crrc_jygs_evo_apl f
    left JOIN crrc_sys.t_sec_user creator_user on f.fk_crrc_creator = creator_user.FID
    left JOIN crrc_sys.t_sec_user f2nd_proposer_user on f.fk_crrc_2nd_proposer = f2nd_proposer_user.FID
    left JOIN crrc_sys.t_sec_user auditor_user on f.fk_crrc_auditor = auditor_user.FID
    left JOIN crrc_sys.t_sec_user f1st_unit_auditor_user on f.fk_crrc_1st_unit_auditor = f1st_unit_auditor_user.FID
    left JOIN crrc_sys.t_sec_user f2nd_unit_auditor_user on f.fk_crrc_2nd_unit_auditor = f2nd_unit_auditor_user.FID
    left JOIN crrc_sys.t_sec_user submit_user on f.fk_crrc_submit_user = submit_user.FID
    left JOIN crrc_sys.t_sec_user end_user on f.fk_crrc_end_user = end_user.FID
    left JOIN crrc_sys.t_org_org org1 on f.fk_crrc_org1 = org1.FID
    left JOIN crrc_sys.t_org_org org2 on f.fk_crrc_org2 = org2.FID
    left JOIN crrc_sys.t_org_org last_org on f.fk_crrc_last_org = last_org.FID