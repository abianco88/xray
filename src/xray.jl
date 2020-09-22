# Julia v1.5.1
#=
    This file contains two modules (PreliminaryQC and SomeQC) that are designed to produce some QC metrics for internal use of the MTA analysis.
    PreliminaryQC contains two functions to construct some input objects for SomeQC that are not directly available: the data frame `dfs` and the ordered dictionary
    `res`. SomeQC takes care of the rest and containes all functions that perform the QC calculations and outputting. The main function is `match_qc_ptsb`.

    The package used in these modules are: DataFrames, DataStructures, GLM, Statistics.

    NOTE: IT MAY BE WORTHED TO INCLUDE BOTH MODULES IN A MASTER MODULE. CHECK WITH GLEN!!!
=#



module PreliminaryQC
using CSV, DataFrames, DataStructures, Statistics    # NEED TO CHECK IF PACKAGE CSV IS STILL NEEDED!!!

function make_dfs(df::DataFrame, rs::DataFrame)
    res_expsd = rs[:, [:orid, :samp]];
    res_expsd[:group] = 1;
    res_cntrl = rs[:, [:ctrl, :samp]];
    rename!(res_cntrl, :ctrl => :orid);
    res_cntrl[:group] = 0;
    df_match = append!(res_expsd, res_cntrl);
    df[:orid] = 1:length(df[:panid]);
    df_all = leftjoin(df_match, df, on = [:orid, :group]);

    gdf = groupby(df_all, :samp);
    dep_vars = Dict("pen" => Dict("pre" => :buyer_pre_p1, "pos" => :buyer_pos_p1), "occ" => Dict("pre" => :trps_pre_p1, "pos" => :trps_pos_p1), "dolocc" => Dict("pre" => :dol_per_trip_pre_p1, "pos" => :dol_per_trip_pos_p1), "dolhh" => Dict("pre" => :prd_1_net_pr_pre, "pos" => :prd_1_net_pr_pos));
    dfs = DataFrame(samp = Int64[], dependent_variable = String[], UDJ_AVG_EXPSD_HH_PRE = Float64[], UDJ_AVG_CNTRL_HH_PRE = Float64[], UDJ_AVG_EXPSD_HH_PST = Float64[], UDJ_AVG_CNTRL_HH_PST = Float64[]);
    for sm in unique(df_all[:samp])
        for vr in keys(dep_vars)
            if (vr != "pen") & (vr != "dolhh")
                out = vcat(sm, vr, mean(gdf[sm][(gdf[sm][:group] .== 1) .& (gdf[sm][values(dep_vars["pen"]["pre"])] .== 1), values(dep_vars[vr]["pre"])]), mean(gdf[sm][(gdf[sm][:group] .== 0) .& (gdf[sm][values(dep_vars["pen"]["pre"])] .== 1), values(dep_vars[vr]["pre"])]), mean(gdf[sm][(gdf[sm][:group] .== 1) .& (gdf[sm][values(dep_vars["pen"]["pos"])] .== 1), values(dep_vars[vr]["pos"])]), mean(gdf[sm][(gdf[sm][:group] .== 0) .& (gdf[sm][values(dep_vars["pen"]["pos"])] .== 1), values(dep_vars[vr]["pos"])]));
            else
                out = vcat(sm, vr, mean(gdf[sm][gdf[sm][:group] .== 1, values(dep_vars[vr]["pre"])]), mean(gdf[sm][gdf[sm][:group] .== 0, values(dep_vars[vr]["pre"])]), mean(gdf[sm][gdf[sm][:group] .== 1, values(dep_vars[vr]["pos"])]), mean(gdf[sm][gdf[sm][:group] .== 0, values(dep_vars[vr]["pos"])]));
            end
            push!(dfs, out);
        end
    end

    na_vars = ["ADJ_MEAN_EXPSD_GRP", "ADJ_MEAN_CNTRL_GRP", "ATTE", "sigma", "z", "pValue", "significance", "var_part1", "var_part2", "LB_80", "UB_80", "LB_90", "UB_90", "LB_95", "UB_95", "bias_correction_offset", "CNT_Model_HH", "controlBuyer", "ACC"];
    na_vars_int = ["CNT_Model_HH", "controlBuyer"];
    for vr in na_vars
        if !(vr in na_vars_int)
            dfs[vr] = 0.0;
        else
            dfs[vr] = 0;
        end
    end

    dfs[:brk] = 1;
    dfs[:MODEL_DESC] = "Total Campaign";

    return dfs
end

function gen_res_dictionary(rs::DataFrame)
    res = OrderedDict{Any,Any}(1 => Dict{Symbol,Any}(:brk => "total", :orid => rs[:orid], :lvl => "total", :mtch => rs));

    return res
end
end



module SomeQC
using DataFrames, DataStructures, GLM, Statistics

function match_metric_qc(df::DataFrame, dfs::DataFrame)
    dfss = deepcopy(dfs);
    match_dfd = DataFrame(DESC = String[], MODEL_DESC = String[], MODEL = String[], UDJ_AVG_EXPSD_HH_PRE = Float64[], UDJ_AVG_CNTRL_HH_PRE = Float64[], UDJ_AVG_EXPSD_HH_PST = Float64[], UDJ_AVG_CNTRL_HH_PST = Float64[]);
    push!(match_dfd, ["Pre Match", "Total Campaign", "PEN", mean(df[df[:group] .== 1, :buyer_pre_p1]), mean(df[df[:group] .== 0, :buyer_pre_p1]), mean(df[df[:group] .== 1, :buyer_pos_p1]), mean(df[df[:group] .== 0, :buyer_pos_p1])]);
    push!(match_dfd, ["Pre Match", "Total Campaign", "OCC", mean(df[((df[:group] .== 1) .& (df[:buyer_pre_p1] .== 1)), :trps_pre_p1]), mean(df[((df[:group] .== 0) .& (df[:buyer_pre_p1] .== 1)), :trps_pre_p1]), mean(df[((df[:group] .== 1) .& (df[:buyer_pos_p1] .== 1)), :trps_pos_p1]), mean(df[((df[:group] .== 0) .& (df[:buyer_pos_p1] .== 1)), :trps_pos_p1])]);
    push!(match_dfd, ["Pre Match", "Total Campaign", "DOLOCC", mean(df[((df[:group] .== 1) .& (df[:buyer_pre_p1] .== 1)), :dol_per_trip_pre_p1]), mean(df[((df[:group] .== 0) .& (df[:buyer_pre_p1] .== 1)), :dol_per_trip_pre_p1]), mean(df[((df[:group] .== 1) .& (df[:buyer_pos_p1] .== 1)), :dol_per_trip_pos_p1]), mean(df[((df[:group] .== 0) .& (df[:buyer_pos_p1] .== 1)), :dol_per_trip_pos_p1])]);
    push!(match_dfd, ["Pre Match", "Total Campaign", "DOLHH", mean(df[df[:group] .== 1, :prd_1_net_pr_pre]), mean(df[df[:group] .== 0, :prd_1_net_pr_pre]), mean(df[df[:group] .== 1, :prd_1_net_pr_pos]), mean(df[df[:group] .== 0, :prd_1_net_pr_pos])]);
    push!(match_dfd, ["Pos Match", "Total Campaign", "PEN", mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "pen")), :UDJ_AVG_EXPSD_HH_PRE]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "pen")), :UDJ_AVG_CNTRL_HH_PRE]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "pen")), :UDJ_AVG_EXPSD_HH_PST]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "pen")), :UDJ_AVG_CNTRL_HH_PST])]);
    push!(match_dfd, ["Pos Match", "Total Campaign", "OCC", mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "occ")), :UDJ_AVG_EXPSD_HH_PRE]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "occ")), :UDJ_AVG_CNTRL_HH_PRE]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "occ")), :UDJ_AVG_EXPSD_HH_PST]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "occ")), :UDJ_AVG_CNTRL_HH_PST])]);
    push!(match_dfd, ["Pos Match", "Total Campaign", "DOLOCC", mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "dolocc")), :UDJ_AVG_EXPSD_HH_PRE]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "dolocc")), :UDJ_AVG_CNTRL_HH_PRE]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "dolocc")), :UDJ_AVG_EXPSD_HH_PST]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "dolocc")), :UDJ_AVG_CNTRL_HH_PST])]);
    push!(match_dfd, ["Pos Match", "Total Campaign", "DOLHH", mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "dolhh")), :UDJ_AVG_EXPSD_HH_PRE]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "dolhh")), :UDJ_AVG_CNTRL_HH_PRE]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "dolhh")), :UDJ_AVG_EXPSD_HH_PST]), mean(dfss[((dfss[:MODEL_DESC] .== "Total Campaign") .& (dfss[:dependent_variable] .== "dolhh")), :UDJ_AVG_CNTRL_HH_PST])]);

    match_dfd_dol = combine(groupby(match_dfd[match_dfd[:MODEL] .!= "DOLHH", :], :DESC), :UDJ_AVG_EXPSD_HH_PRE => prod => :UDJ_AVG_EXPSD_HH_PRE, :UDJ_AVG_CNTRL_HH_PRE => prod => :UDJ_AVG_CNTRL_HH_PRE, :UDJ_AVG_EXPSD_HH_PST => prod => :UDJ_AVG_EXPSD_HH_PST, :UDJ_AVG_CNTRL_HH_PST => prod => :UDJ_AVG_CNTRL_HH_PST);
    match_dfd_dol[:MODEL] = "DOLHH3WAY";
    match_dfd_dol[:MODEL_DESC] = "Total Campaign";
    match_dfd_f = vcat(match_dfd, match_dfd_dol);
    sort!(match_dfd_f, [:DESC, :MODEL], rev = true);

    match_dfd_f[:UDJ_DOD_EFFCT] = ((match_dfd_f[:UDJ_AVG_EXPSD_HH_PST] .- match_dfd_f[:UDJ_AVG_EXPSD_HH_PRE]) .- (match_dfd_f[:UDJ_AVG_CNTRL_HH_PST] .- match_dfd_f[:UDJ_AVG_CNTRL_HH_PRE])) ./ match_dfd_f[:UDJ_AVG_CNTRL_HH_PST] * 100;
    match_dfd_f[:UDJ_DIFF_EFFCT] = (match_dfd_f[:UDJ_AVG_EXPSD_HH_PST] .- match_dfd_f[:UDJ_AVG_CNTRL_HH_PST]) ./ match_dfd_f[:UDJ_AVG_CNTRL_HH_PST] * 100;

    for i in 1:ncol(match_dfd_f)
        if  eltype(match_dfd_f[i]) == Float64
            match_dfd_f[i] = round.(match_dfd_f[i], digits=4);
        end
    end
    
    match_dfd_f[:Product] = "Target Brand";

    return match_dfd_f
end

function pvalues_prematch(df::DataFrame)
    pen_p1 = lm(@formula(buyer_pre_p1 ~ 1 + group), df);
    occ_p1  = lm(@formula(trps_pre_p1 ~ 1 + group), df[df[:buyer_pre_p1] .== 1, :]);
    dolocc_p1 = lm(@formula(dol_per_trip_pre_p1 ~ 1 + group), df[df[:buyer_pre_p1] .== 1, :]);
    dolhh_p1 = lm(@formula(prd_1_net_pr_pre ~ 1 + group), df);
    pen_p0 = lm(@formula(buyer_pre_p0 ~ 1 + group), df);
    occ_p0 = lm(@formula(trps_pre_p0 ~ 1 + group), df[df[:buyer_pre_p0] .== 1, :]);
    dolocc_p0 = lm(@formula(dol_per_trip_pre_p0 ~ 1 + group), df[df[:buyer_pre_p0] .== 1, :]);
    dolhh_p0 = lm(@formula(prd_0_net_pr_pre ~ 1 + group), df);
    match_pval = DataFrame(DESC = String[], MODEL_DESC=String[], MODEL=String[], Product = String[], P_Val = Float64[]);
    push!(match_pval, ["Pre Match", "Total Campaign", "PEN", "Target Brand", coeftable(pen_p1).cols[4][2]]);
    push!(match_pval, ["Pre Match", "Total Campaign", "OCC", "Target Brand", coeftable(occ_p1).cols[4][2]]);
    push!(match_pval, ["Pre Match", "Total Campaign", "DOLOCC", "Target Brand", coeftable(dolocc_p1).cols[4][2]]);
    push!(match_pval, ["Pre Match", "Total Campaign", "DOLHH", "Target Brand", coeftable(dolhh_p1).cols[4][2]]);
    push!(match_pval, ["Pre Match", "Total Campaign", "PEN", "Category", coeftable(pen_p0).cols[4][2]]);
    push!(match_pval, ["Pre Match", "Total Campaign", "OCC", "Category", coeftable(occ_p0).cols[4][2]]);
    push!(match_pval, ["Pre Match", "Total Campaign", "DOLOCC", "Category", coeftable(dolocc_p0).cols[4][2]]);
    push!(match_pval, ["Pre Match", "Total Campaign", "DOLHH", "Category", coeftable(dolhh_p0).cols[4][2]]);

    return match_pval
end

function pvalues_posmatch(i::Int64, res::DataStructures.OrderedDict{Any,Any}, df::DataFrame)
    dfd = df[[Array(sort([res[1][:mtch][(res[1][:mtch][:samp] .== i), :orid]; res[1][:mtch][(res[1][:mtch][:samp] .== i), :ctrl]]))][1], :];
    pen_p1 = lm(@formula(buyer_pos_p1 ~ 1 + group), dfd);
    occ_p1 = lm(@formula(trps_pos_p1 ~ 1 + group), dfd[dfd[:buyer_pos_p1] .== 1, :]);
    dolocc_p1 = lm(@formula(dol_per_trip_pos_p1 ~ 1 + group), dfd[dfd[:buyer_pos_p1] .== 1, :]);
    dolhh_p1 = lm(@formula(prd_1_net_pr_pos ~ 1 + group), dfd);
    pen_p0 = lm(@formula(buyer_pos_p0 ~ 1 + group), dfd);
    occ_p0 = lm(@formula(trps_pos_p0 ~ 1 + group), dfd[dfd[:buyer_pos_p0] .== 1, :]);
    dolocc_p0 = lm(@formula(dol_per_trip_pos_p0 ~ 1 + group), dfd[dfd[:buyer_pos_p0] .== 1, :]);
    dolhh_p0 = lm(@formula(prd_0_net_pr_pos ~ 1 + group), dfd);
    match_post_pval = DataFrame(DESC = String[], MODEL_DESC = String[], MODEL = String[], Product = String[], P_Val = Float64[], samp = Int64[]);
    push!(match_post_pval, ["Pos Match", "Total Campaign", "PEN", "Target Brand", coeftable(pen_p1).cols[4][2], i]);
    push!(match_post_pval, ["Pos Match", "Total Campaign", "OCC", "Target Brand", coeftable(occ_p1).cols[4][2], i]);
    push!(match_post_pval, ["Pos Match", "Total Campaign", "DOLOCC", "Target Brand", coeftable(dolocc_p1).cols[4][2], i]);
    push!(match_post_pval, ["Pos Match", "Total Campaign", "DOLHH", "Target Brand", coeftable(dolhh_p1).cols[4][2], i]);
    push!(match_post_pval, ["Pos Match", "Total Campaign", "PEN", "Category", coeftable(pen_p0).cols[4][2], i]);
    push!(match_post_pval, ["Pos Match", "Total Campaign", "OCC", "Category", coeftable(occ_p0).cols[4][2], i]);
    push!(match_post_pval, ["Pos Match", "Total Campaign", "DOLOCC", "Category", coeftable(dolocc_p0).cols[4][2], i]);
    push!(match_post_pval, ["Pos Match", "Total Campaign", "DOLHH", "Category", coeftable(dolhh_p0).cols[4][2], i]);

    return match_post_pval
end

function match_prop_qc(i::Int64, res::DataStructures.OrderedDict{Any,Any}, df::DataFrame)
    dfd = df[[Array(sort([res[1][:mtch][(res[1][:mtch][:samp] .== i), :orid]; res[1][:mtch][(res[1][:mtch][:samp] .== i), :ctrl]]))][1], :];
    match_dfd_cat = DataFrame(DESC = String[], MODEL_DESC = String[], MODEL = String[], UDJ_AVG_EXPSD_HH_PRE = Float64[], UDJ_AVG_CNTRL_HH_PRE = Float64[], UDJ_AVG_EXPSD_HH_PST = Float64[], UDJ_AVG_CNTRL_HH_PST = Float64[], samp = Int64[]);
    push!(match_dfd_cat, ["Pos Match", "Total Campaign", "PEN", mean(dfd[dfd[:group] .== 1, :buyer_pre_p0]), mean(dfd[dfd[:group] .== 0, :buyer_pre_p0]), mean(dfd[dfd[:group] .== 1, :buyer_pos_p0]), mean(dfd[dfd[:group] .== 0, :buyer_pos_p0]), i]);
    push!(match_dfd_cat, ["Pos Match", "Total Campaign", "OCC", mean(dfd[((dfd[:group] .== 1) .& (dfd[:buyer_pre_p0] .== 1)), :trps_pre_p0]), mean(dfd[((dfd[:group] .== 0) .& (dfd[:buyer_pre_p0] .== 1)), :trps_pre_p0]), mean(dfd[((dfd[:group] .== 1) .& (dfd[:buyer_pos_p0] .== 1)), :trps_pos_p0]), mean(dfd[((dfd[:group] .== 0) .& (dfd[:buyer_pos_p0] .== 1)), :trps_pos_p0]), i]);
    push!(match_dfd_cat, ["Pos Match", "Total Campaign", "DOLOCC", mean(dfd[((dfd[:group] .== 1) .& (dfd[:buyer_pre_p0] .== 1)), :dol_per_trip_pre_p0]), mean(dfd[((dfd[:group] .== 0) .& (dfd[:buyer_pre_p0] .== 1)), :dol_per_trip_pre_p0]), mean(dfd[((dfd[:group] .== 1) .& (dfd[:buyer_pos_p0] .== 1)), :dol_per_trip_pos_p0]), mean(dfd[((dfd[:group] .== 0) .& (dfd[:buyer_pos_p0] .== 1)), :dol_per_trip_pos_p0]), i]);
    push!(match_dfd_cat, ["Pos Match", "Total Campaign", "DOLHH", mean(dfd[dfd[:group] .== 1, :prd_0_net_pr_pre]), mean(dfd[dfd[:group] .== 0, :prd_0_net_pr_pre]), mean(dfd[dfd[:group] .== 1, :prd_0_net_pr_pos]), mean(dfd[dfd[:group] .== 0, :prd_0_net_pr_pos]), i]);

    match_dfd_cat_dol = combine(groupby(match_dfd_cat[match_dfd_cat[:MODEL] .!= "DOLHH", :], :DESC), :UDJ_AVG_EXPSD_HH_PRE => prod => :UDJ_AVG_EXPSD_HH_PRE, :UDJ_AVG_CNTRL_HH_PRE => prod => :UDJ_AVG_CNTRL_HH_PRE, :UDJ_AVG_EXPSD_HH_PST => prod => :UDJ_AVG_EXPSD_HH_PST, :UDJ_AVG_CNTRL_HH_PST => prod => :UDJ_AVG_CNTRL_HH_PST);
    match_dfd_cat_dol[:MODEL] = "DOLHH3WAY";
    match_dfd_cat_dol[:MODEL_DESC] = "Total Campaign";
    match_dfd_cat_dol[:samp] = i;
    match_dfd_cat_f = vcat(match_dfd_cat, match_dfd_cat_dol);
    sort!(match_dfd_cat_f, [:DESC,:MODEL], rev = true);

    match_dfd_cat_f[:UDJ_DOD_EFFCT] = ((match_dfd_cat_f[:UDJ_AVG_EXPSD_HH_PST] .- match_dfd_cat_f[:UDJ_AVG_EXPSD_HH_PRE]) .- (match_dfd_cat_f[:UDJ_AVG_CNTRL_HH_PST] .- match_dfd_cat_f[:UDJ_AVG_CNTRL_HH_PRE])) ./ match_dfd_cat_f[:UDJ_AVG_CNTRL_HH_PST] * 100;
    match_dfd_cat_f[:UDJ_DIFF_EFFCT] = (match_dfd_cat_f[:UDJ_AVG_EXPSD_HH_PST] .- match_dfd_cat_f[:UDJ_AVG_CNTRL_HH_PST]) ./ match_dfd_cat_f[:UDJ_AVG_CNTRL_HH_PST] * 100;
    match_dfd_cat_f[:Product] = "Category";
    
    return match_dfd_cat_f
end

function match_qc_ptsb(df::DataFrame, res::DataStructures.OrderedDict{Any,Any}, dfs::DataFrame)
    match_metrics_tar = match_metric_qc(df, dfs);
    match_dfd_cat_pre = DataFrame(DESC = String[], MODEL_DESC=String[], MODEL=String[], UDJ_AVG_EXPSD_HH_PRE = Float64[], UDJ_AVG_CNTRL_HH_PRE = Float64[], UDJ_AVG_EXPSD_HH_PST = Float64[], UDJ_AVG_CNTRL_HH_PST = Float64[]);
    push!(match_dfd_cat_pre, ["Pre Match", "Total Campaign", "PEN", mean(df[df[:group] .== 1, :buyer_pre_p0]), mean(df[df[:group] .== 0, :buyer_pre_p0]), mean(df[df[:group] .== 1, :buyer_pos_p0]), mean(df[df[:group] .== 0, :buyer_pos_p0])]);
    push!(match_dfd_cat_pre, ["Pre Match", "Total Campaign", "OCC", mean(df[((df[:group] .== 1) .& (df[:buyer_pre_p0] .== 1)), :trps_pre_p0]), mean(df[((df[:group] .== 0) .& (df[:buyer_pre_p0] .== 1)), :trps_pre_p0]), mean(df[((df[:group] .== 1) .& (df[:buyer_pos_p0] .== 1)), :trps_pos_p0]), mean(df[((df[:group] .== 0) .& (df[:buyer_pos_p0] .== 1)), :trps_pos_p0])]);
    push!(match_dfd_cat_pre, ["Pre Match", "Total Campaign", "DOLOCC", mean(df[((df[:group] .== 1) .& (df[:buyer_pre_p0] .== 1)), :dol_per_trip_pre_p0]), mean(df[((df[:group] .== 0) .& (df[:buyer_pre_p0] .== 1)), :dol_per_trip_pre_p0]), mean(df[((df[:group] .== 1) .& (df[:buyer_pos_p0] .== 1)), :dol_per_trip_pos_p0]), mean(df[((df[:group] .== 0) .& (df[:buyer_pos_p0] .== 1)), :dol_per_trip_pos_p0])]);
    push!(match_dfd_cat_pre, ["Pre Match", "Total Campaign", "DOLHH", mean(df[df[:group] .== 1, :prd_0_net_pr_pre]), mean(df[df[:group] .== 0, :prd_0_net_pr_pre]), mean(df[df[:group] .== 1, :prd_0_net_pr_pos]), mean(df[df[:group] .== 0, :prd_0_net_pr_pos])]);

    match_dfd_cat_pre_dolhh = combine(groupby(match_dfd_cat_pre[match_dfd_cat_pre[:MODEL] .!= "DOLHH" , :], :DESC), :UDJ_AVG_EXPSD_HH_PRE => prod => :UDJ_AVG_EXPSD_HH_PRE, :UDJ_AVG_CNTRL_HH_PRE => prod => :UDJ_AVG_CNTRL_HH_PRE, :UDJ_AVG_EXPSD_HH_PST => prod => :UDJ_AVG_EXPSD_HH_PST, :UDJ_AVG_CNTRL_HH_PST => prod => :UDJ_AVG_CNTRL_HH_PST);
    match_dfd_cat_pre_dolhh[:MODEL] = "DOLHH3WAY";
    match_dfd_cat_pre_dolhh[:MODEL_DESC] = "Total Campaign";
    match_dfd_cat_pre = vcat(match_dfd_cat_pre, match_dfd_cat_pre_dolhh);
    match_dfd_cat_pre[:Product] = "Category";
    match_dfd_cat_pre[:UDJ_DOD_EFFCT] = ((match_dfd_cat_pre[:UDJ_AVG_EXPSD_HH_PST] .- match_dfd_cat_pre[:UDJ_AVG_EXPSD_HH_PRE]) .- (match_dfd_cat_pre[:UDJ_AVG_CNTRL_HH_PST] .- match_dfd_cat_pre[:UDJ_AVG_CNTRL_HH_PRE])) ./ match_dfd_cat_pre[:UDJ_AVG_CNTRL_HH_PST] * 100;
    match_dfd_cat_pre[:UDJ_DIFF_EFFCT] = (match_dfd_cat_pre[:UDJ_AVG_EXPSD_HH_PST] .- match_dfd_cat_pre[:UDJ_AVG_CNTRL_HH_PST]) ./ match_dfd_cat_pre[:UDJ_AVG_CNTRL_HH_PST] * 100;

    match_dfd_cat_f = DataFrame();
    for i in 1: length(unique(res[1][:mtch][:samp]))
        match_dfd_cat_f = vcat(match_dfd_cat_f, match_prop_qc(i, res, df));
    end

    match_dfd_cat = combine(groupby(match_dfd_cat_f, [:DESC, :MODEL_DESC, :MODEL, :Product]), :UDJ_AVG_EXPSD_HH_PRE => mean => :UDJ_AVG_EXPSD_HH_PRE, :UDJ_AVG_CNTRL_HH_PRE => mean => :UDJ_AVG_CNTRL_HH_PRE, :UDJ_AVG_EXPSD_HH_PST => mean => :UDJ_AVG_EXPSD_HH_PST, :UDJ_AVG_CNTRL_HH_PST => mean => :UDJ_AVG_CNTRL_HH_PST);
    match_dfd_cat[:UDJ_DOD_EFFCT] = ((match_dfd_cat[:UDJ_AVG_EXPSD_HH_PST] .- match_dfd_cat[:UDJ_AVG_EXPSD_HH_PRE]) .- (match_dfd_cat[:UDJ_AVG_CNTRL_HH_PST] .- match_dfd_cat[:UDJ_AVG_CNTRL_HH_PRE])) ./ match_dfd_cat[:UDJ_AVG_CNTRL_HH_PST] * 100;
    match_dfd_cat[:UDJ_DIFF_EFFCT] = (match_dfd_cat[:UDJ_AVG_EXPSD_HH_PST] .- match_dfd_cat[:UDJ_AVG_CNTRL_HH_PST]) ./ match_dfd_cat[:UDJ_AVG_CNTRL_HH_PST] * 100;
    match_dfd_final = vcat(match_dfd_cat, match_dfd_cat_pre, match_metrics_tar);
    sort!(match_dfd_final, [:Product, :DESC, :MODEL_DESC, :MODEL], rev = [false, true, true, true]);

    for i in 1:ncol(match_dfd_final)
        if  eltype(match_dfd_final[i]) == Float64
            match_dfd_final[i] = round.(match_dfd_final[i], digits=4);
        end
    end

    match_dfd_final = match_dfd_final[match_dfd_final[:MODEL] .!= "DOLHH3WAY", :];
    pre_match_pval = pvalues_prematch(df);

    post_match = DataFrame();
    for i in 1:length(unique(res[1][:mtch][:samp]))
        post_match = vcat(post_match, pvalues_posmatch(i, res, df));
    end

    post_match_p = combine(groupby(post_match, [:DESC, :MODEL_DESC, :MODEL, :Product]), :P_Val => minimum => :P_Val_min, :P_Val => mean => :P_Val_mean, :P_Val => maximum => :P_Val_max);
    match_dfd_final_1 = leftjoin(match_dfd_final, pre_match_pval, on = [:DESC, :MODEL_DESC, :MODEL, :Product]);
    match_dfd_final_2 = leftjoin(match_dfd_final_1, post_match_p, on = [:DESC, :MODEL_DESC, :MODEL, :Product]);

    for i in 1:ncol(match_dfd_final_2)
        if eltype(match_dfd_final_2[i]) == Float64
            match_dfd_final_2[i] = round.(match_dfd_final_2[i], digits=4);
        end
    end

    return match_dfd_final_2

end
end

