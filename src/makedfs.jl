# Julia v1.5.0
# The function requires CSV, DataFrames, and Statistics packages
# The function takes the df and res data frames and output a dfs-like dataframe for QC purposes


function makedfs(df::DataFrame, res::DataFrame)
    res_expsd = res[!, [:orid, :samp]];
    res_expsd[:group] = 1;
    res_cntrl = res[!, [:ctrl, :samp]];
    rename!(res_cntrl, :ctrl => :orid);
    res_cntrl[:group] = 0;
    df_match = append!(res_expsd, res_cntrl);
    # df_match = df_match[df_match[:samp] .<= 3, :];    # --> to be removed - here only for testing purposes
    df_all = leftjoin(df_match, df, on = [:orid, :group]);

    gdf = groupby(df_all, :samp);
    dep_vars = Dict("pen"=>Dict("pre"=>:buyer_pre_p1, "pos"=>:buyer_pos_p1), "occ"=>Dict("pre"=>:trps_pre_p1, "pos"=>:trps_pos_p1), "dolocc"=>Dict("pre"=>:dol_per_trip_pre_p1, "pos"=>:dol_per_trip_pos_p1), "dolhh"=>Dict("pre"=>:prd_1_net_pr_pre, "pos"=>:prd_1_net_pr_pos));
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
            dfs[vr] = 0.0
        else
            dfs[vr] = 0
        end
    end

    dfs[:brk] = 1;
    dfs[:MODEL_DESC] = "Total Campaign";

    return dfs
end
