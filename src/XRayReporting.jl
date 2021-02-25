# Julia v1.5.3

module XRayReporting

include(joinpath(dirname(@__FILE__), "XRayDataPrep.jl"))

using CSV, DataFrames, DataStructures, Dates, Statistics, StatsBase

export viz_RUN, format_unify_reports

# CDMA Reporting functions
function buyerfreqs(group::Int, definition::Dict{Symbol,Any}, df_cdma_dump::DataFrame)
    if haskey(definition, :buyer_pos_p1) && haskey(definition, :buyer_pre_52w_p1) && haskey(definition, :buyer_pre_52w_p0)
        buyers_count = nrow(df_cdma_dump[(df_cdma_dump[:group] .== group) .& (df_cdma_dump[:buyer_pos_p1] .== definition[:buyer_pos_p1]) .& (df_cdma_dump[:buyer_pre_52w_p1] .== definition[:buyer_pre_52w_p1]) .& (df_cdma_dump[:buyer_pre_52w_p0] .== definition[:buyer_pre_52w_p0]), :]);
    elseif haskey(definition, :buyer_pos_p1) && haskey(definition, :buyer_pre_52w_p1) && haskey(definition, :trps_pos_p1)
        buyers_count = nrow(df_cdma_dump[(df_cdma_dump[:group] .== group) .& (df_cdma_dump[:buyer_pos_p1] .== definition[:buyer_pos_p1]) .& (df_cdma_dump[:buyer_pre_52w_p1] .== definition[:buyer_pre_52w_p1]) .& (df_cdma_dump[:trps_pos_p1] .> definition[:trps_pos_p1]), :]);
    elseif haskey(definition, :buyer_pos_p1) && haskey(definition, :buyer_pre_52w_p1)
        buyers_count = nrow(df_cdma_dump[(df_cdma_dump[:group] .== group) .& (df_cdma_dump[:buyer_pos_p1] .== definition[:buyer_pos_p1]) .& (df_cdma_dump[:buyer_pre_52w_p1] .== definition[:buyer_pre_52w_p1]), :]);
    elseif haskey(definition, :buyer_pos_p1)
        buyers_count = nrow(df_cdma_dump[(df_cdma_dump[:group] .== group) .& (df_cdma_dump[:buyer_pos_p1] .== definition[:buyer_pos_p1]), :]);
    else
        buyers_count = nrow(df_cdma_dump[df_cdma_dump[:group] .== group, :]);
    end

    return buyers_count
end

function buyerfactor(buyerclass::Dict{Symbol,Int64}, scored_factor::Float64)
    tot_buyers = buyerclass[:lapsed_buyers]+buyerclass[:non_buyers]+buyerclass[:brand_buyers];
    buyers_ratio = buyerclass[:brand_buyers]/tot_buyers;
    adj_factor = scored_factor/buyers_ratio;

    return adj_factor, tot_buyers
end

function buyermetrics(buyerclass::Dict{Symbol,Int64}, buyer_dict::Dict{Symbol,Any}, dfo::DataFrame)
    r = buyer_dict[:buyer_type];
    adj_factor, tot_buyers = buyerfactor(buyerclass, buyer_dict[:udj_avg_hh_pst]);

    for (k, v) in buyerclass
        classname = uppercase(replace(string(k), "_" => " "));
        if string(k) == "non_buyers"
            val1 = 1-((1-(v/tot_buyers))*adj_factor);
            val2 = (tot_buyers)-((buyerclass[:lapsed_buyers]+buyerclass[:brand_buyers])*adj_factor);
        else
            val1 = v/tot_buyers*adj_factor;
            val2 = v*adj_factor;
        end
        push!(dfo, [r, classname, val1, Int64(round(val2))]);
    end

    return dfo
end

function triermetrics(trierclass::Dict{Symbol,Int64}, buyerclass::Dict{Symbol,Int64}, buyer_dict::Dict{Symbol,Any}, dfo::DataFrame)
    r = :trial_repeat;
    adj_factor, tot_buyers = buyerfactor(buyerclass, buyer_dict[:udj_avg_hh_pst]);

    for (k, v) in trierclass
        classname = string(k)*"_percent";
        if string(k) == "repeaters"
            val1 = v/trierclass[:triers];
        elseif  string(k) == "triers"
            val1 = v/trierclass[:cat]*adj_factor;
        else
            val1 = v/trierclass[:cat];
        end
        val2 = v*adj_factor;
        push!(dfo, [r, buyer_dict[:group], classname, val1, Int64(round(val2))]);
    end

    return dfo
end

function genbuyerclass(df_cdma_dump::DataFrame, udj_avg_expsd_pst::Float64, udj_avg_cntrl_pst::Float64)
    #= For exposed and unexposed (controls), compute the number of HHs and the proportion of HHs in each buyer category. =#
    exp_dict = Dict{Symbol,Any}(:group=>1, :buyer_type=>:buyer_exposed, :udj_avg_hh_pst=>udj_avg_expsd_pst);
    unexp_dict = Dict{Symbol,Any}(:group=>0, :buyer_type=>:buyer_unexposed, :udj_avg_hh_pst=>udj_avg_cntrl_pst);
    exp_unexp_dict = Dict{Symbol,Dict}(:exp=>exp_dict, :unexp=>unexp_dict);

    lapsed_buyers = Dict{Symbol,Any}(:name=>:lapsed_buyers, :buyer_pos_p1=>0, :buyer_pre_52w_p1=>1);
    non_buyers = Dict{Symbol,Any}(:name=>:non_buyers, :buyer_pos_p1=>0, :buyer_pre_52w_p1=>0);
    brand_buyers = Dict{Symbol,Any}(:name=>:brand_buyers, :buyer_pos_p1=>1);
    new_buyers = Dict{Symbol,Any}(:name=>:new_buyers, :buyer_pos_p1=>1, :buyer_pre_52w_p1=>0);
    repeat_buyers = Dict{Symbol,Any}(:name=>:repeat_buyers, :buyer_pos_p1=>1, :buyer_pre_52w_p1=>1);
    category_switchers = Dict{Symbol,Any}(:name=>:category_switchers, :buyer_pos_p1=>1, :buyer_pre_52w_p1=>0, :buyer_pre_52w_p0=>0);
    brand_switchers = Dict{Symbol,Any}(:name=>:brand_switchers, :buyer_pos_p1=>1, :buyer_pre_52w_p1=>0, :buyer_pre_52w_p0=>1);
    definitions = [lapsed_buyers, non_buyers, brand_buyers, new_buyers, repeat_buyers, category_switchers, brand_switchers];

    dfo = DataFrame(reptype=Symbol[], desc=String[], val=Float64[], cnt=Int64[]);
    for k in keys(exp_unexp_dict)
        buyerclass = Dict{Symbol,Int64}(Symbol(def[:name])=>buyerfreqs(exp_unexp_dict[k][:group], def, df_cdma_dump) for def in definitions);
        dfo = buyermetrics(buyerclass, exp_unexp_dict[k], dfo);    # Calculations different from documentation: expected count and % of buyers!
    end

    dfo_Exp = DataFrame(buyer_type = dfo[dfo[:reptype] .== :buyer_exposed, :desc], buyer_percent = dfo[dfo[:reptype] .== :buyer_exposed, :val], CNT = dfo[dfo[:reptype] .== :buyer_exposed, :cnt]);
    dfo_UnExp = DataFrame(buyer_type = dfo[dfo[:reptype] .== :buyer_unexposed, :desc], buyer_percent = dfo[dfo[:reptype] .== :buyer_unexposed, :val], CNT = dfo[dfo[:reptype] .== :buyer_unexposed, :cnt]);
# NOTE: DATA FRAME CONTENT IS SAME AS IN ORIGINAL CDMA CODE, BUT ROW ORDER IS DIFFERENT (not sure it matters)
    return dfo_Exp, dfo_UnExp
end

function gentrialrepeat(df_cdma_dump::DataFrame, udj_avg_expsd_pst::Float64, udj_avg_cntrl_pst::Float64)    # NOT USED IN VISUALIZATION TOOL! This is only needed for the format_unify_reports function.
    #= For exposed and unexposed (controls), compute the number and the proportion of HHs that are new buyers ("triers": they purchase target brand in campaign period but not in the 52 weeks before) and new recurring buyers ("new repeating buyers": they purchase target brand in campaign period more than 1 times but never in the 52 weeks before) among the category buyers (it's actually the exposed/unexposed HHs adjusted per udj_avg_hh_pst). =#
    exp_dict = Dict{Symbol,Any}(:group=>1, :buyer_type=>:buyer_exposed, :udj_avg_hh_pst=>udj_avg_expsd_pst);
    unexp_dict = Dict{Symbol,Any}(:group=>0, :buyer_type=>:buyer_unexposed, :udj_avg_hh_pst=>udj_avg_cntrl_pst);
    exp_unexp_dict = Dict{Symbol,Dict}(:exp=>exp_dict, :unexp=>unexp_dict);

    lapsed_buyers = Dict{Symbol,Any}(:name=>:lapsed_buyers, :buyer_pos_p1=>0, :buyer_pre_52w_p1=>1);
    non_buyers = Dict{Symbol,Any}(:name=>:non_buyers, :buyer_pos_p1=>0, :buyer_pre_52w_p1=>0);
    brand_buyers = Dict{Symbol,Any}(:name=>:brand_buyers, :buyer_pos_p1=>1);
    definitions = [lapsed_buyers, non_buyers, brand_buyers];

    trial_buyers = Dict{Symbol,Any}(:name=>:triers, :buyer_pos_p1=>1, :buyer_pre_52w_p1=>0);
    repeating_buyers = Dict{Symbol,Any}(:name=>:repeaters, :buyer_pos_p1=>1, :buyer_pre_52w_p1=>0, :trps_pos_p1=>1);
    category_buyers = Dict{Symbol,Any}(:name=>:cat);
    definitions_trier = [trial_buyers, repeating_buyers, category_buyers];

    dfo = DataFrame(reptype=Symbol[], id=Int64[], desc=String[], val=Float64[] ,cnt=Int64[]);
    for k in keys(exp_unexp_dict)
        buyerclass = Dict{Symbol,Int64}(Symbol(def[:name])=>buyerfreqs(exp_unexp_dict[k][:group], def, df_cdma_dump) for def in definitions);
        trierclass = Dict{Symbol,Int64}(Symbol(def[:name])=>buyerfreqs(exp_unexp_dict[k][:group], def, df_cdma_dump) for def in definitions_trier);
        dfo = triermetrics(trierclass, buyerclass, exp_unexp_dict[k], dfo);    # Calculations different from documentation!!!
    end

    dfo_1 =  DataFrames.unstack(dfo, :id, :desc, :val);
    dfo_2 =  DataFrames.unstack(dfo, :id, :desc, :cnt);
    dfo_1 = dfo_1[sort(names(dfo_1), rev=true)];
    dfo_2 = dfo_2[sort(names(dfo_2), rev=true)];
    DataFrames.rename!(dfo_2, Symbol.(map(x->replace(string(x), "_percent" => "_cnt"), names(dfo_2))));
    dfo_f = DataFrames.innerjoin(dfo_1, dfo_2, on=:id);
    dfo_f[:grouptype] = [x == 1 ? "Exposed" : "Unexposed" for x in dfo_f[:id]];
    select!(dfo_f, DataFrames.Not(:id));
    dfo_f = sort(dfo_f[vcat(:grouptype, Symbol.(filter!(x -> string(x)!=string(:grouptype), names(dfo_f))))]);
    dfo_f[!, DataFrames.Not(:grouptype)] = identity.(dfo_f[!, DataFrames.Not(:grouptype)]);   # Convert to TYPE from Union{Missing, TYPE}

    return dfo_f
end

function fairshare(df_cdma_dump::DataFrame, brand_data::DataFrame)
    #= For buyers of target brand in post-campaign period who are exposed, compute the proportion of change in target brand dollar sales coming from the change in competitor product dollar sales (`pp_of_feature_brand` = change of competitor product dollar sales/change of target brand dollar sales). Then, to obtain `fair_share_index`, adjust it for the pre-campaign proportion of competitor dollar sales of total competitor sales. =#
    brand_data2 = deepcopy(brand_data);
    DataFrames.rename!(brand_data2, :product_id => :id, :group_name => :product);

    prd_net_pr_pos = [:prd_1_net_pr_pos, :prd_2_net_pr_pos, :prd_3_net_pr_pos, :prd_4_net_pr_pos, :prd_5_net_pr_pos, :prd_6_net_pr_pos, :prd_7_net_pr_pos, :prd_8_net_pr_pos, :prd_9_net_pr_pos, :prd_10_net_pr_pos];
    prd_net_pr_pre = [:prd_1_net_pr_pre, :prd_2_net_pr_pre, :prd_3_net_pr_pre, :prd_4_net_pr_pre, :prd_5_net_pr_pre, :prd_6_net_pr_pre, :prd_7_net_pr_pre, :prd_8_net_pr_pre, :prd_9_net_pr_pre, :prd_10_net_pr_pre];

    prd_sales_sums = combine(DataFrames.groupby(df_cdma_dump[(df_cdma_dump[:group] .== 1) .& (df_cdma_dump[:buyer_pos_p1] .== 1), vcat([:group, :buyer_pos_p1], prd_net_pr_pos, prd_net_pr_pre)], [:group, :buyer_pos_p1]), vcat(prd_net_pr_pos, prd_net_pr_pre) .=> sum);
    df = DataFrames.stack(prd_sales_sums, DataFrames.Not([:group, :buyer_pos_p1]));
    df[:id] = map(x -> parse(Int, split(string(x), "_")[2]), df[:variable]);
    df[:desc] = map(x -> split(string(x), "_")[end-1]*"_sales", df[:variable]);
    df = df[:, [:id, :desc, :value]];
    totals = combine(DataFrames.groupby(df, :desc), :value => sum);
    df[:percent] = 0.0;
    for salestype in unique(totals[:desc])
        df[df[:desc] .== salestype, :percent] = df[df[:desc] .== salestype, :value]./totals[totals[:desc] .== salestype, :value_sum];
    end

    df_val = DataFrames.unstack(df, :id, :desc, :value);
    df_pct = DataFrames.unstack(df, :id, :desc, :percent);
    DataFrames.rename!(df_pct, map(x -> x != "id" ? Symbol(string(x)*"_pct") : Symbol(x), names(df_pct)));
    df_pct[:pct_change] = df_pct[:pos_sales_pct]-df_pct[:pre_sales_pct];
    targetbrand_pct_change = df_pct[(df_pct[:id] .== 1), :pct_change][1];
    df_pct[:pct_of_targetbrand] = df_pct[:pct_change]./(-targetbrand_pct_change);
    df_agg = DataFrames.innerjoin(df_val, df_pct, on = :id);
    df_agg[:fair_share_index] = df_agg[:pct_of_targetbrand]./(df_agg[:pre_sales]/sum(df_agg[df_agg[:id] .!= 1, :pre_sales]))*100;
    df_agg[df_agg[:id] .== 1, [:pct_of_targetbrand, :fair_share_index]] = 0.0;
    df_agg = df_agg[(df_agg[:pre_sales] .!= 0) .& (df_agg[:pos_sales] .!= 0), :];
    agg_fair_share_index = DataFrames.leftjoin(df_agg, brand_data2, on = :id);
    ordered_cols = [:id, :product, :pos_sales, :pos_sales_pct, :pre_sales, :pre_sales_pct, :pct_change, :pct_of_targetbrand, :fair_share_index];
    agg_fair_share_index = agg_fair_share_index[ordered_cols];
    agg_fair_share_index[!, DataFrames.Not([:id, :pct_change, :pct_of_targetbrand, :fair_share_index])] = identity.(agg_fair_share_index[!, DataFrames.Not([:id, :pct_change, :pct_of_targetbrand, :fair_share_index])]);   # Convert to TYPE from Union{Missing, TYPE}
    DataFrames.rename!(agg_fair_share_index, :id => :product_grp_id);

    return agg_fair_share_index
end

function targetshare_of_category(df_cdma_dump::DataFrame)    # NOT USED IN VISUALIZATION TOOL! - Previously called `Share_of_requirements`; compute target brand/category sales in post-campaign
    #= In the post-campaign period, calculate the target brand proportion of total category dollar sales for exposed and unexposed =#
    sales_sum = combine(DataFrames.groupby(df_cdma_dump[[:group, :prd_0_net_pr_pos, :prd_1_net_pr_pos]], :group), [:group, :prd_0_net_pr_pos, :prd_1_net_pr_pos] .=> sum);
    sales_sum[:product_group_share] = sales_sum[:prd_1_net_pr_pos_sum]./sales_sum[:prd_0_net_pr_pos_sum];
    agg_share_of_requirements = sales_sum[[:group, :product_group_share]];
    DataFrames.rename!(agg_share_of_requirements, :group => :exposed_flag);

    return agg_share_of_requirements
end

function upc_growth(df_cdma_dump::DataFrame, upc_data::DataFrame);
    #= Breakdown of contribution to the product growth during campaign period by UPC (only consider the positive contributions) =#
    upc_data2 = deepcopy(upc_data);
    dfupc = DataFrame(DESCRIPTION=[], UPC=[], pos_sales=[], pre_sales=[], pos_sales_share=[], pre_sales_share=[], GROWTH_SALES=[], growth_contribution=[]);
    DataFrames.rename!(upc_data2, :experian_id => :panid);
    j = DataFrames.innerjoin(upc_data2, df_cdma_dump[df_cdma_dump[:group] .== 1, [:panid,:group]], on = :panid);
    upc_1 = combine(DataFrames.groupby(j, [:period, :upc, :description]), :net_price => sum => :sales);
    upc_1_1 = upc_1[upc_1[:period] .== 1, [:upc, :description, :sales]];
    upc_1_2 = upc_1[upc_1[:period] .== 2, [:upc, :description, :sales]];
    upc_1_3 = DataFrames.outerjoin(upc_1_1, upc_1_2, on=[:upc, :description], makeunique=true);
    DataFrames.rename!(upc_1_3, :sales => :pre_sales);
    DataFrames.rename!(upc_1_3, :sales_1 => :pos_sales);
    upc_1_3[[:pre_sales]] = coalesce.(upc_1_3[[:pre_sales]], 0);
    upc_1_3[[:pos_sales]] = coalesce.(upc_1_3[[:pos_sales]], 0);
    upc_1_3[:pre_sales_share] = upc_1_3[:pre_sales]./sum(upc_1_3[:pre_sales]);
    upc_1_3[:pos_sales_share] = upc_1_3[:pos_sales]./sum(upc_1_3[:pos_sales]);
    upc_1_3[:growth_contribution] = (upc_1_3[:pos_sales].-upc_1_3[:pre_sales])/abs((sum(upc_1_3[:pos_sales]).-sum(upc_1_3[:pre_sales])))*100;
    sort!(upc_1_3, [:growth_contribution], rev=true);

    upc_1_3[:DESCRIPTION_WO_SIZE] = map(x -> join(split(x[1:findlast('-', x)-2])[1:end-2], ' '), upc_1_3[:description]);
    upc_1_3[:TYPE] = map(x -> split(x[1:findlast('-', x)-2])[end], upc_1_3[:description]);
    upc_1_3[:SIZE] = map(x -> parse(Float64, split(x[1:findlast('-', x)-2])[end-1]), upc_1_3[:description]);
    upc_1_4 = upc_1_3[upc_1_3[:pre_sales] .== 0, :];
    upc_1_5 = upc_1_3[upc_1_3[:pre_sales] .!= 0, :];

    upc_3 = unique(upc_1_5[:, [:SIZE, :TYPE, :DESCRIPTION_WO_SIZE]]);
    sort!(upc_3, [:DESCRIPTION_WO_SIZE, :TYPE, :SIZE]);
    upc_3[:LAG_SIZE] = upc_3[:SIZE].-vcat(0, upc_3[:SIZE][1:length(upc_3[:SIZE])-1]);
    upc_3[:PERCENT_LAG] = upc_3[:LAG_SIZE]./upc_3[:SIZE]*100;
    upc_3[:ROW_NN] = 1:nrow(upc_3);

    upc_4 = DataFrame();
    for i in DataFrames.groupby(upc_3, [:DESCRIPTION_WO_SIZE, :TYPE])
        i[:ROW_NN] = 1:nrow(i);
        upc_4 = vcat(i, upc_4);
    end
    upc_4[:LAG_SIZE] = map((x, y, z) -> ifelse(x == 1, y, z), upc_4[:ROW_NN], upc_4[:SIZE], upc_4[:LAG_SIZE]);
    upc_4[:PERCENT_LAG] = map((x, y) -> ifelse(x == 1, 100, y), upc_4[:ROW_NN], upc_4[:PERCENT_LAG]);
    sort!(upc_4, [:DESCRIPTION_WO_SIZE, :TYPE, :SIZE]);
    upc_4[:NEW_SIZE] = map((x, y) -> ifelse(x > 30, y, 0), upc_4[:PERCENT_LAG], upc_4[:SIZE]);
    for i in 2:nrow(upc_4)
        if upc_4[i, :NEW_SIZE] == 0;
            upc_4[i, :NEW_SIZE] = upc_4[i-1, :NEW_SIZE];
        end
    end

    sort!(upc_4, [:DESCRIPTION_WO_SIZE, :TYPE, :SIZE]);
    upc_5 = deepcopy(upc_4);
    upc_6 = deepcopy(upc_5);
    upc_6[:ROW_NN] = map(x -> x-1, upc_6[:ROW_NN]);

    upc_7 = DataFrames.leftjoin(upc_5, upc_6, on = [:ROW_NN, :TYPE, :DESCRIPTION_WO_SIZE], makeunique=true);
    upc_7 = upc_7[:, [:SIZE, :TYPE, :DESCRIPTION_WO_SIZE, :LAG_SIZE, :PERCENT_LAG_1, :ROW_NN, :NEW_SIZE]];
    DataFrames.rename!(upc_7, :PERCENT_LAG_1 => :PERCENT_LAG);
    sort!(upc_7, [:SIZE], rev=true);
    sort!(upc_7, [:TYPE, :DESCRIPTION_WO_SIZE]);
    upc_7[[:PERCENT_LAG]] = coalesce.(upc_7[[:PERCENT_LAG]], 100);
    upc_7[:NEW_SIZE] = map((x, y) -> ifelse(x > 30, y, 0), upc_7[:PERCENT_LAG], upc_7[:SIZE]);
    for i in 2:nrow(upc_7)
        if upc_7[i, :NEW_SIZE] == 0
            upc_7[i, :NEW_SIZE] = upc_7[i-1, :NEW_SIZE];
        end
    end
    sort!(upc_7, [:TYPE, :DESCRIPTION_WO_SIZE, :SIZE]);

    upc_8 = DataFrames.innerjoin(upc_4, upc_7, on = [:TYPE, :DESCRIPTION_WO_SIZE, :ROW_NN], makeunique=true);
    upc_8 = upc_8[:, [:DESCRIPTION_WO_SIZE, :TYPE, :SIZE, :ROW_NN, :NEW_SIZE, :NEW_SIZE_1]];
    DataFrames.rename!(upc_8, :NEW_SIZE => :LOW_SIZE);
    DataFrames.rename!(upc_8, :NEW_SIZE_1 => :HIGH_SIZE);
    upc_dummy = DataFrame(TYPE = unique(upc_3[:TYPE]));
    upc_8 = DataFrames.innerjoin(upc_8, upc_dummy, on = :TYPE);
    upc_8[:SIZE_LEVEL] = map((x, y, z) -> ifelse(x == y, string(y, " ", z), string("(", x, " ", z, " - ", y, " ", z, ")")), upc_8[:LOW_SIZE], upc_8[:HIGH_SIZE], upc_8[:TYPE]);
    upc_9 = DataFrames.innerjoin(upc_1_5, upc_8, on = [:SIZE, :TYPE, :DESCRIPTION_WO_SIZE]);
    upc_9 = combine(x -> DataFrame(pre_sales=sum(x[:pre_sales]), pos_sales=sum(x[:pos_sales]), AGG=nrow(x), UPC=minimum(x[:upc])), DataFrames.groupby(upc_9, [:DESCRIPTION_WO_SIZE, :SIZE_LEVEL]));
    upc_9[:GROWTH_SALES] = upc_9[:pos_sales].-upc_9[:pre_sales];

    if nrow(upc_1_4) > 0
        upc_10 = deepcopy(upc_1_4);
        upc_10[:SIZE_LEVEL] = map((x, y) -> string(x, " ", y), upc_10[:SIZE], upc_10[:TYPE]);
        upc_10 = combine(x -> DataFrame(pre_sales=sum(x[:pre_sales]), pos_sales=sum(x[:pos_sales]), AGG=nrow(x), UPC=minimum(x[:upc])), DataFrames.groupby(upc_10, [:DESCRIPTION_WO_SIZE, :SIZE_LEVEL]))
        upc_10[:GROWTH_SALES] = upc_10[:pos_sales].-upc_10[:pre_sales];
        upc_11 = vcat(upc_9, upc_10);
    else
        upc_11 = upc_9;
    end
    upc_11[:DESCRIPTION] = map((x, y) -> string(x, " - ", y), upc_11[:DESCRIPTION_WO_SIZE], upc_11[:SIZE_LEVEL]);
    upc_11[:pre_sales_share] = upc_11[:pre_sales]./sum(upc_11[:pre_sales]);
    upc_11[:pos_sales_share] = upc_11[:pos_sales]./sum(upc_11[:pos_sales]);

    upc_12 = upc_11[upc_11[:GROWTH_SALES] .> 0, :];
    upc_12[:growth_contribution] = (upc_12[:pos_sales].-upc_12[:pre_sales])/abs((sum(upc_12[:pos_sales]).-sum(upc_12[:pre_sales])));
    sort!(upc_12, [:growth_contribution], rev=true);
    upc_12[:UPC] = map((x, y) -> ifelse(x == 1, string(y), string("Aggregation of ", x, " UPCS")), upc_12[:AGG], upc_12[:UPC]);
    upc_final = upc_12[:, [:DESCRIPTION, :UPC, :pos_sales, :pre_sales, :pos_sales_share, :pre_sales_share, :GROWTH_SALES, :growth_contribution]];
    dfupc = vcat(dfupc, upc_final);

    DataFrames.rename!(dfupc, :DESCRIPTION => :description, :UPC => :upc10, :pos_sales => :sales_upc_post, :pre_sales => :sales_upc_pre, :pos_sales_share => :percentage_sales_upc_post, :pre_sales_share => :percentage_sales_upc_pre, :GROWTH_SALES => :growth_sales);
    dfupc = dfupc[:, [:description, :upc10, :sales_upc_pre, :sales_upc_post, :percentage_sales_upc_pre, :percentage_sales_upc_post, :growth_contribution]];
    upc_growth = deepcopy(dfupc);
    agg_upc_growth = DataFrame(description=[], upc10=[], sales_upc_pre=[], sales_upc_post=[], percentage_sales_upc_pre=[], percentage_sales_upc_post=[], growth_contribution=[]);
    for i in unique(upc_growth[:description])
        push!(agg_upc_growth, [i, upc_growth[upc_growth[:description] .== i, :upc10][1], mean(upc_growth[upc_growth[:description] .== i, :sales_upc_pre]), mean(upc_growth[upc_growth[:description] .== i, :sales_upc_post]), mean(upc_growth[upc_growth[:description] .== i, :percentage_sales_upc_pre]), mean(upc_growth[upc_growth[:description] .== i, :percentage_sales_upc_post]), mean(upc_growth[upc_growth[:description] .== i, :growth_contribution])]);
    end
    sort!(agg_upc_growth, :growth_contribution, rev=true);

    return agg_upc_growth
end

function freq_HH_Cum1stpur(combined::DataFrame, freq_index::DataFrame)
    #= For `First_Buy_by_Frequency_Digital_std`, compute the frequency (number of HHs) of exposures before first purchase (capped to 10 exposures), the cumulative frequency, and the cumulative frequency percentage of total exposures before the first purchase. For `First_Buy_by_Frequency_Digital_Dyn`, compute the frequency (number of HHs) of exposure levels (ranges of exposures) before first purchase (capped), the cumulative frequency, and the cumulative percentage of total exposure levels before the first purchase. =#
    #Calculate 1st Buy by Standard Frequency Buckets
    Exposed_Buyer = deepcopy(combined[combined[:Number_exposure_before_1st_buy] .!= 0, [:Number_exposure_before_1st_buy]]);
    Exposed_Buyer[Exposed_Buyer[:Number_exposure_before_1st_buy] .>= 10, :Number_exposure_before_1st_buy] = 10;
    Exposed_Buyer_1 = combine(DataFrames.groupby(Exposed_Buyer, :Number_exposure_before_1st_buy), nrow => :Buying_HHs);
    sort!(Exposed_Buyer_1, :Number_exposure_before_1st_buy);
    Exposed_Buyer_1[:Cum_1st_purchases_capped] = cumsum(Exposed_Buyer_1[:Buying_HHs]);
    Exposed_Buyer_1[:Percentage_of_total_1st_purchases] = Exposed_Buyer_1[:Cum_1st_purchases_capped]/sum(Exposed_Buyer_1[:Buying_HHs]);
    Exposed_Buyer_1[:Obs] = collect(1:nrow(Exposed_Buyer_1));
    Exposed_Buyer_final = Exposed_Buyer_1[:, [:Obs, :Number_exposure_before_1st_buy, :Buying_HHs, :Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]];
    DataFrames.rename!(Exposed_Buyer_final, :Number_exposure_before_1st_buy => :Frequency);
    First_Buy_by_Frequency_Digital_std = Exposed_Buyer_final;
    First_Buy_by_Frequency_Digital_std[!, :Frequency] = identity.(First_Buy_by_Frequency_Digital_std[!, :Frequency]);   # Convert to TYPE from Union{Missing, TYPE}

    #Calculate 1st Buy by Dynamic Frequency Buckets
    Exposed_Buyer_dyn = deepcopy(combined[combined[:Number_exposure_before_1st_buy] .!= 0, [:Number_exposure_before_1st_buy]]);
    Exposed_Buyer_dyn[:Exposures] = "Exposures_le_"*string(freq_index[:frq_index][1]);    
    for dec in range(1, length(freq_index[:frq_index])-2);
        Exposed_Buyer_dyn[(Exposed_Buyer_dyn[:Number_exposure_before_1st_buy] .> freq_index[:frq_index][dec]) .& (Exposed_Buyer_dyn[:Number_exposure_before_1st_buy] .<= freq_index[:frq_index][dec+1]) .& (freq_index[:frq_index][dec] .!= freq_index[:frq_index][dec+1]), :Exposures] = "Exposures_g_"*string(freq_index[:frq_index][dec])*"_le_"*string(freq_index[:frq_index][dec+1]);
    end
    Exposed_Buyer_dyn[(Exposed_Buyer_dyn[:Number_exposure_before_1st_buy] .> freq_index[:frq_index][end-1]) .& (freq_index[:frq_index][end-1] .!= freq_index[:frq_index][end-2]), :Exposures] = "Exposures_ge_"*string(freq_index[:frq_index][end-1]);
    Exposed_Buyer_1_dyn = combine(DataFrames.groupby(Exposed_Buyer_dyn, :Exposures), nrow => :Buying_HHs);
    Exposed_Buyer_1_dyn = DataFrames.leftjoin(Exposed_Buyer_1_dyn, freq_index, on = :Exposures);
    Exposed_Buyer_1_dyn = sort!(Exposed_Buyer_1_dyn, :frq_index);
    Exposed_Buyer_1_dyn[:Cum_1st_purchases_capped] = cumsum(Exposed_Buyer_1_dyn[:Buying_HHs]);
    Exposed_Buyer_1_dyn[:Percentage_of_total_1st_purchases] = Exposed_Buyer_1_dyn[:Cum_1st_purchases_capped]/sum(Exposed_Buyer_1_dyn[:Buying_HHs]);
    Exposed_Buyer_1_dyn[:Obs] = collect(1:nrow(Exposed_Buyer_1_dyn));
    Exposed_Buyer_final_dyn = Exposed_Buyer_1_dyn[:, [:Obs, :Exposures, :Buying_HHs, :Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]];
    DataFrames.rename!(Exposed_Buyer_final_dyn, :Exposures => :Frequency);
    First_Buy_by_Frequency_Digital_Dyn = Exposed_Buyer_final_dyn;

    return First_Buy_by_Frequency_Digital_std, First_Buy_by_Frequency_Digital_Dyn
end

function freq_HH_buying(combined::DataFrame)
    #= Calculate the count of HHs and the proportion of the total HHs per number of exposures (capped to 10). =#
    buyer_freq = deepcopy(combined[:, [:Exposures]]);
    buyer_freq[buyer_freq[:Exposures] .>= 10, :Exposures] = 10;
    buyer_freq_1 = combine(DataFrames.groupby(buyer_freq, :Exposures), nrow => :HHs);
    sort!(buyer_freq_1, :Exposures);
    buyer_freq_1[:Obs] = collect(1:nrow(buyer_freq_1));
    buyer_freq_1[:Percentage_of_buying_HHs] = buyer_freq_1[:HHs]/sum(buyer_freq_1[:HHs]);
    buyer_freq_1 = buyer_freq_1[:, [:Obs, :Exposures, :HHs, :Percentage_of_buying_HHs]];
    Buyer_Frequency_Digital = buyer_freq_1;

    return Buyer_Frequency_Digital
end

function first_buy_last_exp(combined::DataFrame)
    #= Calculate the HH count and percentage of total HHs (this only for buying HHs in post-campaign period) for each level of number of weeks lapsed between last exposure and first purchase. Then, only for buying HHs in post-campaign period, calculate the average number of exposures per each level of number of weeks lapsed between last exposure and first purchase and an "adjusted average" computed as the previous average plus 2.35 times the standard deviation of the number of exposures prior to the first purchase each level of number of weeks lapsed between last exposure and first purchase [--> NOTE: THIS METRIC IS MISLEADINGLY LABELLED `Avg_Exposures_to_1st_buy_without_outliers`. NOT SURE IT'S CORRECTLY USED.]. =#
    buyer_exposure = deepcopy(combined[:, [:Time, :Number_exposure_before_1st_buy]]);
    buyer_exposure_1 = combine(DataFrames.groupby(buyer_exposure, :Time), nrow => :Buying_HHs);
    sort!(buyer_exposure_1, :Time);
    buyer_exposure_1[:Obs] = collect(1:nrow(buyer_exposure_1));
    buyer_exposure_1[:Percentage_of_total_buying_HHs] = 0.0;
    buyer_exposure_1[buyer_exposure_1[:Time] .!= "Pre", :Percentage_of_total_buying_HHs] = buyer_exposure_1[buyer_exposure_1[:Time] .!= "Pre", :Buying_HHs]/sum(buyer_exposure_1[buyer_exposure_1[:Time] .!= "Pre", :Buying_HHs]);
    buyer_exposure_1_1 = combine(DataFrames.groupby(buyer_exposure, :Time), :Number_exposure_before_1st_buy => mean => :Avg_Exposures_to_1st_buy);
    buyer_exposure_1 = DataFrames.innerjoin(buyer_exposure_1, buyer_exposure_1_1, on = :Time);
    buyer_exposure_adj = combine(x -> mean(x.Number_exposure_before_1st_buy)+2.35*std(x.Number_exposure_before_1st_buy), DataFrames.groupby(buyer_exposure, :Time));
    buyer_exposure = DataFrames.leftjoin(buyer_exposure, buyer_exposure_adj, on = :Time);
    buyer_exposure_2 = combine(x -> mean(x[x.Number_exposure_before_1st_buy .<= x.x1, :Number_exposure_before_1st_buy]), DataFrames.groupby(buyer_exposure, :Time));
    buyer_exposure_final = DataFrames.leftjoin(buyer_exposure_1, buyer_exposure_2, on = :Time);
    buyer_exposure_final = buyer_exposure_final[:, [:Obs, :Time, :Buying_HHs, :Percentage_of_total_buying_HHs, :Avg_Exposures_to_1st_buy, :x1]];
    buyer_exposure_final[!, :x1] = identity.(buyer_exposure_final[!, :x1]);   # Convert to TYPE from Union{Missing, TYPE}
    DataFrames.rename!(buyer_exposure_final, :x1 => :Avg_Exposures_to_1st_buy_without_outliers);

    return buyer_exposure_final;
end

function Total_freq_digital(hhcounts_date::DataFrame)
    #= Calculate the count of HHs and percentage of total HHs per each exposure level (capped to 10 exposures). Based on hhcounts_date.cvs. =#
    exp_data2 = deepcopy(hhcounts_date);
    exp_data2 = exp_data2[exp_data2[:brk] .== unique(exp_data2[:brk])[1], :];
    expocnt_1 = combine(DataFrames.groupby(exp_data2, :panid), :impressions => sum => :Exposures);
    expocnt_1[expocnt_1[:Exposures] .>= 10, :Exposures] = 10;
    Total_Freq = combine(DataFrames.groupby(expocnt_1, :Exposures), nrow => :HHs);
    Total_Freq[:Percentage_of_Total_HHs] = Total_Freq[:HHs]/sum(Total_Freq[:HHs]);
    Total_Freq = hcat(sort(Total_Freq, :Exposures), collect(1:nrow(Total_Freq)));
    DataFrames.rename!(Total_Freq, :x1 => :Obs);
    Total_Freq = Total_Freq[:, [:Obs, :Exposures, :HHs, :Percentage_of_Total_HHs]];

    return Total_Freq
end

function Cum_IMP(expocnt::DataFrame)
    #= By exposure level (i.e., number of exposures), compute the number of of HHs, the number of impressions served (= exposures*HHs), the cumulative number of impressions served, and the capped impressions served. Capped impressions served are calculated as the sum of the product of the total HHs having at least a level of exposures and that level of exposure and the total impressions served across all smaller exposure level [--> NOTE: Not sure `imps_served_capped` is a clear label. The metric measures the number of impressions that has translated to at least as many exposures as the exposure level.]. =#
    Cum_IMPs = combine(DataFrames.groupby(expocnt, :Exposures), nrow => :HHs);
    sort!(Cum_IMPs, :Exposures);
    Cum_IMPs[:imps_Served] = Int.(Cum_IMPs[:HHs].*Cum_IMPs[:Exposures]);
    Cum_IMPs[:CUM_IMPs_Served] = cumsum(Cum_IMPs[:imps_Served]);
    Cum_IMPs = hcat(sort(Cum_IMPs, :Exposures), collect(1:nrow(Cum_IMPs)));
    DataFrames.rename!(Cum_IMPs, :x1 => :Obs);
    Cum_IMPs[:imps_served_capped] = sum(Cum_IMPs[:HHs]);
    for row in 2:nrow(Cum_IMPs)
        Cum_IMPs[row, :imps_served_capped] = (Cum_IMPs[row, :Exposures]*sum(Cum_IMPs[row:nrow(Cum_IMPs), :HHs]))+Cum_IMPs[row-1, :CUM_IMPs_Served];
    end
    Cum_IMPs = Cum_IMPs[:, [:Obs, :Exposures, :HHs, :imps_Served, :CUM_IMPs_Served, :imps_served_capped]];

    return Cum_IMPs
end

function Buyer_Frequency_Characteristics(hhcounts_date::DataFrame, df_cdma_dump::DataFrame)
    #= Create table mapping IRI weeks to "real time" weeks for the campaign period. For each IRI week, compute the number of impressions for lapsed buyers, brand switch buyers, brand buyers, and category buyers (exposed_buyer_by_week). For each IRI week, compute the cumulative proportion of total impressions for lapsed buyers, brand switch buyers, brand buyers, and category buyers throughout the entire campaign, i.e., across all weeks (cumulative_by_week). =#
    df = Dates.DateFormat("y-m-d");
    dt_base = Date("2014-12-28", df);
    buyer_exposure = DataFrames.innerjoin(df_cdma_dump, hhcounts_date, on = :panid);
    buyer_exposure = sort!(buyer_exposure[:, [:panid, :buyer_pos_p1, :buyer_pre_52w_p1, :buyer_pre_52w_p0, :trps_pos_p1, :dte, :impressions]], :dte);
    buyer_exposure[:dte] = map(x -> join([string(x)[1:4], string(x)[5:6], string(x)[7:8]], "-"), buyer_exposure[:dte]);
    buyer_exposure[:iri_week] = map(x -> Int(1843+round(ceil(Dates.value((Date(x, df)-dt_base))/7); digits=0)), buyer_exposure[:dte]);

    exposed_buyer_by_week = DataFrame(iri_week=Int64[], WEEK_ID=String[], PCT_LAPSED_BUYERS=Int64[], PCT_BRAND_SWITCH_BUYERS=Int64[], PCT_BRAND_BUYERS=Int64[], PCT_CATEGORY_BUYERS=Int64[]);
    df_weeks = DataFrames.groupby(buyer_exposure, :iri_week);
    for subdf in df_weeks
        PCT_LAPSED_BUYERS = sum(subdf[(subdf[:buyer_pos_p1] .== 0) .& (subdf[:buyer_pre_52w_p1] .== 1), :impressions]);
        PCT_BRAND_SWITCH_BUYERS = sum(subdf[(subdf[:buyer_pos_p1] .== 1) .& (subdf[:buyer_pre_52w_p1] .== 0) .& (subdf[:buyer_pre_52w_p0] .== 1), :impressions]);
        PCT_BRAND_BUYERS = sum(subdf[(subdf[:buyer_pos_p1] .== 1), :impressions]);
        PCT_CATEGORY_BUYERS = sum(subdf[(subdf[:buyer_pre_52w_p0] .== 1), :impressions]);
        final_pur_data_buyer_temp = [unique(subdf[:iri_week]) minimum(subdf[:dte]) PCT_LAPSED_BUYERS PCT_BRAND_SWITCH_BUYERS PCT_BRAND_BUYERS PCT_CATEGORY_BUYERS];
        push!(exposed_buyer_by_week, final_pur_data_buyer_temp);
    end

    rowsum = mapcols(sum, exposed_buyer_by_week[:, 3:end]);
    cumulative_by_week = deepcopy(exposed_buyer_by_week);
    for i in 3:ncol(cumulative_by_week)
        cumulative_by_week[i] = cumsum(cumulative_by_week[i]);
    end
    for i in 3:ncol(cumulative_by_week)
        cumulative_by_week[i] = cumulative_by_week[i]./rowsum[i-2][1]*100;
    end

    return exposed_buyer_by_week, cumulative_by_week
end

# DESCRIPTIVES Reporting functions
function sales_metrics(df::DataFrame)
    #= Calculate: 1) the proportion of buyers of target brand and category among the exposed and unexposed in pre-campaign and post-campaign; 2) the average dollars-per-trip for buyers of target brand and category among the exposed and unexposed in pre-campaign and post-campaign; 3) the average trips for buyers of target brand and category among the exposed and unexposed in pre-campaign and post-campaign. =#
    df_groups = DataFrames.groupby(df, :group);  # first grouped dataframe is `group=0`, second is `group=1`
    metric_types = Dict{Symbol, Array{Symbol,1}}(:buyers=>[:buyer_pre_p1, :buyer_pre_p0, :buyer_pos_p1, :buyer_pos_p0], :doll=>[:dol_per_trip_pre_p1, :dol_per_trip_pre_p0, :dol_per_trip_pos_p1, :dol_per_trip_pos_p0], :trps=>[:trps_pre_p1, :trps_pre_p0, :trps_pos_p1, :trps_pos_p0]);
    value_names = ["Pre_Brand_", "Pre_Cat_", "Pos_Brand_", "Pos_Cat_"];
    df_metrics = DataFrame(group=Int64[]; metrics=String[], val_name=String[], val=Float64[]);
    for subdf in df_groups
        grp = Int64(mean(subdf[:group]));
        for (k, v) in metric_types
            if k == :buyers
                append!(df_metrics, DataFrame(group = repeat([grp], outer=[4]), metrics = repeat([string(k)], outer=[4]), val_name = value_names, val = [mean(subdf[col] .== 1) for col in metric_types[k]]));
            else
                append!(df_metrics, DataFrame(group = repeat([grp], outer=[4]), metrics = repeat([string(k)], outer=[4]), val_name = value_names, val = [mean(subdf[subdf[col1] .== 1, col2]) for (col1, col2) in zip(metric_types[:buyers], v)]));
            end
        end
    end
    df_metrics[:val_name] = ifelse.(df_metrics[:group] .== 1, df_metrics[:val_name].*"Exp", df_metrics[:val_name].*"NExp");
    df_metrics = DataFrames.unstack(df_metrics, :metrics, :val_name, :val);
    df_metrics = df_metrics[[:metrics, :Pre_Brand_Exp, :Pre_Brand_NExp, :Pre_Cat_Exp, :Pre_Cat_NExp, :Pos_Brand_Exp, :Pos_Brand_NExp, :Pos_Cat_Exp, :Pos_Cat_NExp]];    # --> Do we need to have this specific column order in the output report table?
    df_metrics[!, DataFrames.Not(:metrics)] = identity.(df_metrics[!, DataFrames.Not(:metrics)]);   # Convert to TYPE from Union{Missing, TYPE}

    return df_metrics
end

function dolocc_bins(descDump::DataFrame)   # --> Perhaps bins should be created based on the range of the variables?
    #= For exposed and unexposed buyers of target brand and exposed and unexposed buyers of category, calculate the number of HHs per each level of average dollars spent per trip in pre-campaign and post-campaign. =#
    bin_first = 0;
    bin_last = 42;
    df_groups = DataFrames.groupby(descDump, :group);  # first grouped dataframe is `group=0`, second is `group=1`
    dolocc_bins_types = OrderedDict{Int64, Array{Symbol,1}}(
        i => if i != 0
            [:dol_per_trip_pre_p1, :dol_per_trip_pre_p0, :dol_per_trip_pos_p1, :dol_per_trip_pos_p0]
        else
            [:buyer_pre_p1, :buyer_pre_p0, :buyer_pos_p1, :buyer_pos_p0]
        end
        for i = bin_first:2:bin_last
    );
    value_names = ["Pre_Brand_", "Pre_Cat_", "Pos_Brand_", "Pos_Cat_"];
    df_bin_names = DataFrame(bin = Int64[], sales = String[]);
    df_dolocc_bins = DataFrame(group = Int64[]; bin = Int64[], val_name = String[], val = Int64[]);
    for subdf in df_groups
        grp = Int64(mean(subdf[:group]));
        bin_counter = 0;
        for (k, v) in dolocc_bins_types
            bin_counter = bin_counter+1;
            if (k != bin_first) & (k != bin_last)
                append!(df_dolocc_bins, DataFrame(group = repeat([grp], outer=[4]), bin = repeat([bin_counter], outer=[4]), val_name = value_names, val = [nrow(subdf[(subdf[col] .> (k-2)) .& (subdf[col] .<= k), :]) for col in dolocc_bins_types[k]]));
                push!(df_bin_names, [bin_counter, "("*string(k-2)*"-"*string(k)*"]"]);
            elseif k == bin_first
                append!(df_dolocc_bins, DataFrame(group = repeat([grp], outer=[4]), bin = repeat([bin_counter], outer=[4]), val_name = value_names, val = [nrow(subdf[subdf[col] .== 0, :]) for col in dolocc_bins_types[k]]));
                push!(df_bin_names, [bin_counter, "["*string(k)*"]"]);
            else
                append!(df_dolocc_bins, DataFrame(group = repeat([grp], outer=[4]), bin = repeat([bin_counter], outer=[4]), val_name = value_names, val = [nrow(subdf[subdf[col] .> (k-2), :]) for col in dolocc_bins_types[k]]));
                push!(df_bin_names, [bin_counter, "("*string(k-2)*" or more)"]);
            end
        end
    end
    df_dolocc_bins[:val_name] = ifelse.(df_dolocc_bins[:group] .== 1, df_dolocc_bins[:val_name].*"Exp", df_dolocc_bins[:val_name].*"NExp");
    df_dolocc_bins = DataFrames.innerjoin(df_bin_names[unique(df_bin_names[:bin]), :], DataFrames.unstack(df_dolocc_bins, :bin, :val_name, :val), on = :bin);
    df_dolocc_bins = df_dolocc_bins[[:sales, :Pre_Brand_Exp, :Pre_Brand_NExp, :Pre_Cat_Exp, :Pre_Cat_NExp, :Pos_Brand_Exp, :Pos_Brand_NExp, :Pos_Cat_Exp, :Pos_Cat_NExp]];  # --> Do we need to have this specific column order in the output report table?
    df_dolocc_bins[!, DataFrames.Not(:sales)] = identity.(df_dolocc_bins[!, DataFrames.Not(:sales)]);   # Convert to TYPE from Union{Missing, TYPE}

    return df_dolocc_bins
end

function catg_relfreq(descDump::DataFrame, catg_var::Symbol)
    #= Calculate the relative frequency (%) of `model` (= proscore) or `banner` (= retailer) among exposed and controls. =#
    if catg_var == :model
        catg_nm = :Proscore;
    elseif catg_var == :banner
        catg_nm = :Banner;
    else
        catg_nm = :UNKNOWN;
    end
    df_groups = DataFrames.groupby(descDump[[:group, catg_var]], :group);  # first grouped dataframe is `group=0`, second is `group=1`
    df_0 = DataFrames.rename!(combine(x -> round(nrow(x)./nrow(df_groups[1]); digits=2), DataFrames.groupby(df_groups[1], catg_var)), :x1 => :Control);
    df_1 = DataFrames.rename!(combine(x -> round(nrow(x)./nrow(df_groups[2]); digits=2), DataFrames.groupby(df_groups[2], catg_var)), :x1 => :Exposed);
    df_out = sort(DataFrames.innerjoin(df_0, df_1, on = catg_var), catg_var);
    df_out = DataFrames.rename!(df_out, catg_var => catg_nm);

    return df_out
end

function upc_stats(df_upcs_mx::DataFrame)   # NOT USED IN VISUALIZATION TOOL!
    #= Calculate average net price, quantity, number of HHs, total sales, and total units sold of target brand by group and UPC in pre-campaign and post-campaign. =#
    gdf = DataFrames.groupby(df_upcs_mx[df_upcs_mx[:product_grp_id] .== 1, :], [:group, :period_buyer, :derived_upc10]);
    df_stats = combine(x -> [mean(x.net_price), sum(x.quantity), length(unique(x.panid)), sum(x.net_price), sum(x.:units)], gdf);
    df_stats[:var_name] = repeat(["avg_net_price", "sum_quantity", "distinct_experian_id", "sum_net_price", "sum_units"], outer=[Int64(size(df_stats, 1)/5)]);
    df_stats = DataFrames.unstack(df_stats, :var_name, :x1)[[:period_buyer, :group, :derived_upc10, :avg_net_price, :sum_quantity, :distinct_experian_id, :sum_net_price, :sum_units]];   # --> Do we need to have this specific column order in the output report table?
    df_stats[!, DataFrames.Not([:period_buyer, :group, :derived_upc10])] = identity.(df_stats[!, DataFrames.Not([:period_buyer, :group, :derived_upc10])]);   # Convert to TYPE from Union{Missing, TYPE}
    DataFrames.rename!(df_stats, :group => :exposed_flag, :derived_upc10 => :derived_upc);
    df_stats_pre = df_stats[df_stats[:period_buyer] .== 1, setdiff(names(df_stats), ["period_buyer"])];  # `period_buyer=1` is pre-campaign
    df_stats_pos = df_stats[df_stats[:period_buyer] .== 2, setdiff(names(df_stats), ["period_buyer"])];  # `period_buyer=2` is post-campaign

    return df_stats_pre, df_stats_pos
end

function brand_dist(df_upcs_mx::DataFrame)
    #= By group and campaign period calculate the dollar sales and share of sales for each 3-tuple tsv_brand-majorbrand-product_grp_id.  =#
    df_subs = combine(x -> DataFrame(sales=sum(x.net_price)), DataFrames.groupby(df_upcs_mx, [:group, :period_buyer]));
    df_calc = combine(x -> DataFrame(sums=sum(x.net_price)), DataFrames.groupby(df_upcs_mx, [:group, :period_buyer, :tsv_brand, :majorbrand, :product_grp_id]));
    df_calc = DataFrames.innerjoin(df_calc, df_subs, on = [:group, :period_buyer]);
    df_calc[:pct] = df_calc[:sums]./df_calc[:sales]*100;
    df_calc[[:sums, :pct]] = coalesce.(df_calc[[:sums, :pct]], 0);
    df_calc[:block] = 2;

    df_calc_tot = combine(x -> DataFrame(sums=sum(x.sums), pct=sum(x.pct)), DataFrames.groupby(df_calc, [:group, :period_buyer, :product_grp_id]));
    df_calc_tot[:tsv_brand] = "Total_brand";
    df_calc_tot[:majorbrand] = "Total";
    df_calc_tot[:block] = 1;

    df_all = vcat(df_calc_tot[[:group, :period_buyer, :block, :tsv_brand, :majorbrand, :product_grp_id, :sums, :pct]], df_calc[[:group, :period_buyer, :block, :tsv_brand, :majorbrand, :product_grp_id, :sums, :pct]]);
    df_all[:prd] = ifelse.(df_all[:period_buyer] .== 1, "PRD1", "PRD2");
    df_sales = DataFrames.rename!(DataFrames.unstack(df_all[[:group, :prd, :block, :tsv_brand, :majorbrand, :product_grp_id, :sums]], :prd, :sums), :PRD1 => :Pre_Dol_Sales, :PRD2 => :Pos_Dol_Sales);
    df_shares = DataFrames.rename!(DataFrames.unstack(df_all[[:group, :prd, :block, :tsv_brand, :majorbrand, :product_grp_id, :pct]], :prd, :pct), :PRD1 => :Pre_Dol_Sales_Share, :PRD2 => :Pos_Dol_Sales_Share);
    df_out = DataFrames.outerjoin(df_sales, df_shares, on = [:group, :tsv_brand, :majorbrand, :product_grp_id], makeunique=true);
    df_out[[:Pre_Dol_Sales, :Pos_Dol_Sales, :Pre_Dol_Sales_Share, :Pos_Dol_Sales_Share]] = coalesce.(df_out[[:Pre_Dol_Sales, :Pos_Dol_Sales, :Pre_Dol_Sales_Share, :Pos_Dol_Sales_Share]], 0);
    sort!(df_out, [:block, :product_grp_id]);
    df_out[!, [:tsv_brand, :majorbrand, :product_grp_id]] = identity.(df_out[!, [:tsv_brand, :majorbrand, :product_grp_id]]);   # Convert to TYPE from Union{Missing, TYPE}

    exposed_net = df_out[df_out[:group] .== 1, [:tsv_brand, :majorbrand, :product_grp_id, :Pre_Dol_Sales, :Pos_Dol_Sales, :Pre_Dol_Sales_Share, :Pos_Dol_Sales_Share]];
    control_net = df_out[df_out[:group] .== 0, [:tsv_brand, :majorbrand, :product_grp_id, :Pre_Dol_Sales, :Pos_Dol_Sales, :Pre_Dol_Sales_Share, :Pos_Dol_Sales_Share]];

    return exposed_net, control_net
end

# UNIFY REPORTS FORMATTING
function format_unify_reports(Tm_1st_by_lst_xpsur_Dgtl::DataFrame, exposed_buyer_by_week::DataFrame, dfo_Exp::DataFrame, dfo_UnExp::DataFrame, dfd_upc_grwth_cnt::DataFrame, dfd_fr_shr_ndx::DataFrame, imp_week::DataFrame, ChannelCode::Int64, flag::Int64=1)
    cfg_str = read("./cfg.json", String);
    cfg_str = replace(cfg_str, "{" => "");
    cfg_str = replace(cfg_str, "}" => "");
    cfg_str = split(cfg_str, "\n");
    cfg_str = replace.(cfg_str, "\"" => "");
    cfg = Dict{String,String}();
    for s in cfg_str
        if s != ""
            push!(cfg, string(split(s, ":")[1]) => string(split(s, ":")[2]));
        end
    end
    for (k, v) in cfg
        cfg[k] = rstrip(cfg[k], ',');
    end

    start_week = parse(Int64, cfg["start_week"]);
    end_week = parse(Int64, cfg["end_week"]);

    Lift_Buyer_char_template = DataFrame(model_desc=String[], model=String[], time_agg_period=Int64[], start_week=Int64[], end_week=Int64[], characteristics_indicator_flag=String[], cum_num_cat_buyer=Float64[], cum_num_brd_shift_buyer=Float64[], cum_num_brd_buyer=Float64[], cum_num_lapsed_buyer=Float64[], cum_total_buyer=Float64[], pct_tot_hh=Float64[], pct_buy_hh=Float64[], cum_num_new_buyer=Int64[], cum_non_brd=Int64[], cum_num_repeat=Int64[], cum_num_cat_shift_buyer=Int64[], cum_pct_repeat_expsd=Float64[], cum_pct_trail_expsd=Float64[], impression_count=Int64[], cumulatve_hh_count=Int64[], channel_code=Int64[]);

    Time_1st_buy = Tm_1st_by_lst_xpsur_Dgtl;
    FREQ = "FRQ";
    if cfg["campaign_type"] == "lift" || cfg["campaign_type"] == "SamsClub"  || cfg["campaign_type"] == "digitallift" || cfg["campaign_type"] == "digitalliftuat" || cfg["campaign_type"] == "tvlift" || cfg["campaign_type"] == "tvliftuat"
        for i in 1:(nrow(Time_1st_buy)-1)
            if i == 10
                push!(Lift_Buyer_char_template, [string("10+"), "FRQ1388", end_week-start_week+1, start_week, start_week+i-1, "BUYER_CHAR_WEEK_FREQUENCY", 0, 0, 0, 0, 0, Frst_Buy_Frq_Dgtl_std[i, :Percentage_of_total_1st_purchases], Time_1st_buy[i, :Percentage_of_total_buying_HHs], 0, 0, 0, 0, 0, 0, 0, 0, ChannelCode]);
            else	
                push!(Lift_Buyer_char_template, [string(i), FREQ*string(i), end_week-start_week+1, start_week, start_week+i-1, "BUYER_CHAR_WEEK_FREQUENCY", 0, 0, 0, 0, 0, Frst_Buy_Frq_Dgtl_std[i, :Percentage_of_total_1st_purchases], Time_1st_buy[i, :Percentage_of_total_buying_HHs], 0, 0, 0, 0, 0, 0, 0, 0, ChannelCode]);
            end
        end
        for i in 1:(nrow(Time_1st_buy)-1)
            if i == 10
                push!(Lift_Buyer_char_template, [string("10+"), "FRQ1388", end_week-start_week+1, start_week, start_week+i-1, "BUYER_CHAR_FREQUENCY", 0, 0, 0, 0, 0, Frst_Buy_Frq_Dgtl_std[i, :Percentage_of_total_1st_purchases], Time_1st_buy[i, :Percentage_of_total_buying_HHs], 0, 0, 0, 0, 0, 0, 0, 0, ChannelCode]);
            else
                push!(Lift_Buyer_char_template, [string(i), FREQ*string(i), end_week-start_week+1, start_week, start_week+i-1, "BUYER_CHAR_FREQUENCY", 0, 0, 0, 0, 0, Frst_Buy_Frq_Dgtl_std[i, :Percentage_of_total_1st_purchases], Time_1st_buy[i, :Percentage_of_total_buying_HHs], 0, 0, 0, 0, 0, 0, 0, 0, ChannelCode]);
            end
        end
    end
    if cfg["campaign_type"] == "google"
        for i in 1:(nrow(Time_1st_buy)-1)
            push!(Lift_Buyer_char_template, [string(i), FREQ*string(i), end_week-start_week+1, start_week, start_week+i-1, "BUYER_CHAR_WEEK_FREQUENCY", 0, 0, 0, 0, 0, 0, Time_1st_buy[i, :Percentage_of_total_buying_HHs], 0, 0, 0, 0, 0, 0, 0, 0, ChannelCode]);
        end
        for i in 1:(nrow(Time_1st_buy)-1)
            push!(Lift_Buyer_char_template, [string(i), FREQ*string(i), end_week-start_week+1, start_week, start_week+i-1, "BUYER_CHAR_FREQUENCY", 0, 0, 0, 0, 0, 0, Time_1st_buy[i, :Percentage_of_total_buying_HHs], 0, 0, 0, 0, 0, 0, 0, 0, ChannelCode]);
        end
    end

    TCP0="TCP0"
    if flag == 1
        for i in unique(exposed_buyer_by_week[:iri_week])
            PCT_LAPSED_BUYERS_T = sum(exposed_buyer_by_week[:PCT_LAPSED_BUYERS]);
            PCT_BRAND_SWITCH_BUYERS_T = sum(exposed_buyer_by_week[:PCT_BRAND_SWITCH_BUYERS]);
            PCT_BRAND_BUYERS_T = sum(exposed_buyer_by_week[:PCT_BRAND_BUYERS]);
            PCT_CATEGORY_BUYERS_T = sum(exposed_buyer_by_week[:PCT_CATEGORY_BUYERS]);

            push!(Lift_Buyer_char_template, ["Total Campaign", TCP0, i - start_week + 1, i, end_week, "BUYER_CHAR_REACH", sum(exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].<=i, :PCT_CATEGORY_BUYERS])/PCT_CATEGORY_BUYERS_T, sum(exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].<=i, :PCT_BRAND_SWITCH_BUYERS])/PCT_BRAND_SWITCH_BUYERS_T, sum(exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].<=i, :PCT_BRAND_BUYERS])/PCT_BRAND_BUYERS_T, sum(exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].<=i, :PCT_LAPSED_BUYERS])/PCT_LAPSED_BUYERS_T, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ChannelCode]);

            push!(Lift_Buyer_char_template, ["Total Campaign", TCP0, i - start_week + 1, i, end_week, "BUYER_CHAR_EXPOSURES", convert(Float64, exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].==i, :PCT_CATEGORY_BUYERS][1]), convert(Float64, exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].==i, :PCT_BRAND_SWITCH_BUYERS][1]), convert(Float64, exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].==i, :PCT_BRAND_BUYERS][1]), convert(Float64, exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].==i, :PCT_LAPSED_BUYERS][1]), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ChannelCode]);
        end
    end

    brand_buyers_unexposed_cnt = dfo_UnExp;
    push!(Lift_Buyer_char_template, ["Total Campaign", TCP0, end_week-start_week+1, start_week, end_week, "BUYER_CHAR_NON_EXP_BUYER", 0, convert(Int32, ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "BRAND SWITCHERS", :CNT][1])), convert(Int32, ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "BRAND BUYERS", :CNT][1])), convert(Int32, ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "LAPSED BUYERS", :CNT][1])), 0, 0, 0, convert(Int32, ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "NEW BUYERS", :CNT][1])), convert(Int32, ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "NON BUYERS", :CNT][1])), convert(Int32, ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "REPEAT BUYERS", :CNT][1])), convert(Int32, ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "CATEGORY SWITCHERS", :CNT][1])), 0, 0, 0, 0, ChannelCode]);

    brand_buyers_exposed_cnt = dfo_Exp;
    push!(Lift_Buyer_char_template, ["Total Campaign", TCP0, end_week-start_week+1, start_week, end_week, "BUYER_CHAR_BUYER", 0, convert(Int32, ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "BRAND SWITCHERS", :CNT][1])), convert(Int32, ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "BRAND BUYERS", :CNT][1])), convert(Int32, ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "LAPSED BUYERS", :CNT][1])), 0, 0, 0, convert(Int32, ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "NEW BUYERS", :CNT][1])), convert(Int32, ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "NON BUYERS", :CNT][1])), convert(Int32, ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "REPEAT BUYERS", :CNT][1])), convert(Int32, ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "CATEGORY SWITCHERS", :CNT][1])), 0, 0, 0, 0, ChannelCode]);

    csv_trial_repeat = gentrialrepeat(df_cdma_dump, udj_avg_expsd_pst, udj_avg_cntrl_pst);  # NED TO RUN FUNCTION!
    push!(Lift_Buyer_char_template, ["Total Campaign", TCP0, end_week-start_week+1, start_week, end_week, "BUYER_CHAR_TRIAL_REPEAT", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, csv_trial_repeat[csv_trial_repeat[:grouptype] .== "Exposed", :repeaters_percent][1], csv_trial_repeat[csv_trial_repeat[:grouptype] .== "Exposed", :triers_percent][1], 0, 0, ChannelCode]);
    push!(Lift_Buyer_char_template, ["Total Campaign", TCP0, end_week-start_week+1, start_week, end_week, "BUYER_CHAR_NON_EXP_TRIAL_REPEAT", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, csv_trial_repeat[csv_trial_repeat[:grouptype] .== "Unexposed", :repeaters_percent][1], csv_trial_repeat[csv_trial_repeat[:grouptype] .== "Unexposed", :triers_percent][1], 0, 0, ChannelCode]);

    mlift_temp_additional_report = DataFrame(target_competitor_ind=String[], description=String[], upc10=String[], PRE_sales=String[], POS_sales=String[], VARIATION=String[], time_agg_period=Int64[], channel_code=Int64[], grp_index=Int64[], registration_id=String[], registration_request_id=String[]);
    agg_upc_growth_df = DataFrame(description=String[],upc10=String[],sales_upc_pre=Float64[],sales_upc_post=Float64[],percentage_sales_upc_pre=Float64[],percentage_sales_upc_post=Float64[],growth_contribution=Float64[]);
    time_agg_period = end_week-start_week+1;

    upc_growth_df = dfd_upc_grwth_cnt;
    for i in unique(upc_growth_df[:description])
        push!(agg_upc_growth_df, [upc_growth_df[upc_growth_df[:description] .== i , :description][1], upc_growth_df[upc_growth_df[:description] .== i ,:upc10][1], mean(upc_growth_df[upc_growth_df[:description] .== i , :sales_upc_pre]), mean(upc_growth_df[upc_growth_df[:description] .== i , :sales_upc_post]), mean(upc_growth_df[upc_growth_df[:description] .== i , :percentage_sales_upc_pre]), mean(upc_growth_df[upc_growth_df[:description] .== i , :percentage_sales_upc_post]), mean(upc_growth_df[upc_growth_df[:description] .== i , :growth_contribution])]);
    end
    sort!(agg_upc_growth_df, :growth_contribution, rev=true);
    for i in 1:nrow(agg_upc_growth_df)
        push!(mlift_temp_additional_report, ["", upc_growth_df[i, 1], string(upc_growth_df[i, 2]), string(convert(BigInt, trunc(upc_growth_df[i, 3]))), string(convert(BigInt, trunc(upc_growth_df[i, 4]))),"UPC_GROWTH_CONTRIBUTION", time_agg_period, ChannelCode, 0, cfg["reg_id"], cfg["reg_req_id"]]);
    end

    fair_share_df = deepcopy(dfd_fr_shr_ndx);
    fair_share_df[:type] = "";
    for i in 1:nrow(fair_share_df)
        if fair_share_df[:product_grp_id][i] == 1 fair_share_df[:type][i] = "advertised" else fair_share_df[:type][i] = "competitor" end
    end
    for i in 1:nrow(fair_share_df)
        push!(mlift_temp_additional_report, [fair_share_df[i, 10], string(fair_share_df[i, 2]), "", string(convert(BigInt, trunc(fair_share_df[i, 3]))), string(convert(BigInt, trunc(fair_share_df[i, 5]))), "FAIR_SHARE_INDEX", time_agg_period, ChannelCode, fair_share_df[i, 1], cfg["reg_id"], cfg["reg_req_id"]]);
    end
    #Share of Requirement
    if cfg["campaign_type"] == "lift" || cfg["campaign_type"] == "SamsClub " || cfg["campaign_type"] == "digitallift" || cfg["campaign_type"] == "digitalliftuat" || cfg["campaign_type"] == "tvlift" || cfg["campaign_type"] == "tvliftuat"
        push!(mlift_temp_additional_report, ["", "", "", string(1-shr_rqrmnt[shr_rqrmnt[:exposed_flag] .== 1, :product_group_share][1]), string(shr_rqrmnt[shr_rqrmnt[:exposed_flag] .== 1, :product_group_share][1]), "BRAND_SHARE_EXPOSED", time_agg_period,ChannelCode, 0, cfg["reg_id"],cfg["reg_req_id"]]);
        push!(mlift_temp_additional_report, ["", "", "", string(1-shr_rqrmnt[shr_rqrmnt[:exposed_flag] .== 0, :product_group_share][1]), string(shr_rqrmnt[shr_rqrmnt[:exposed_flag] .== 0, :product_group_share][1]), "BRAND_SHARE_NONEXPOSED", time_agg_period,ChannelCode, 0, cfg["reg_id"],cfg["reg_req_id"]]);
    end

    if cfg["campaign_type"] == "lift" || cfg["campaign_type"] == "SamsClub " || cfg["campaign_type"] == "digitallift" || cfg["campaign_type"] == "digitalliftuat" || cfg["campaign_type"] == "tvlift" || cfg["campaign_type"] == "tvliftuat"
        for i in sort(imp_week[:iri_week])
            CumulativeHhs = sum(imp_week[imp_week[:iri_week].<=i, :hhs]);
            push!(Lift_Buyer_char_template, [string(i-start_week+1), TCP0, i-start_week+1, start_week, i, "BUYER_CHAR_IMP_HH_COUNTS", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, imp_week[imp_week[:iri_week] .== i, :impressions][1], CumulativeHhs, ChannelCode]);
        end
    end

    return mlift_temp_additional_report, Lift_Buyer_char_template
end

# VISUALIZATION data collection functions
function viz_build_report(dc::Dict{String,Symbol}, df::DataFrame)
    dt = DataFrame(id=String[], value=String[]);
    for (k, v) in dc
        push!(dt, [k, string(df[v])]);
    end

    return dt
end

function viz_build_report(nm::String, vl::String)
    dt = DataFrame(id=String[], value=String[]);
    push!(dt, [nm, vl]);
end

# Page 1 charts
function viz_product_sales(dfd_fr_shr_ndx::DataFrame)
    dc_data = Dict{String,Symbol}(
        "data_p1_t1_a"=>:pre_sales,
        "data_p1_t1_b"=>:pos_sales
    );
    data = viz_build_report(dc_data, dfd_fr_shr_ndx);

    dc_label = Dict{String,Symbol}(
        "lbl_p1_t1"=>:product
    );
    label = viz_build_report(dc_label, dfd_fr_shr_ndx);

    extra = viz_build_report("trgt_brand_name_p1_t1", string(dfd_fr_shr_ndx[dfd_fr_shr_ndx[:product_grp_id] .== 1, :product][1]));

    return data, label, extra
end

function viz_brand_names_sales(exposed_net::DataFrame, control_net::DataFrame)
    df1 = exposed_net[(exposed_net[:majorbrand] .== "Total") .& (exposed_net[:tsv_brand] .== "Total_brand"), [:product_grp_id, :Pre_Dol_Sales, :Pos_Dol_Sales]];
    df2 = control_net[(control_net[:majorbrand] .== "Total") .& (control_net[:tsv_brand] .== "Total_brand"), [:product_grp_id, :Pre_Dol_Sales, :Pos_Dol_Sales]];

    dc_data1 = Dict{String,Symbol}(
        "data_p1_t2_a"=>:Pre_Dol_Sales,
        "data_p1_t2_b"=>:Pos_Dol_Sales
    );
    dc_data2 = Dict{String,Symbol}(
        "data_p1_t2_c"=>:Pre_Dol_Sales,
        "data_p1_t2_d"=>:Pos_Dol_Sales
    );
    data1 = viz_build_report(dc_data1, df1);
    data2 = viz_build_report(dc_data2, df2);
    data = vcat(data1, data2);

    label = viz_build_report("lbl_p1_t2", string(string.(df1[:product_grp_id])));

    extra = viz_build_report("trgt_brand_name_p1_t2", string(df1[df1[:product_grp_id] .== 1, :product_grp_id][1]));

    return data, label, extra
end

function viz_contribution_to_target_brand_change_in_sales(dfd_fr_shr_ndx::DataFrame)
    df = dfd_fr_shr_ndx[dfd_fr_shr_ndx[:product_grp_id] .!= 1, :];
    df[:pct_of_targetbrand] = df[:pct_of_targetbrand].*100;

    dc_data = Dict{String,Symbol}(
        "data_p1_t3_a"=>:pct_of_targetbrand,
        "data_p1_t3_b"=>:fair_share_index
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p1_t3"=>:product
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

function viz_upcs_pos_contrib_to_sale_change(dfd_upc_grwth_cnt::DataFrame)
    df = dfd_upc_grwth_cnt[:, DataFrames.Not([:upc10, :sales_upc_pre, :sales_upc_post])];
    df[:description] = String.(df[:description]);
    df[!, DataFrames.Not(:description)] = mapcols(x -> Float64.(x), df[:, DataFrames.Not(:description)]);
    df[!, DataFrames.Not(:description)] = mapcols(x -> x.*100, df[:, DataFrames.Not(:description)]);

    dc_data = Dict{String,Symbol}(
        "data_p1_t4_a"=>:percentage_sales_upc_pre,
        "data_p1_t4_b"=>:percentage_sales_upc_post,
        "data_p1_t4_c"=>:growth_contribution
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p1_t4"=>:description
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

# Page 2 charts
function viz_buyer_class_exp(dfo_Exp::DataFrame)
    col_order = [:buyer_type, :buyer_percent];
    df = dfo_Exp[col_order];
    df[:buyer_percent] = df[:buyer_percent].*100;
    row_order = ["BRAND BUYERS", "NON BUYERS", "LAPSED BUYERS", "NEW BUYERS", "REPEAT BUYERS", "CATEGORY SWITCHERS", "BRAND SWITCHERS"];
    df[:row_index] = zeros(Int64, nrow(df));
    for r in enumerate(row_order)
        df[df[:buyer_type] .== r[2], :row_index] = r[1];
    end
    sort!(df, :row_index);
    df1 = df[(df[:buyer_type] .== row_order[1]) .| (df[:buyer_type] .== row_order[2]) .| (df[:buyer_type] .== row_order[3]), col_order];
    df2 = df[(df[:buyer_type] .== row_order[4]) .| (df[:buyer_type] .== row_order[5]), col_order];
    push!(df2, ["COMPLEMENT TO 100", 100-sum(df2[:buyer_percent])]);
    df3 = df[(df[:buyer_type] .== row_order[6]) .| (df[:buyer_type] .== row_order[7]), col_order];
    push!(df3, ["COMPLEMENT TO 100", 100-sum(df3[:buyer_percent])]);

    data1 = viz_build_report("data_p2_t1_a", string(df1[:buyer_percent]));
    data2 = viz_build_report("data_p2_t1_b", string(df2[:buyer_percent]));
    data3 = viz_build_report("data_p2_t1_c", string(df3[:buyer_percent]));
    data = vcat(data1, data2, data3);

    label1 = viz_build_report("lbl_p2_t1_a", string(df1[:buyer_type]));
    label2 = viz_build_report("lbl_p2_t1_b", string(df2[1:end-1, :buyer_type]));
    label3 = viz_build_report("lbl_p2_t1_c", string(df3[1:end-1, :buyer_type]));
    label = vcat(label1, label2, label3);

    return data, label
end

function viz_buyer_class_unexp(dfo_UnExp::DataFrame)
    col_order = [:buyer_type, :buyer_percent];
    df = dfo_UnExp[col_order];
    df[:buyer_percent] = df[:buyer_percent].*100;
    row_order = ["BRAND BUYERS", "NON BUYERS", "LAPSED BUYERS", "NEW BUYERS", "REPEAT BUYERS", "CATEGORY SWITCHERS", "BRAND SWITCHERS"];
    df[:row_index] = zeros(Int64, nrow(df));
    for r in enumerate(row_order)
        df[df[:buyer_type] .== r[2], :row_index] = r[1];
    end
    sort!(df, :row_index);
    df1 = df[(df[:buyer_type] .== row_order[1]) .| (df[:buyer_type] .== row_order[2]) .| (df[:buyer_type] .== row_order[3]), col_order];
    df2 = df[(df[:buyer_type] .== row_order[4]) .| (df[:buyer_type] .== row_order[5]), col_order];
    push!(df2, ["COMPLEMENT TO 100", 100-sum(df2[:buyer_percent])]);
    df3 = df[(df[:buyer_type] .== row_order[6]) .| (df[:buyer_type] .== row_order[7]), col_order];
    push!(df3, ["COMPLEMENT TO 100", 100-sum(df3[:buyer_percent])]);

    data1 = viz_build_report("data_p2_t2_a", string(df1[:buyer_percent]));
    data2 = viz_build_report("data_p2_t2_b", string(df2[:buyer_percent]));
    data3 = viz_build_report("data_p2_t2_c", string(df3[:buyer_percent]));
    data = vcat(data1, data2, data3);

    label1 = viz_build_report("lbl_p2_t2_a", string(df1[:buyer_type]));
    label2 = viz_build_report("lbl_p2_t2_b", string(df2[1:end-1, :buyer_type]));
    label3 = viz_build_report("lbl_p2_t2_c", string(df3[1:end-1, :buyer_type]));
    label = vcat(label1, label2, label3);

    return data, label
end

function viz_exposures_per_buyer_type(exposed_buyer_by_week::DataFrame)
    dc_data = Dict{String,Symbol}(
        "data_p2_t3_a"=>:PCT_LAPSED_BUYERS,
        "data_p2_t3_b"=>:PCT_BRAND_SWITCH_BUYERS,
        "data_p2_t3_c"=>:PCT_BRAND_BUYERS,
        "data_p2_t3_d"=>:PCT_CATEGORY_BUYERS
    );
    data = viz_build_report(dc_data, exposed_buyer_by_week);

    dc_label = Dict{String,Symbol}(
        "lbl_p2_t3"=>:WEEK_ID
    );
    label = viz_build_report(dc_label, exposed_buyer_by_week);

    return data, label
end

function viz_cum_exposures_per_buyer_type(cumulative_by_week::DataFrame)
    dc_data = Dict{String,Symbol}(
        "data_p2_t4_a"=>:PCT_LAPSED_BUYERS,
        "data_p2_t4_b"=>:PCT_BRAND_SWITCH_BUYERS,
        "data_p2_t4_c"=>:PCT_BRAND_BUYERS,
        "data_p2_t4_d"=>:PCT_CATEGORY_BUYERS
    );
    data = viz_build_report(dc_data, cumulative_by_week);

    dc_label = Dict{String,Symbol}(
        "lbl_p2_t4"=>:WEEK_ID
    );
    label = viz_build_report(dc_label, cumulative_by_week);

    return data, label
end

# Page 3 charts
function viz_exposure_distribution(Byr_Frq_Dgtl::DataFrame)
    df = Byr_Frq_Dgtl[[:Exposures, :HHs, :Percentage_of_buying_HHs]];
    df[:Percentage_of_buying_HHs] = df[:Percentage_of_buying_HHs].*100;
    df[:Exposures] = string.(df[:Exposures]);
    df[df[:Exposures] .== "10", :Exposures] = "10+";

    dc_data = Dict{String,Symbol}(
        "data_p3_t1_a"=>:HHs,
        "data_p3_t1_b"=>:Percentage_of_buying_HHs
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p3_t1"=>:Exposures
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

function viz_weeks_last_exp_1stbuy_distribut(Tm_1st_by_lst_xpsur_Dgtl::DataFrame)
    df = Tm_1st_by_lst_xpsur_Dgtl[[:Time, :Buying_HHs, :Avg_Exposures_to_1st_buy]];
    df = df[df[:Time] .!= "Pre", :];

    dc_data = Dict{String,Symbol}(
        "data_p3_t2_a"=>:Buying_HHs,
        "data_p3_t2_b"=>:Avg_Exposures_to_1st_buy
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p3_t2"=>:Time
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

function viz_exposure_before_1stbuy_distribution(Frst_Buy_Frq_Dgtl_std::DataFrame)
    df = Frst_Buy_Frq_Dgtl_std[[:Frequency, :Buying_HHs, :Percentage_of_total_1st_purchases]];
    df[:Percentage_of_total_1st_purchases] = df[:Percentage_of_total_1st_purchases].*100;
    df[:Frequency] = string.(df[:Frequency]);
    df[df[:Frequency] .== "10", :Frequency] = "10+";

    dc_data = Dict{String,Symbol}(
        "data_p3_t3_a"=>:Buying_HHs,
        "data_p3_t3_b"=>:Percentage_of_total_1st_purchases
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p3_t3"=>:Frequency
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

function viz_impressions_dist(Tot_Frq_Dgtl::DataFrame)
    df = Tot_Frq_Dgtl[[:Exposures, :HHs]];
    df[:Exposures] = string.(df[:Exposures]);
    df[df[:Exposures] .== "10", :Exposures] = "10+";

    data = viz_build_report("data_p3_t4_a", string(df[:HHs]));

    label = viz_build_report("lbl_p3_t4", string(df[:Exposures]));

    return data, label
end

function viz_total_exposures_distrib(dfd_Cum_IMP::DataFrame)
    df = dfd_Cum_IMP[:, DataFrames.Not(:Obs)];
    df[:Exposures] = string.(df[:Exposures]);

    dc_data = Dict{String,Symbol}(
        "data_p3_t5_a"=>:HHs,
        "data_p3_t5_b"=>:imps_Served,
        "data_p3_t5_c"=>:CUM_IMPs_Served,
        "data_p3_t5_d"=>:imps_served_capped
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p3_t5"=>:Exposures
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

# Page 4 charts
function viz_proscore_distribution(df_proscore_dist::DataFrame)
    df = df_proscore_dist[:, :];
    df[:Proscore] = string.(df[:Proscore]);
    df[:, DataFrames.Not(:Proscore)] = mapcols(x -> x.*100, df[:, DataFrames.Not(:Proscore)]);

    dc_data = Dict{String,Symbol}(
        "data_p4_t1_a"=>:Control,
        "data_p4_t1_b"=>:Exposed
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p4_t1"=>:Proscore
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

function viz_retailer_distribution(df_retailer_dist::DataFrame)
    df = df_retailer_dist[:, :];
    df[:Banner] = string.(df[:Banner]);
    df[:, DataFrames.Not(:Banner)] = mapcols(x -> x.*100, df[:, DataFrames.Not(:Banner)]);

    dc_data = Dict{String,Symbol}(
        "data_p4_t2_a"=>:Control,
        "data_p4_t2_b"=>:Exposed
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p4_t2"=>:Banner
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

function viz_dolocc_bins_targetbrand(df_dolocc_bins::DataFrame)
    df = df_dolocc_bins[vcat(:sales, Symbol.(filter(x -> contains(string(x), "Brand"), names(df_dolocc_bins))))];

    dc_data = Dict{String,Symbol}(
        "data_p4_t3_a"=>:Pre_Brand_Exp,
        "data_p4_t3_b"=>:Pre_Brand_NExp,
        "data_p4_t3_c"=>:Pos_Brand_Exp,
        "data_p4_t3_d"=>:Pos_Brand_NExp
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p4_t3"=>:sales
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

function viz_dolocc_bins_category(df_dolocc_bins::DataFrame)
    df = df_dolocc_bins[vcat(:sales, Symbol.(filter(x -> contains(string(x), "Cat"), names(df_dolocc_bins))))];

    dc_data = Dict{String,Symbol}(
        "data_p4_t4_a"=>:Pre_Cat_Exp,
        "data_p4_t4_b"=>:Pre_Cat_NExp,
        "data_p4_t4_c"=>:Pos_Cat_Exp,
        "data_p4_t4_d"=>:Pos_Cat_NExp
    );
    data = viz_build_report(dc_data, df);

    dc_label = Dict{String,Symbol}(
        "lbl_p4_t4"=>:sales
    );
    label = viz_build_report(dc_label, df);

    return data, label
end

# Page 5 charts
function viz_sales_metrics_raw_matched_buyers(df_raw_metrics::DataFrame, df_matched_metrics::DataFrame)
    col_order = [:Pre_Brand, :Pos_Brand, :Pre_Cat, :Pos_Cat];
    df1 = df_raw_metrics[df_raw_metrics[:metrics] .== "buyers", filter(x -> contains(string(x), "_Exp"), names(df_raw_metrics))];
    DataFrames.rename!(df1, map(x -> Symbol(replace(string(x), "_Exp" => "")), names(df1)));
    df1 = df1[col_order];
    df1 = mapcols(x -> x.*100, df1);
    df1 = DataFrames.stack(df1);
    df2 = df_raw_metrics[df_raw_metrics[:metrics] .== "buyers", filter(x -> contains(string(x), "_NExp"), names(df_raw_metrics))];
    names!(df2, map(x -> Symbol(replace(string(x), "_NExp" => "")), names(df2)));
    df2 = df2[col_order];
    df2 = mapcols(x -> x.*100, df2);
    df2 = DataFrames.stack(df2);
    df3 = df_matched_metrics[df_matched_metrics[:metrics] .== "buyers", filter(x -> contains(string(x), "_Exp"), names(df_matched_metrics))];
    names!(df3, map(x -> Symbol(replace(string(x), "_Exp" => "")), names(df3)));
    df3 = df3[col_order];
    df3 = mapcols(x -> x.*100, df3);
    df3 = DataFrames.stack(df3);
    df4 = df_matched_metrics[df_matched_metrics[:metrics] .== "buyers", filter(x -> contains(string(x), "_NExp"), names(df_matched_metrics))];
    names!(df4, map(x -> Symbol(replace(string(x), "_NExp" => "")), names(df4)));
    df4 = df4[col_order];
    df4 = mapcols(x -> x.*100, df4);
    df4 = DataFrames.stack(df4);

    col_names = string.(col_order);
    col_names = map(x -> replace(x, "Brand" => "Target Brand"), col_names);
    col_names = map(x -> replace(x, "Cat" => "Category"), col_names);
    col_names = map(x -> replace(x, "Pre" => "Pre-Campaign"), col_names);
    col_names = map(x -> replace(x, "Pos" => "Post-Campaign"), col_names);
    col_names = map(x -> split(x, '_')[2]*" "*split(x, '_')[1], col_names);

    data1 = viz_build_report("data_p5_t1_a", string(df1[:value]));
    data2 = viz_build_report("data_p5_t1_b", string(df2[:value]));
    data3 = viz_build_report("data_p5_t1_c", string(df3[:value]));
    data4 = viz_build_report("data_p5_t1_d", string(df4[:value]));
    data = vcat(data1, data2, data3, data4);

    label = viz_build_report("lbl_p5_t1", string(col_names));

    return data, label
end

function viz_sales_metrics_raw_matched_trips(df_raw_metrics::DataFrame, df_matched_metrics::DataFrame)
    col_order = [:Pre_Brand, :Pos_Brand, :Pre_Cat, :Pos_Cat];
    df = DataFrame(id=String[], kind=String[], Pre_Brand=Float64[], Pos_Brand=Float64[], Pre_Cat=Float64[], Pos_Cat=Float64[]);
    datasets = Dict("raw" => df_raw_metrics, "matched" => df_matched_metrics);
    for (k, v) in datasets
        for kind in ["_Exp", "_NExp"]
            df_new = v[v[:metrics] .== "trps", filter(x -> contains(string(x), kind), names(v))];
            names!(df_new, map(x -> Symbol(replace(string(x), kind => "")), names(df_new)));
            # df_new = map(x -> x.*100, eachcol(df_new)); # NOT NEEDED IN THIS CASE
            df_new[:id] = k;
            df_new[:kind] = kind;
            df_new = df_new[vcat([:id, :kind], col_order)];
            append!(df, df_new);
        end
    end
    df1 = DataFrames.stack(df[(df[:id] .== "raw") .& (df[:kind] .== "_Exp"), col_order]);
    df2 = DataFrames.stack(df[(df[:id] .== "raw") .& (df[:kind] .== "_NExp"), col_order]);
    df3 = DataFrames.stack(df[(df[:id] .== "matched") .& (df[:kind] .== "_Exp"), col_order]);
    df4 = DataFrames.stack(df[(df[:id] .== "matched") .& (df[:kind] .== "_NExp"), col_order]);

    col_names = string.(col_order);
    col_names = map(x -> replace(x, "Brand" => "Target Brand"), col_names);
    col_names = map(x -> replace(x, "Cat" => "Category"), col_names);
    col_names = map(x -> replace(x, "Pre" => "Pre-Campaign"), col_names);
    col_names = map(x -> replace(x, "Pos" => "Post-Campaign"), col_names);
    col_names = map(x -> split(x, '_')[2]*" "*split(x, '_')[1], col_names);

    data1 = viz_build_report("data_p5_t2_a", string(df1[:value]));
    data2 = viz_build_report("data_p5_t2_b", string(df2[:value]));
    data3 = viz_build_report("data_p5_t2_c", string(df3[:value]));
    data4 = viz_build_report("data_p5_t2_d", string(df4[:value]));
    data = vcat(data1, data2, data3, data4);

    label = viz_build_report("lbl_p5_t2", string(col_names));

    return data, label
end

function viz_sales_metrics_raw_matched_doll_per_trip(df_raw_metrics::DataFrame, df_matched_metrics::DataFrame)
    col_order = [:Pre_Brand, :Pos_Brand, :Pre_Cat, :Pos_Cat];
    df = DataFrame(id=String[], kind=String[], Pre_Brand=Float64[], Pos_Brand=Float64[], Pre_Cat=Float64[], Pos_Cat=Float64[]);
    datasets = Dict("raw" => df_raw_metrics, "matched" => df_matched_metrics);
    for (k, v) in datasets
        for kind in ["_Exp", "_NExp"]
            df_new = v[v[:metrics] .== "doll", filter(x -> contains(string(x), kind), names(v))];
            names!(df_new, map(x -> Symbol(replace(string(x), kind => "")), names(df_new)));
            # df_new = map(x -> x.*100, eachcol(df_new)); # NOT NEEDED IN THIS CASE
            df_new[:id] = k;
            df_new[:kind] = kind;
            df_new = df_new[vcat([:id, :kind], col_order)];
            append!(df, df_new);
        end
    end
    df1 = DataFrames.stack(df[(df[:id] .== "raw") .& (df[:kind] .== "_Exp"), col_order]);
    df2 = DataFrames.stack(df[(df[:id] .== "raw") .& (df[:kind] .== "_NExp"), col_order]);
    df3 = DataFrames.stack(df[(df[:id] .== "matched") .& (df[:kind] .== "_Exp"), col_order]);
    df4 = DataFrames.stack(df[(df[:id] .== "matched") .& (df[:kind] .== "_NExp"), col_order]);

    col_names = string.(col_order);
    col_names = map(x -> replace(x, "Brand" => "Target Brand"), col_names);
    col_names = map(x -> replace(x, "Cat" => "Category"), col_names);
    col_names = map(x -> replace(x, "Pre" => "Pre-Campaign"), col_names);
    col_names = map(x -> replace(x, "Pos" => "Post-Campaign"), col_names);
    col_names = map(x -> split(x, '_')[2]*" "*split(x, '_')[1], col_names);

    data1 = viz_build_report("data_p5_t3_a", string(df1[:value]));
    data2 = viz_build_report("data_p5_t3_b", string(df2[:value]));
    data3 = viz_build_report("data_p5_t3_c", string(df3[:value]));
    data4 = viz_build_report("data_p5_t3_d", string(df4[:value]));
    data = vcat(data1, data2, data3, data4);

    label = viz_build_report("lbl_p5_t3", string(col_names));

    return data, label
end

# VISUALIZATION web page generation function
function viz_write(htmldoc::String, data::DataFrame, label::DataFrame; extra=nothing)
    for r in eachrow(data)
        needle = "var "*r[:id]*" = [];";
        replac = "var "*r[:id]*" = "*r[:value]*";";
        htmldoc = replace(htmldoc, needle => replac);
    end
    for r in eachrow(label)
        needle = "var "*r[:id]*" = [];";
        replac = replace(("var "*r[:id]*" = "*r[:value])*";", "String[" => "[");
        htmldoc = replace(htmldoc, needle => replac);
    end
    if !isnothing(extra)
        for r in eachrow(extra)
            needle = "var "*r[:id]*" = [];";
            replac = "var "*r[:id]*" = "*"\""*r[:value]*"\""*";";
            htmldoc = replace(htmldoc, needle => replac);
        end
    end

    return htmldoc
end

# VISUALIZATION template generation functions
function viz_out_prep(dfd_fr_shr_ndx::DataFrame, exposed_net::DataFrame, control_net::DataFrame, dfd_upc_grwth_cnt::DataFrame, dfo_Exp::DataFrame, dfo_UnExp::DataFrame, exposed_buyer_by_week::DataFrame, cumulative_by_week::DataFrame, Byr_Frq_Dgtl::DataFrame, Tm_1st_by_lst_xpsur_Dgtl::DataFrame, Frst_Buy_Frq_Dgtl_std::DataFrame, Tot_Frq_Dgtl::DataFrame, dfd_Cum_IMP::DataFrame, df_proscore_dist::DataFrame, df_retailer_dist::DataFrame, df_dolocc_bins::DataFrame, df_raw_metrics::DataFrame, df_matched_metrics::DataFrame)
    # Page 1 charts
    d1, l1, e1 = viz_product_sales(dfd_fr_shr_ndx);
    d2, l2, e2 = viz_brand_names_sales(exposed_net, control_net);
    d3, l3 = viz_contribution_to_target_brand_change_in_sales(dfd_fr_shr_ndx);
    d4, l4 = viz_upcs_pos_contrib_to_sale_change(dfd_upc_grwth_cnt);
    # Page 2 charts
    d5, l5 = viz_buyer_class_exp(dfo_Exp);
    d6, l6 = viz_buyer_class_unexp(dfo_UnExp);
    d7, l7 = viz_exposures_per_buyer_type(exposed_buyer_by_week);
    d8, l8 = viz_cum_exposures_per_buyer_type(cumulative_by_week);
    # Page 3 charts
    d9, l9 = viz_exposure_distribution(Byr_Frq_Dgtl);
    d10, l10 = viz_weeks_last_exp_1stbuy_distribut(Tm_1st_by_lst_xpsur_Dgtl);
    d11, l11 = viz_exposure_before_1stbuy_distribution(Frst_Buy_Frq_Dgtl_std);
    d12, l12 = viz_impressions_dist(Tot_Frq_Dgtl);
    d13, l13 = viz_total_exposures_distrib(dfd_Cum_IMP);
    # Page 4 charts
    d14, l14 = viz_proscore_distribution(df_proscore_dist);
    d15, l15 = viz_retailer_distribution(df_retailer_dist);
    d16, l16 = viz_dolocc_bins_targetbrand(df_dolocc_bins);
    d17, l17 = viz_dolocc_bins_category(df_dolocc_bins);
    # Page 5 charts
    d18, l18 = viz_sales_metrics_raw_matched_buyers(df_raw_metrics, df_matched_metrics);
    d19, l19 = viz_sales_metrics_raw_matched_trips(df_raw_metrics, df_matched_metrics);
    d20, l20 = viz_sales_metrics_raw_matched_doll_per_trip(df_raw_metrics, df_matched_metrics);

    data = sort(vcat(d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15, d16, d17, d18, d19, d20), :id);
    label = sort(vcat(l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, l14, l15, l16, l17, l18, l19, l20), :id);
    extra = sort(vcat(e1, e2), :id);

    return data, label, extra
end

# Module signature function --- REQUIRES: XRayDataPrep
function viz_RUN(template_file::String=Sys.BINDIR*"/rep/XRayVisualizationUI.html", out_file::String="./rep.html")
    print("\n-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.\n")
    print("\n\t\t\t~~~XRay Visualization Tool~~~\n")
    println("********************************************************************************")
    println("STEP 1: Data Prep")
    println("********************************************************************************")
    # Generate inputs from XRayDataPrep
    descDump, df_desc, brand_data, hhcounts_date, upc_data, udj_avg_expsd_pst, udj_avg_cntrl_pst, df_cdma_dump, df_upcs_mx, combined, freq_index, expocnt, imp_week = viz_dataprep();
    print("\n\n\n\t---> STEP 1 completed!\n\n\n")

    println("********************************************************************************")
    println("STEP 2: Generate Reports")
    println("********************************************************************************")
    # Generate report data frames to be shown in the visualization tool
    dfd_fr_shr_ndx = fairshare(df_cdma_dump, brand_data);
    exposed_net, control_net = brand_dist(df_upcs_mx);
    dfd_upc_grwth_cnt = upc_growth(df_cdma_dump, upc_data);
    dfo_Exp, dfo_UnExp = genbuyerclass(df_cdma_dump, udj_avg_expsd_pst, udj_avg_cntrl_pst);
    exposed_buyer_by_week, cumulative_by_week = Buyer_Frequency_Characteristics(hhcounts_date, df_cdma_dump);
    Byr_Frq_Dgtl = freq_HH_buying(combined);
    Tm_1st_by_lst_xpsur_Dgtl = first_buy_last_exp(combined);
    Frst_Buy_Frq_Dgtl_std, Frst_Buy_Frq_Dgtl_Dyn = freq_HH_Cum1stpur(combined, freq_index); # `Frst_Buy_Frq_Dgtl_Dyn` not needed
    Tot_Frq_Dgtl = Total_freq_digital(hhcounts_date);
    dfd_Cum_IMP = Cum_IMP(expocnt);
    df_proscore_dist = catg_relfreq(descDump, :model);
    df_retailer_dist = catg_relfreq(descDump, :banner);
    df_dolocc_bins = dolocc_bins(descDump);
    df_raw_metrics = sales_metrics(df_desc);
    df_matched_metrics = sales_metrics(descDump);
    print("\n\n\n\t---> STEP 2 completed!\n\n\n")

    println("********************************************************************************")
    println("STEP 3: Create Web Page Visualization Tool")
    println("********************************************************************************")
    # Create data frames with the data and labels to be inserted in the web page
    data, label, extra = viz_out_prep(dfd_fr_shr_ndx, exposed_net, control_net, dfd_upc_grwth_cnt, dfo_Exp, dfo_UnExp, exposed_buyer_by_week, cumulative_by_week, Byr_Frq_Dgtl, Tm_1st_by_lst_xpsur_Dgtl, Frst_Buy_Frq_Dgtl_std, Tot_Frq_Dgtl, dfd_Cum_IMP, df_proscore_dist, df_retailer_dist, df_dolocc_bins, df_raw_metrics, df_matched_metrics);

    # Load web page template, insert the relevant data and labels, and save the final HTML file to disk
    htmldoc = read(template_file, String);
    htmldoc = viz_write(htmldoc, data, label; extra = extra);
    write(out_file, htmldoc);

    print("\n\nGenerated HTML VISUALIZATION TOOL and saved to\n\nas "*out_file*".html\n")
    print("\n\n\n\t---> STEP 3 completed!\n\n")
    print("-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.\n")
    println("")
end

# !!!--> WRITE COMMON FUNCTION FOR PAGE 5 CHART FUNCTIONS
# !!!--> Should I have a call of `format_unify_reports` inside `viz_RUN`?

end

using .XRayReporting
