# Julia v0.5.2
module CDMAReporting

using Base.Dates, DataFrames

function buyerfreqs(group::Int, definition::Dict{Symbol,Any}, dfd::DataFrame)
    if haskey(definition, :buyer_pos_p1) && haskey(definition, :buyer_pre_52w_p1) && haskey(definition, :buyer_pre_52w_p0)
        buyers_count = nrow(dfd[(dfd[:group] .== group) & (dfd[:buyer_pos_p1] .== definition[:buyer_pos_p1]) & (dfd[:buyer_pre_52w_p1] .== definition[:buyer_pre_52w_p1]) & (dfd[:buyer_pre_52w_p0] .== definition[:buyer_pre_52w_p0]), :]);
    elseif haskey(definition, :buyer_pos_p1) && haskey(definition, :buyer_pre_52w_p1) && haskey(definition, :trps_pos_p1)
        buyers_count = nrow(dfd[(dfd[:group] .== group) & (dfd[:buyer_pos_p1] .== definition[:buyer_pos_p1]) & (dfd[:buyer_pre_52w_p1] .== definition[:buyer_pre_52w_p1]) & (dfd[:trps_pos_p1] .> definition[:trps_pos_p1]), :]);
    elseif haskey(definition, :buyer_pos_p1) && haskey(definition, :buyer_pre_52w_p1)
        buyers_count = nrow(dfd[(dfd[:group] .== group) & (dfd[:buyer_pos_p1] .== definition[:buyer_pos_p1]) & (dfd[:buyer_pre_52w_p1] .== definition[:buyer_pre_52w_p1]), :]);
    elseif haskey(definition, :buyer_pos_p1)
        buyers_count = nrow(dfd[(dfd[:group] .== group) & (dfd[:buyer_pos_p1] .== definition[:buyer_pos_p1]), :]);
    else
        buyers_count = nrow(dfd[dfd[:group] .== group, :]);
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
        classname = uppercase(replace(string(k), '_', " "));
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

function genbuyerclass(dfd::DataFrame, udj_avg_expsd_pst::Float64, udj_avg_cntrl_pst::Float64)
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

    dfo = DataFrame(reptype=[], desc=[], val=[], cnt=[]);
    for k in keys(exp_unexp_dict)
        buyerclass = Dict{Symbol,Int64}(Symbol(def[:name])=>buyerfreqs(exp_unexp_dict[k][:group], def, dfd) for def in definitions);
        dfo = buyermetrics(buyerclass, exp_unexp_dict[k], dfo);    # Calculations different from documentation: expected count and % of buyers!
    end

    dfo_Exp = DataFrame(buyer_type = dfo[dfo[:reptype] .== :buyer_exposed, :desc], buyer_percent = dfo[dfo[:reptype] .== :buyer_exposed, :val], CNT = dfo[dfo[:reptype] .== :buyer_exposed, :cnt]);
    dfo_UnExp = DataFrame(buyer_type = dfo[dfo[:reptype] .== :buyer_unexposed, :desc], buyer_percent = dfo[dfo[:reptype] .== :buyer_unexposed, :val], CNT = dfo[dfo[:reptype] .== :buyer_unexposed, :cnt]);
# NOTE: DATA FRAME CONTENT IS SAME AS IN ORIGINAL CDMA CODE, BUT ROW ORDER IS DIFFERENT (not sure it matters)
    return dfo_Exp, dfo_UnExp
end

function gentrialrepeat(dfd::DataFrame, udj_avg_expsd_pst::Float64, udj_avg_cntrl_pst::Float64)
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

    dfo = DataFrame(reptype=[], id=[], desc=[], val=[] ,cnt=[]);
    for k in keys(exp_unexp_dict)
        buyerclass = Dict{Symbol,Int64}(Symbol(def[:name])=>buyerfreqs(exp_unexp_dict[k][:group], def, dfd) for def in definitions);
        trierclass = Dict{Symbol,Int64}(Symbol(def[:name])=>buyerfreqs(exp_unexp_dict[k][:group], def, dfd) for def in definitions_trier);
        dfo = triermetrics(trierclass, buyerclass, exp_unexp_dict[k], dfo);    # Calculations different from documentation!!!
    end

    dfo_1 = unstack(dfo, :id, :desc, :val);
    dfo_2 = unstack(dfo, :id, :desc, :cnt);
    dfo_1 = dfo_1[sort(names(dfo_1), rev=true)];
    dfo_2 = dfo_2[sort(names(dfo_2), rev=true)];
    names!(dfo_2, map(parse, map(x->replace(string(x), "_percent", "_cnt"), names(dfo_2))));
    dfo_f = join(dfo_1, dfo_2, on=:id, kind=:inner);
    dfo_f[:grouptype] = [x == 1 ? "Exposed" : "Unexposed" for x in dfo_f[:id]];
    delete!(dfo_f, :id);
    dfo_f = sort(dfo_f[vcat(:grouptype, filter!(x->string(x)!=string(:grouptype), names(dfo_f)))]);

    return dfo_f
end

function fairshare(dfd::DataFrame, brand_data::DataFrame)
    #= For buyers of target brand in post-campaign period who are exposed, compute the proportion of change in target brand dollar sales coming from the change in competitor product dollar sales (`pp_of_feature_brand` = change of competitor product dollar sales/change of target brand dollar sales). Then, to obtain `fair_share_index`, adjust it for the pre-campaign proportion of competitor dollar sales of total competitor sales. =#
    brand_data = deepcopy(brand_data);
    rename!(brand_data, [:product_id, :group_name], [:id, :product]);   # ATTENTION: Rename cols of dataframe outside this function too!!!

    prd_net_pr_pos = [:prd_1_net_pr_pos, :prd_2_net_pr_pos, :prd_3_net_pr_pos, :prd_4_net_pr_pos, :prd_5_net_pr_pos, :prd_6_net_pr_pos, :prd_7_net_pr_pos, :prd_8_net_pr_pos, :prd_9_net_pr_pos, :prd_10_net_pr_pos];
    prd_net_pr_pre = [:prd_1_net_pr_pre, :prd_2_net_pr_pre, :prd_3_net_pr_pre, :prd_4_net_pr_pre, :prd_5_net_pr_pre, :prd_6_net_pr_pre, :prd_7_net_pr_pre, :prd_8_net_pr_pre, :prd_9_net_pr_pre, :prd_10_net_pr_pre];

    prd_sales_sums = aggregate(dfd[(dfd[:group] .== 1) & (dfd[:buyer_pos_p1] .== 1), vcat([:group, :buyer_pos_p1], prd_net_pr_pos, prd_net_pr_pre)], [:group, :buyer_pos_p1], sum);
    df = melt(prd_sales_sums, [:group, :buyer_pos_p1]);
    df[:id] = map(x->parse(Int, split(string(x), "_")[2]), df[:variable]);
    df[:desc] = map(x->split(string(x), "_")[end-1]*"_sales", df[:variable]);
    df = df[:, [:id, :desc, :value]];
    totals = aggregate(df, :desc, sum);

    df[:percent] = 0.0;
    for salestype in unique(totals[:desc])
        df[df[:desc] .== salestype, :percent] = df[df[:desc] .== salestype, :value]./totals[totals[:desc] .== salestype, :value_sum];
    end

    df_val = unstack(df, :id, :desc, :value);
    df_pct = unstack(df, :id, :desc, :percent);
    names!(df_pct, map(x->x != :id ? Symbol(string(x)*"_pct") : x, names(df_pct)));
    df_pct[:pct_change] = df_pct[:pos_sales_pct]-df_pct[:pre_sales_pct];
    targetbrand_pct_change = df_pct[(df_pct[:id] .== 1), :pct_change][1];
    df_pct[:pct_of_targetbrand] = df_pct[:pct_change]./(-targetbrand_pct_change);

    df_agg = join(df_val, df_pct, on=:id, kind=:inner);
    df_agg[:fair_share_index] = df_agg[:pct_of_targetbrand]./(df_agg[:pre_sales]/sum(df_agg[df_agg[:id] .!= 1, :pre_sales]))*100;
    df_agg[df_agg[:id] .== 1, [:pct_of_targetbrand, :fair_share_index]] = 0.0;
    df_agg = df_agg[(df_agg[:pre_sales] .!= 0) & (df_agg[:pos_sales] .!= 0), :];

    agg_fair_share_index = join(df_agg, brand_data, on=:id, kind=:left);
    ordered_cols = [:id, :product, :pos_sales, :pos_sales_pct, :pre_sales, :pre_sales_pct, :pct_change, :pct_of_targetbrand, :fair_share_index];
    agg_fair_share_index = agg_fair_share_index[ordered_cols];
    rename!(agg_fair_share_index, :id, :product_grp_id);

    return agg_fair_share_index
end

function targetshare_of_category(dfd::DataFrame)    # Previously called `Share_of_requirements`; compute target brand/category sales in post-campaign
    #= In the post-campaign period, calculate the target brand proportion of total category dollar sales for exposed and unexposed =#
    sales_sum = aggregate(dfd[[:group, :prd_0_net_pr_pos, :prd_1_net_pr_pos]], :group, sum);
    sales_sum[:product_group_share] = sales_sum[:prd_1_net_pr_pos_sum]./sales_sum[:prd_0_net_pr_pos_sum];
    agg_share_of_requirements = sales_sum[[:group, :product_group_share]];
    rename!(agg_share_of_requirements, :group, :exposed_flag);

    return agg_share_of_requirements
end

function upc_growth(dfa::DataFrame, upc_data::DataFrame);
    #= Breakdown of contribution to the product growth during campaign period by UPC (only consider the positive contributions) =#
    upc_data = deepcopy(upc_data);
    dfupc = DataFrame(DESCRIPTION=[], UPC=[], pos_sales=[], pre_sales=[], pos_sales_share=[], pre_sales_share=[], GROWTH_SALES=[], growth_contribution=[]);
    rename!(upc_data, :experian_id, :panid);
    j = join(upc_data, dfa[dfa[:group] .== 1, [:panid,:group]], on=:panid);
    upc_1 = by(j, [:period, :upc, :description], d->DataFrame(sales = sum(d[:net_price])));
    upc_1_1 = upc_1[upc_1[:period] .== 1, [:upc, :description, :sales]];
    upc_1_2 = upc_1[upc_1[:period] .== 2, [:upc, :description, :sales]];
    upc_1_3 = join(upc_1_1, upc_1_2, on=[:upc,:description], kind=:outer);
    rename!(upc_1_3, :sales, :pre_sales);
    rename!(upc_1_3, :sales_1, :pos_sales);
    upc_1_3[isna(upc_1_3[:pos_sales]), :pos_sales] = 0;
    upc_1_3[isna(upc_1_3[:pre_sales]), :pre_sales] = 0;
    upc_1_3[:pre_sales_share] = upc_1_3[:pre_sales]./sum(upc_1_3[:pre_sales]);
    upc_1_3[:pos_sales_share] = upc_1_3[:pos_sales]./sum(upc_1_3[:pos_sales]);
    upc_1_3[:growth_contribution] = (upc_1_3[:pos_sales].-upc_1_3[:pre_sales])/abs((sum(upc_1_3[:pos_sales]).-sum(upc_1_3[:pre_sales])))*100;
    sort!(upc_1_3, cols=[:growth_contribution], rev=true);

    function findchar(a, charac)
        m=0;
        for i in (1:length(a))
            if a[i] == charac
                m = i;
                break
            end
        end

        return m
    end

    upc_1_3[:DESCRIPTION_WO_SIZE] = map(x->reverse(SubString(SubString(reverse(SubString(x, 1, findlast(x, '-')-2)), findchar(reverse(SubString(x, 1, findlast(x, '-')-2)), ' ')+1, 1000), findchar(SubString(reverse(SubString(x, 1,findlast(x, '-')-2)), findchar(reverse(SubString(x, 1, findlast(x, '-')-2)), ' ')+1, 1000), ' ')+1, 1000)), upc_1_3[:description]);
    upc_1_3[:TYPE] = map(x->reverse(SubString(reverse(SubString(x, 1, findlast(x, '-')-2)), 1, 2)), upc_1_3[:description]);
    upc_1_3[:SIZE] = map(x->parse(Float64, reverse(SubString(SubString(reverse(SubString(x, 1, findlast(x, '-')-2)), 4, 1000), 1, findchar(SubString(reverse(SubString(x, 1, findlast(x, '-')-2)), 4, 1000), ' ')-1))), upc_1_3[:description]);
    upc_1_4 = upc_1_3[upc_1_3[:pre_sales] .== 0, :];
    upc_1_5 = upc_1_3[upc_1_3[:pre_sales] .!= 0, :];

    upc_3 = unique(upc_1_5[:, [:SIZE, :TYPE, :DESCRIPTION_WO_SIZE]]);
    sort!(upc_3, cols=[:DESCRIPTION_WO_SIZE, :TYPE, :SIZE]);
    upc_3[:LAG_SIZE] = (upc_3[:SIZE].-vcat(0, upc_3[:SIZE][1:length(upc_3[:SIZE])-1]));
    upc_3[:PERCENT_LAG] = (upc_3[:LAG_SIZE])./upc_3[:SIZE]*100;
    upc_3[:ROW_NN] = 1:nrow(upc_3);

    upc_4 = DataFrame();
    for i in groupby(upc_3, [:DESCRIPTION_WO_SIZE, :TYPE])
        i[:ROW_NN] = 1:nrow(i);
        upc_4 = vcat(i,upc_4);
    end
    upc_4[:LAG_SIZE] = map((x, y, z)->ifelse(x .== 1, y, z), upc_4[:ROW_NN], upc_4[:SIZE], upc_4[:LAG_SIZE]);
    upc_4[:PERCENT_LAG] = map((x, y)->ifelse(x .== 1, 100, y), upc_4[:ROW_NN], upc_4[:PERCENT_LAG]);
    sort!(upc_4, cols=[:DESCRIPTION_WO_SIZE, :TYPE, :SIZE]);
    upc_4[:NEW_SIZE] = map((x, y)->ifelse(x > 30, y, 0), upc_4[:PERCENT_LAG], upc_4[:SIZE]);
    for i in 2:size(upc_4, 1)
        if upc_4[i,:NEW_SIZE] .== 0;
            upc_4[i,:NEW_SIZE] = upc_4[i-1, :NEW_SIZE];
        end
    end

    sort!(upc_4, cols=[:DESCRIPTION_WO_SIZE, :TYPE, :SIZE]);
    upc_5 = deepcopy(upc_4);
    upc_6 = deepcopy(upc_5);
    upc_6[:ROW_NN] = map(x->x-1, upc_6[:ROW_NN]);

    upc_7 = join(upc_5, upc_6, on=[:ROW_NN, :TYPE, :DESCRIPTION_WO_SIZE], kind=:left);
    upc_7 = upc_7[:, [:SIZE, :TYPE, :DESCRIPTION_WO_SIZE, :LAG_SIZE, :PERCENT_LAG_1, :ROW_NN, :NEW_SIZE]];
    rename!(upc_7, :PERCENT_LAG_1, :PERCENT_LAG);
    sort!(upc_7, cols=[:SIZE], rev=true);
    sort!(upc_7, cols=[:TYPE, :DESCRIPTION_WO_SIZE]);
    upc_7[isna(upc_7[:PERCENT_LAG]), :PERCENT_LAG] = 100;
    upc_7[:NEW_SIZE] = map((x, y)->ifelse(x > 30, y ,0), upc_7[:PERCENT_LAG], upc_7[:SIZE]);
    for i in 2:size(upc_7, 1)
        if upc_7[i, :NEW_SIZE] .== 0
            upc_7[i, :NEW_SIZE] = upc_7[i-1, :NEW_SIZE];
        end
    end
    sort!(upc_7, cols=[:TYPE, :DESCRIPTION_WO_SIZE, :SIZE]);

    upc_8 = join(upc_4, upc_7, on=[:TYPE, :DESCRIPTION_WO_SIZE, :ROW_NN]);
    upc_8 = upc_8[:, [:DESCRIPTION_WO_SIZE, :TYPE, :SIZE, :ROW_NN, :NEW_SIZE, :NEW_SIZE_1]];
    rename!(upc_8, :NEW_SIZE, :LOW_SIZE);
    rename!(upc_8, :NEW_SIZE_1, :HIGH_SIZE);
    upc_dummy = DataFrame(TYPE = unique(upc_3[:TYPE]));
    upc_8 = join(upc_8, upc_dummy, on=:TYPE);
    upc_8[:SIZE_LEVEL] = map((x, y, z)->ifelse(x == y, string(y, " ", z), string("(", x, " ", z, " - ", y, " ", z, ")")), upc_8[:LOW_SIZE], upc_8[:HIGH_SIZE], upc_8[:TYPE]);
    upc_9 = join(upc_1_5, upc_8, on=[:SIZE, :TYPE, :DESCRIPTION_WO_SIZE]);
    upc_9 = by(upc_9, [:DESCRIPTION_WO_SIZE, :SIZE_LEVEL], df->DataFrame(pre_sales = sum(df[:pre_sales]), pos_sales = sum(df[:pos_sales]), AGG = size(df, 1), UPC = minimum(df[:upc])));
    upc_9[:GROWTH_SALES] = upc_9[:pos_sales].-upc_9[:pre_sales];

    if nrow(upc_1_4) > 0
        upc_10 = deepcopy(upc_1_4);
        upc_10[:SIZE_LEVEL] = map((x, y)->string(x, " ", y), upc_10[:SIZE], upc_10[:TYPE]);
        upc_10 = by(upc_10, [:DESCRIPTION_WO_SIZE, :SIZE_LEVEL], df->DataFrame(pre_sales = sum(df[:pre_sales]), pos_sales = sum(df[:pos_sales]), AGG = size(df, 1), UPC = minimum(df[:upc])));
        upc_10[:GROWTH_SALES] = upc_10[:pos_sales].-upc_10[:pre_sales];
        upc_11 = vcat(upc_9, upc_10);
    else
        upc_11 = upc_9;
    end
    upc_11[:DESCRIPTION] = map((x, y)->string(x, " - ", y), upc_11[:DESCRIPTION_WO_SIZE], upc_11[:SIZE_LEVEL]);
    upc_11[:pre_sales_share] = upc_11[:pre_sales]./sum(upc_11[:pre_sales]);
    upc_11[:pos_sales_share] = upc_11[:pos_sales]./sum(upc_11[:pos_sales]);

    upc_12 = upc_11[upc_11[:GROWTH_SALES] .> 0, :];
    upc_12[:growth_contribution] = (upc_12[:pos_sales].-upc_12[:pre_sales])/abs((sum(upc_12[:pos_sales]).-sum(upc_12[:pre_sales])));
    sort!(upc_12, cols=[:growth_contribution], rev=true);
    upc_12[:UPC] = map((x, y)->ifelse(x .== 1, string(y), string("Aggregation of ", x, " UPCS")), upc_12[:AGG], upc_12[:UPC]);
    upc_final = upc_12[:, [:DESCRIPTION, :UPC, :pos_sales, :pre_sales, :pos_sales_share, :pre_sales_share, :GROWTH_SALES, :growth_contribution]];
    dfupc = vcat(dfupc, upc_final);

    rename!(dfupc, :DESCRIPTION, :description);
    rename!(dfupc, :UPC, :upc10);
    rename!(dfupc, :pos_sales, :sales_upc_post);
    rename!(dfupc, :pre_sales, :sales_upc_pre);
    rename!(dfupc, :pos_sales_share, :percentage_sales_upc_post);
    rename!(dfupc, :pre_sales_share, :percentage_sales_upc_pre);
    rename!(dfupc, :GROWTH_SALES, :growth_sales);
    dfupc = dfupc[:, [:description, :upc10, :sales_upc_pre, :sales_upc_post, :percentage_sales_upc_pre, :percentage_sales_upc_post, :growth_contribution]];
    upc_growth = deepcopy(dfupc);
    agg_upc_growth = DataFrame(description = [], upc10 = [], sales_upc_pre = [], sales_upc_post = [], percentage_sales_upc_pre = [], percentage_sales_upc_post = [], growth_contribution = []);
    for i in unique(upc_growth[:description])
        push!(agg_upc_growth, [i, upc_growth[upc_growth[:description] .== i, :upc10][1], mean(upc_growth[upc_growth[:description] .== i, :sales_upc_pre]), mean(upc_growth[upc_growth[:description] .== i, :sales_upc_post]), mean(upc_growth[upc_growth[:description] .== i, :percentage_sales_upc_pre]), mean(upc_growth[upc_growth[:description] .== i, :percentage_sales_upc_post]), mean(upc_growth[upc_growth[:description] .== i, :growth_contribution])]);
    end
    sort!(agg_upc_growth, cols=:growth_contribution, rev=true);

    return agg_upc_growth
end

function freq_HH_Cum1stpur(combined::DataFrame, freq_index::DataFrame)
    #= For `First_Buy_by_Frequency_Digital_std`, compute the frequency (number of HHs) of exposures before first purchase (capped to 10 exposures), the cumulative frequency, and the cumulative frequency percentage of total exposures before the first purchase. For `First_Buy_by_Frequency_Digital_Dyn`, compute the frequency (number of HHs) of exposure levels (ranges of exposures) before first purchase (capped), the cumulative frequency, and the cumulative percentage of total exposure levels before the first purchase =#
    Exposed_Buyer = deepcopy(combined[combined[:Number_exposure_before_1st_buy] .!= 0, [:Number_exposure_before_1st_buy]]);
    Exposed_Buyer[Exposed_Buyer[:Number_exposure_before_1st_buy] .>= 10, :Number_exposure_before_1st_buy] = 10;
    Exposed_Buyer_1 = by(Exposed_Buyer, [:Number_exposure_before_1st_buy], nrow);
    Exposed_Buyer_1[:Cum_1st_purchases_capped] = cumsum(Exposed_Buyer_1[:x1]);
    Exposed_Buyer_1[:Percentage_of_total_1st_purchases] = Exposed_Buyer_1[:Cum_1st_purchases_capped]/sum(Exposed_Buyer_1[:x1]);
    Exposed_Buyer_1[:Obs] = collect(1:size(Exposed_Buyer_1, 1));
    Exposed_Buyer_final = Exposed_Buyer_1[:, [:Obs, :Number_exposure_before_1st_buy, :x1, :Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]];
    names!(Exposed_Buyer_final, [:Obs, :Frequency, :Buying_HHs, :Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]);
    First_Buy_by_Frequency_Digital_std = Exposed_Buyer_final;
    #Calculate 1st Buy by Dynamic Frequency Buckets
    Exposed_Buyer_dyn = deepcopy(combined[combined[:Number_exposure_before_1st_buy] .!= 0, [:Number_exposure_before_1st_buy]]);
    Exposed_Buyer_dyn[:Exposures] = "Exposures_le_"*string(freq_index[:frq_index][1]);
    
    for dec in range(1, length(freq_index[:frq_index])-2);
        Exposed_Buyer_dyn[(Exposed_Buyer_dyn[:Number_exposure_before_1st_buy] .> freq_index[:frq_index][dec]) & (Exposed_Buyer_dyn[:Number_exposure_before_1st_buy] .<= freq_index[:frq_index][dec+1]) & (freq_index[:frq_index][dec] .!= freq_index[:frq_index][dec+1]), :Exposures] = "Exposures_g_"*string(freq_index[:frq_index][dec])*"_le_"*string(freq_index[:frq_index][dec+1]);
    end

    Exposed_Buyer_dyn[(Exposed_Buyer_dyn[:Number_exposure_before_1st_buy] .> freq_index[:frq_index][end-1]) & (freq_index[:frq_index][end-1] .!= freq_index[:frq_index][end-2]), :Exposures] = "Exposures_ge_"*string(freq_index[:frq_index][end-1]);
    Exposed_Buyer_1_dyn=by(Exposed_Buyer_dyn, [:Exposures], nrow);
    Exposed_Buyer_1_dyn = join(Exposed_Buyer_1_dyn, freq_index, on=:Exposures, kind=:left);
    Exposed_Buyer_1_dyn = sort!(Exposed_Buyer_1_dyn, cols = order(:frq_index));
    Exposed_Buyer_1_dyn[:Cum_1st_purchases_capped] = cumsum(Exposed_Buyer_1_dyn[:x1]);
    Exposed_Buyer_1_dyn[:Percentage_of_total_1st_purchases] = Exposed_Buyer_1_dyn[:Cum_1st_purchases_capped]/sum(Exposed_Buyer_1_dyn[:x1]);
    Exposed_Buyer_1_dyn[:Obs] = collect(1:size(Exposed_Buyer_1_dyn, 1));
    Exposed_Buyer_final_dyn = Exposed_Buyer_1_dyn[:, [:Obs, :Exposures, :x1, :Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]];
    names!(Exposed_Buyer_final_dyn, [:Obs, :Frequency, :Buying_HHs, :Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]);
    First_Buy_by_Frequency_Digital_Dyn = Exposed_Buyer_final_dyn;

    return First_Buy_by_Frequency_Digital_std, First_Buy_by_Frequency_Digital_Dyn
end

function freq_HH_buying(combined::DataFrame)
    #= Calculate the count of HHs and the proportion of the total HHs per number of exposures (capped to 10) =#
    buyer_freq = deepcopy(combined[:, [:Exposures]]);
    buyer_freq[buyer_freq[:Exposures] .>= 10, :Exposures] = 10;
    buyer_freq_1 = by(buyer_freq, [:Exposures], nrow);
    buyer_freq_1[:Obs] = collect(1:size(buyer_freq_1, 1));
    buyer_freq_1[:Percentage_of_buying_HHs] = buyer_freq_1[:x1]/sum(buyer_freq_1[:x1]);
    buyer_freq_1 = buyer_freq_1[:, [:Obs, :Exposures, :x1, :Percentage_of_buying_HHs]];
    names!(buyer_freq_1, [:Obs, :Exposures, :HHs, :Percentage_of_buying_HHs]);
    Buyer_Frequency_Digital = buyer_freq_1;

    return Buyer_Frequency_Digital
end

function first_buy_last_exp(combined::DataFrame)
    #= Calculate the HH count and percentage of total HHs (this only for buying HHs in post-campaign period) for each level of number of weeks lapsed between last exposure and first purchase. Then, only for buying HHs in post-campaign period, calculate the average number of exposures per each level of number of weeks lapsed between last exposure and first purchase and an "adjusted average" computed as the previous average plus 2.35 times the standard deviation of the number of exposures prior to the first purchase each level of number of weeks lapsed between last exposure and first purchase [--> NOTE: THIS METRIC IS MISLEADINGLY LABELLED `Avg_Exposures_to_1st_buy_without_outliers`. NOT SURE IT'S CORRECTLY USED.]. =#
    buyer_exposure = deepcopy(combined[:, [:Time, :Number_exposure_before_1st_buy]]);
    buyer_exposure_1 = by(buyer_exposure, [:Time], nrow);
    buyer_exposure_1[:Obs] = collect(1:size(buyer_exposure_1, 1));
    buyer_exposure_1[:Percentage_of_total_buying_HHs] = 0.0;
    buyer_exposure_1[buyer_exposure_1[:Time] .!= "Pre", :Percentage_of_total_buying_HHs] = buyer_exposure_1[buyer_exposure_1[:Time] .!= "Pre", :x1]/sum(buyer_exposure_1[buyer_exposure_1[:Time] .!= "Pre", :x1]);
    buyer_exposure_1 = hcat(buyer_exposure_1, by(buyer_exposure, [:Time], buyer_exposure->mean(buyer_exposure[:Number_exposure_before_1st_buy])));
    buyer_exposure = join(buyer_exposure, by(buyer_exposure, [:Time], buyer_exposure->mean(buyer_exposure[:Number_exposure_before_1st_buy]).+2.35.*std(buyer_exposure[:Number_exposure_before_1st_buy])), on=:Time, kind=:left);
    buyer_exposure_2 = by(buyer_exposure, [:Time], buyer_exposure->mean(buyer_exposure[buyer_exposure[:Number_exposure_before_1st_buy] .<= buyer_exposure[:x1], :Number_exposure_before_1st_buy]));
    buyer_exposure_final = join(buyer_exposure_1, buyer_exposure_2, on=:Time, kind=:left);
    buyer_exposure_final = buyer_exposure_final[:, [:Obs, :Time, :x1, :Percentage_of_total_buying_HHs, :x1_1, :x1_2]];
    names!(buyer_exposure_final, [:Obs, :Time, :Buying_HHs, :Percentage_of_total_buying_HHs, :Avg_Exposures_to_1st_buy, :Avg_Exposures_to_1st_buy_without_outliers]);

    return buyer_exposure_final;
end

function Total_freq_digital(hhcounts_date::DataFrame)
    #= Calculate the count of HHs and percentage of total HHs per each exposure level (capped to 10 exposures). Based on hhcounts_date.cvs. =#
    exp_data2 = hhcounts_date;
    exp_data2 = exp_data2[exp_data2[:brk] .== unique(exp_data2[:brk])[1], :];
    expocnt_1 = by(exp_data2, [:panid], df->DataFrame(impressions = sum(df[:impressions])));
    rename!(expocnt_1, :impressions, :Exposures);
    expocnt_1[(expocnt_1[:Exposures] .>= 10), :Exposures] = 10;
    Total_Freq = by(expocnt_1, [:Exposures], nrow);
    names!(Total_Freq, [:Exposures, :HHs]);
    Total_Freq[:Percentage_of_Total_HHs] = Total_Freq[:HHs]/sum(Total_Freq[:HHs]);
    Total_Freq = hcat(Total_Freq, collect(1:size(Total_Freq, 1)));
    Total_Freq = Total_Freq[:, [:x1, :Exposures, :HHs, :Percentage_of_Total_HHs]];
    names!(Total_Freq, [:Obs, :Exposures, :HHs, :Percentage_of_Total_HHs]);

    return Total_Freq
end

function Cum_IMP(expocnt::DataFrame)
    #= By exposure level (i.e., number of exposures), compute the number of of HHs, the number of impressions served (= exposures*HHs), the cumulative number of impressions served, and the capped impressions served. Capped impressions served are calculated as the product of the total HHs from each exposure level to the max exposure level and the number of exposures for that level, summed to the previous level value [--> NOTE: Not sure `imps_served_capped` is a clear label. The metric measures the number of impressions that has translated to at least as many exposures as the exposure level.]. =#
    Cum_IMPs = by(expocnt, [:Exposures], nrow);
    names!(Cum_IMPs, [:Exposures, :HHs]);
    Cum_IMPs[:imps_Served] = map(Int, Cum_IMPs[:HHs].*Cum_IMPs[:Exposures]);
    Cum_IMPs[:CUM_IMPs_Served] = cumsum(Cum_IMPs[:imps_Served]);
    Cum_IMPs = hcat(Cum_IMPs, collect(1:size(Cum_IMPs, 1)));
    Cum_IMPs[(Cum_IMPs[:x1] .>= Cum_IMPs[:Exposures]), [:x1, :Exposures]];
    Cum_IMPs[:imps_served_capped] = sum(Cum_IMPs[:HHs]);

    for row in 2:size(Cum_IMPs, 1);
        Cum_IMPs[row, :imps_served_capped] = ((Cum_IMPs[row, :Exposures])*sum(Cum_IMPs[row:size(Cum_IMPs, 1), :HHs]))+Cum_IMPs[row-1, :CUM_IMPs_Served];
    end

    Cum_IMPs = Cum_IMPs[:, [:x1, :Exposures, :HHs, :imps_Served, :CUM_IMPs_Served, :imps_served_capped]];
    names!(Cum_IMPs, [:Obs, :Exposures, :HHs, :imps_Served, :CUM_IMPs_Served, :imps_served_capped]);

    return Cum_IMPs
end

function Buyer_Frequency_Characteristics(hhcounts_date::DataFrame, dfd::DataFrame)
    #= Create table mapping IRI weeks to "real time" weeks for the campaign period. For each IRI week, compute the number of lapsed buyers, brand switch buyers, brand buyers, and category buyers (exposed_buyer_by_week). For each IRI week, compute the proportion of cumulative sum of the count of lapsed buyers, brand switch buyers, brand buyers, and category buyers throughout the entire campaign, i.e., across all weeks (cumulative_by_week). =#
    df = Dates.DateFormat("y-m-d");
    dt_base = Date("2014-12-28", df);
    buyer_exposure = join(dfd, hhcounts_date, on=:panid, kind=:inner);
    buyer_exposure = sort!(buyer_exposure[:, [:panid, :buyer_pos_p1, :buyer_pre_52w_p1, :buyer_pre_52w_p0, :trps_pos_p1, :dte, :impressions]], cols=[order(:dte)]);
    buyer_exposure[:dte] = map(x->string(SubString(string(x), 1, 4), '-', SubString(string(x), 5, 6), '-', SubString(string(x), 7, 8)), buyer_exposure[:dte]);
    buyer_exposure[:iri_week] = map(x->convert(Int64, 1843+round(ceil(convert(Int64, (Date(x, df)-dt_base))/7), 0)), buyer_exposure[:dte]);
    buyer_exposure_dates = by(buyer_exposure, :iri_week, buyer_exposure->minimum(buyer_exposure[:dte]));

    exposed_buyer_by_week = DataFrame(iri_week=Int64[], WEEK_ID=String[], PCT_LAPSED_BUYERS=Int64[], PCT_BRAND_SWITCH_BUYERS=Int64[], PCT_BRAND_BUYERS=Int64[], PCT_CATEGORY_BUYERS=Int64[]);
    for l=(1:length(buyer_exposure_dates[:iri_week]))
        merge_table_per_date = buyer_exposure[buyer_exposure[:iri_week] .== buyer_exposure_dates[l, :iri_week], :];
        PCT_LAPSED_BUYERS = sum(merge_table_per_date[(merge_table_per_date[:buyer_pos_p1] .== 0) & (merge_table_per_date[:buyer_pre_52w_p1] .== 1), :impressions]);
        PCT_BRAND_SWITCH_BUYERS = sum(merge_table_per_date[(merge_table_per_date[:buyer_pos_p1] .== 1) & (merge_table_per_date[:buyer_pre_52w_p1] .== 0) & (merge_table_per_date[:buyer_pre_52w_p0] .== 1), :impressions]);
        PCT_BRAND_BUYERS = sum(merge_table_per_date[(merge_table_per_date[:buyer_pos_p1] .== 1), :impressions]);
        PCT_CATEGORY_BUYERS = sum(merge_table_per_date[(merge_table_per_date[:buyer_pre_52w_p0] .== 1), :impressions]);
        final_pur_data_buyer_temp = [buyer_exposure_dates[l, :iri_week] buyer_exposure_dates[l, :x1] PCT_LAPSED_BUYERS PCT_BRAND_SWITCH_BUYERS PCT_BRAND_BUYERS PCT_CATEGORY_BUYERS];
        push!(exposed_buyer_by_week, final_pur_data_buyer_temp);
    end

    rowsum = DataFrame(sum(Array(exposed_buyer_by_week[:, collect(3:ncol(exposed_buyer_by_week))]), 1));
    exposed_buyer_by_week_copy = deepcopy(exposed_buyer_by_week);
    for i in 3:ncol(exposed_buyer_by_week_copy)
        exposed_buyer_by_week_copy[i] = cumsum(exposed_buyer_by_week_copy[i]);
    end
    for i in 3:ncol(exposed_buyer_by_week_copy)
        exposed_buyer_by_week_copy[i] = exposed_buyer_by_week_copy[i]./rowsum[i-2][1]*100;
    end
    cumulative_by_week = deepcopy(exposed_buyer_by_week_copy);

    return exposed_buyer_by_week, cumulative_by_week;
end

end
