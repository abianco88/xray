# Julia v0.5.2
module DESCReporting

using DataFrames, DataStructures

function sales_metrics(dfd::DataFrame)
    #= Calculate: 1) the proportion of buyers of target brand and category among the exposed and unexposed in pre-campaign and post-campaign; 2) the average dollars-per-trip for buyers of target brand and category among the exposed and unexposed in pre-campaign and post-campaign; 3) the average trips for buyers of target brand and category among the exposed and unexposed in pre-campaign and post-campaign. =#
    df_groups = groupby(dfd, :group);  # first grouped dataframe is `group=0`, second is `group=1`
    metric_types = Dict{Symbol, Array{Symbol,1}}(:buyers=>[:buyer_pre_p1, :buyer_pre_p0, :buyer_pos_p1, :buyer_pos_p0], :doll=>[:dol_per_trip_pre_p1, :dol_per_trip_pre_p0, :dol_per_trip_pos_p1, :dol_per_trip_pos_p0], :trps=>[:trps_pre_p1, :trps_pre_p0, :trps_pos_p1, :trps_pos_p0]);
    value_names = ["Pre_Brand_", "Pre_Cat_", "Pos_Brand_", "Pos_Cat_"];
    df_metrics = DataFrame(group = Int64[]; metrics = String[], val_name = String[], val = Float64[]);
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
    df_metrics[:val_name] = ifelse.(df_metrics[:group] .== 1, df_metrics[:val_name]*"Exp", df_metrics[:val_name]*"NExp");
    df_metrics = unstack(df_metrics, :metrics, :val_name, :val);
    df_metrics = df_metrics[[:metrics, :Pre_Brand_Exp, :Pre_Brand_NExp, :Pre_Cat_Exp, :Pre_Cat_NExp, :Pos_Brand_Exp, :Pos_Brand_NExp, :Pos_Cat_Exp, :Pos_Cat_NExp]];    # --> Do we need to have this specific column order in the output report table?

    return df_metrics
end

function dolocc_bins(dfd::DataFrame)   # --> Perhaps bins should be created based on the range of the variables?
    #= For exposed and unexposed buyers of target brand and exposed and unexposed buyers of category, calculate the number of HHs per each level of average dollars spent per trip in pre-campaign and post-campaign. =#
    bin_first = 0;
    bin_last = 42;
    df_groups = groupby(dfd, :group);  # first grouped dataframe is `group=0`, second is `group=1`
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
                append!(df_dolocc_bins, DataFrame(group = repeat([grp], outer=[4]), bin = repeat([bin_counter], outer=[4]), val_name = value_names, val = [nrow(subdf[(subdf[col] .> (k-2)) & (subdf[col] .<= k), :]) for col in dolocc_bins_types[k]]));
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
    df_dolocc_bins[:val_name] = ifelse.(df_dolocc_bins[:group] .== 1, df_dolocc_bins[:val_name]*"Exp", df_dolocc_bins[:val_name]*"NExp");
    df_dolocc_bins = join(df_bin_names[unique(df_bin_names[:bin]), :], unstack(df_dolocc_bins, :bin, :val_name, :val), on=:bin, kind=:inner);
    df_dolocc_bins = df_dolocc_bins[[:sales, :Pre_Brand_Exp, :Pre_Brand_NExp, :Pre_Cat_Exp, :Pre_Cat_NExp, :Pos_Brand_Exp, :Pos_Brand_NExp, :Pos_Cat_Exp, :Pos_Cat_NExp]];  # --> Do we need to have this specific column order in the output report table?

    return df_dolocc_bins
end

function catg_relfreq(dfd::DataFrame, catg_var::Symbol)
    #= Calculate the relative frequency (%) of `model` (= proscore) or `banner` (= retailer) among exposed and controls. =#
    if catg_var == :model
        catg_nm = :Proscore;
    elseif catg_var == :banner
        catg_nm = :Banner;
    else
        catg_nm = :UNKNOWN;
    end
    df_groups = groupby(dfd[[:group, catg_var]], :group);  # first grouped dataframe is `group=0`, second is `group=1`
    df_out = hcat(by(df_groups[1], catg_var, d -> round(nrow(d)/nrow(df_groups[1]), 2)), by(df_groups[2], catg_var, d -> round(nrow(d)/nrow(df_groups[2]), 2))[:x1]);
    rename!(df_out, [catg_var, :x1, :x1_1], [catg_nm, :Control, :Exposed]);

    return df_out
end

function upc_stats(dfd::DataFrame)
    #= Calculate average net price, quantity, number of HHs, total sales, and total units sold of target brand by group and UPC in pre-campaign and post-campaign. =#
    df_stats = by(dfd[dfd[:product_grp_id] .== 1, :], [:group, :period_buyer, :derived_upc10], d -> [mean(d[:net_price]), sum(d[:quantity]), length(unique(d[:panid])), sum(d[:net_price]), sum(d[:units])]);
    df_stats[:var_name] = repeat(["avg_net_price", "sum_quantity", "distinct_experian_id", "sum_net_price", "sum_units"], outer=[Int64(size(df_stats, 1)/5)]);
    df_stats = unstack(df_stats, :var_name, :x1)[[:period_buyer, :group, :derived_upc10, :avg_net_price, :sum_quantity, :distinct_experian_id, :sum_net_price, :sum_units]];   # --> Do we need to have this specific column order in the output report table?
    rename!(df_stats, [:group, :derived_upc10], [:exposed_flag, :derived_upc]);
    df_stats_pre = df_stats[df_stats[:period_buyer] .== 1, setdiff(names(df_stats), [:period_buyer])];  # `period_buyer=1` is pre-campaign
    df_stats_pos = df_stats[df_stats[:period_buyer] .== 2, setdiff(names(df_stats), [:period_buyer])];  # `period_buyer=2` is post-campaign

    return df_stats_pre, df_stats_pos
end

function brand_dist(dfd::DataFrame)
    #= By group and campaign period calculate the dollar sales and share of sales for each 3-tuple tsv_brand-majorbrand-product_grp_id.  =#
    df_subs = by(dfd, [:group, :period_buyer], d -> DataFrame(sales = sum(d[:net_price])));
    df_calc = by(dfd, [:group, :period_buyer, :tsv_brand, :majorbrand, :product_grp_id], d -> DataFrame(sums = sum(d[:net_price])));
    df_calc = join(df_calc, df_subs, on=[:group, :period_buyer], kind=:inner);
    df_calc[:pct] = df_calc[:sums]./df_calc[:sales]*100;
    #= for col in [:sums, :pct]
        df_calc[isna(df_calc[col]), col] = 0.0;
    end =#
    df_calc[:block] = 2;

    df_calc_tot = by(df_calc, [:group, :period_buyer, :product_grp_id], df -> DataFrame(sums = sum(df[:sums]), pct = sum(df[:pct])));
    df_calc_tot[:tsv_brand] = "Total_brand";
    df_calc_tot[:majorbrand] = "Total";
    df_calc_tot[:block] = 1;

    df_all = vcat(df_calc_tot[[:group, :period_buyer, :block, :tsv_brand, :majorbrand, :product_grp_id, :sums, :pct]], df_calc[[:group, :period_buyer, :block, :tsv_brand, :majorbrand, :product_grp_id, :sums, :pct]]);
    df_all[:prd] = ifelse(df_all[:period_buyer] .== 1, "PRD1", "PRD2");
    df_sales = rename!(unstack(df_all[[:group, :prd, :block, :tsv_brand, :majorbrand, :product_grp_id, :sums]], :prd, :sums), [:PRD1, :PRD2], [:Pre_Dol_Sales, :Pos_Dol_Sales]);
    df_shares = rename!(unstack(df_all[[:group, :prd, :block, :tsv_brand, :majorbrand, :product_grp_id, :pct]], :prd, :pct), [:PRD1, :PRD2], [:Pre_Dol_Sales_Share, :Pos_Dol_Sales_Share]);
    df_out = join(df_sales, df_shares, on=[:group, :tsv_brand, :majorbrand, :product_grp_id]);
    for col in [:Pre_Dol_Sales, :Pos_Dol_Sales, :Pre_Dol_Sales_Share, :Pos_Dol_Sales_Share]
        df_out[isna(df_out[col]), col] = 0.0;
    end
    sort!(df_out, cols = [:block, :product_grp_id]);

    exposed_net = df_out[df_out[:group] .== 1, [:tsv_brand, :majorbrand, :product_grp_id, :Pre_Dol_Sales, :Pos_Dol_Sales, :Pre_Dol_Sales_Share, :Pos_Dol_Sales_Share]];
    control_net = df_out[df_out[:group] .== 0, [:tsv_brand, :majorbrand, :product_grp_id, :Pre_Dol_Sales, :Pos_Dol_Sales, :Pre_Dol_Sales_Share, :Pos_Dol_Sales_Share]];

    return exposed_net, control_net
end

end
