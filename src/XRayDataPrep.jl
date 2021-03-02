# Julia v1.5.3

module XRayDataPrep

using CSV, DataFrames, DataStructures, Dates

export viz_dataprep

# COMMON inputs
function common_load_input(dump_file::String)
    # Load `match dump` file
    descDump = CSV.read(dump_file, DataFrame); # Required for `df_cdma_dump` creation: is it ready before Lift?
 
    # Load `orig.csv` file
    orig_cols_cdma = [:panid, :group, :prd_1_net_pr_pre, :prd_2_net_pr_pre, :prd_3_net_pr_pre, :prd_4_net_pr_pre, :prd_5_net_pr_pre, :prd_6_net_pr_pre, :prd_7_net_pr_pre, :prd_8_net_pr_pre, :prd_9_net_pr_pre, :prd_10_net_pr_pre, :prd_0_net_pr_pos, :prd_1_net_pr_pos, :prd_2_net_pr_pos, :prd_3_net_pr_pos, :prd_4_net_pr_pos, :prd_5_net_pr_pos, :prd_6_net_pr_pos, :prd_7_net_pr_pos, :prd_8_net_pr_pos, :prd_9_net_pr_pos, :prd_10_net_pr_pos, :buyer_pos_p1, :buyer_pos_p0, :buyer_pre_52w_p1, :buyer_pre_52w_p0, :trps_pos_p1];
    orig_cols_desc = [:panid, :group, :buyer_pos_p1, :buyer_pos_p0, :trps_pos_p1, :buyer_pre_p1, :buyer_pre_p0, :dol_per_trip_pre_p1, :dol_per_trip_pre_p0, :dol_per_trip_pos_p1, :dol_per_trip_pos_p0, :trps_pos_p0, :trps_pre_p1, :trps_pre_p0, :banner, :model];
    orig_cols = unique(vcat(orig_cols_cdma, orig_cols_desc));

    orig_hdr_df = CSV.read("./origHead.csv", DataFrame; header=0, threaded=true);
    orig_hdr_df[:Column1] = lowercase.(orig_hdr_df[:Column1]);  # Make sure all column names are lowercase - not default in MTA
    orig_hdr = Symbol.(convert(Vector{String}, orig_hdr_df[:Column1]));
    for i in 1:length(orig_hdr) orig_hdr[i] = orig_hdr[i] == :iri_link_id ? :banner : orig_hdr[i] end
    for i in 1:length(orig_hdr) orig_hdr[i] = orig_hdr[i] == :proscore ? :model : orig_hdr[i] end
    for i in 1:length(orig_hdr) orig_hdr[i] = orig_hdr[i] == :experian_id ? :panid : orig_hdr[i] end
    for i in 1:length(orig_hdr) orig_hdr[i] = orig_hdr[i] == :exposed_flag ? :group : orig_hdr[i] end

    orig_cols_indx = Int.(indexin(orig_cols, orig_hdr));
    orig_cols_dict = Dict("Column".*string.(orig_cols_indx) .=> orig_cols);

    orig_df = CSV.read("./orig.csv", DataFrame; select=orig_cols_indx, header=false, threaded=true);
    DataFrames.rename!(orig_df, orig_cols_dict);

    # Select relevant column of `orig.csv` for CDMA and DESC
    df_cdma = orig_df[orig_cols_cdma];
    df_desc = orig_df[orig_cols_desc];

    return descDump, df_cdma, df_desc
end

# CDMA data prep
function cdma_load_input(scored_file::String)
    # Load CDMA specific input files
    brand_data = CSV.read("./CDMA/brand_data.csv", DataFrame);
    buyer_week_data = CSV.read("./CDMA/buyer_week_data.csv", DataFrame);
    hhcounts_date = CSV.read("./CDMA/hhcounts_date.csv", DataFrame);
    imp_week = CSV.read("./CDMA/imp_week.csv", DataFrame);  # imp_week not needed (it's required for Unify only!)
    upc_data = CSV.read("./CDMA/upc_data.csv", DataFrame);

    # Load `scored.csv` and read the 2 values needed for the adjustments
    scored = CSV.read(scored_file, DataFrame);
    udj_avg_expsd_pst = scored[(scored[:MODEL_DESC] .== "Total Campaign") .& (scored[:dependent_variable] .== "pen"), :UDJ_AVG_EXPSD_HH_PST][1];
    udj_avg_cntrl_pst = scored[(scored[:MODEL_DESC] .== "Total Campaign") .& (scored[:dependent_variable] .== "pen"), :UDJ_AVG_CNTRL_HH_PST][1];

    return brand_data, buyer_week_data, hhcounts_date, imp_week, upc_data, udj_avg_expsd_pst, udj_avg_cntrl_pst
end

function cdma_base_dataprep(df_cdma::DataFrame, descDump::DataFrame)
    # Subset `orig.csv` by `match dump` for CDMA
    df_cdma_dump = DataFrames.innerjoin(df_cdma, descDump[:, [:panid]], on = :panid);

    return df_cdma_dump
end

function cdma_freq_dataprep(hhcounts_date::DataFrame, buyer_week_data::DataFrame)   # Port of Sampath's original: same logic!
    buyer_week_data = deepcopy(buyer_week_data);
    exp_data_n1 = hhcounts_date[:, [:panid, :lvl, :impressions]];
    pur_data_id = DataFrame(panid = deepcopy(unique(buyer_week_data[:experian_id])));
    hhcounts_date_new = DataFrames.innerjoin(hhcounts_date, pur_data_id, on = :panid);
    hhcounts_date_new = hhcounts_date_new[:, [:panid, :lvl, :dte, :impressions]];
    DataFrames.rename!(hhcounts_date_new, [:exposureid, :Exposures, :iriweek, :imp]);
    hhcounts_date_new[:iriweek] = map(x->string(SubString(string(x), 5, 6), '/', SubString(string(x), 7, 8), '/', SubString(string(x), 1, 4)),hhcounts_date_new[:iriweek]);
    BinSize = ["1" 1 1; "2 to 4" 2 4; "5 to 10" 5 10; "11+" 11 10000000];
    DataFrames.rename!(buyer_week_data, [:exposureid, :iriweek]);
    exp_data1 = hhcounts_date_new;
    exp_data1[:time1] = map(x->datetime2unix(DateTime(x, DateFormat("mm/dd/yyyy  HH:MM:SS"))), exp_data1[:iriweek]);
    exp_data1[:timestamp] = map(x->DateTime(x, DateFormat("mm/dd/yyyy  HH:MM:SS")), exp_data1[:iriweek]);
    ##Add additional information to purchase data
    pur_data1 = buyer_week_data;
    pur_data1[:trans_date] = rata2datetime.(722694 .+ 7 .* pur_data1[:iriweek]);
    pur_data1[:time1] = datetime2unix.(pur_data1[:trans_date]);
    purch = sort!(pur_data1, [order(:exposureid), order(:iriweek), order(:trans_date)]);
    #Creating the first purchase and occasion
    purcdate = purch[:, [:exposureid, :trans_date]];
    purcdate = combine(DataFrames.groupby(purcdate, :exposureid), :trans_date => minimum);
    DataFrames.rename!(purcdate, [:exposureid, :firstsale]);
    purchase = combine(DataFrames.groupby(purch, :exposureid), nrow);
    DataFrames.rename!(purchase, [:exposureid, :occ]);
    purch1 = DataFrames.innerjoin(purch, purcdate, on = :exposureid);
    #Reading the date values
    exp = sort!(exp_data1, [order(:exposureid), order(:timestamp), order(:Exposures), order(:imp)]);
    #Creating the first exposure and last exposure before purchase
    exp_temp = DataFrames.innerjoin(exp, purch1, on = :exposureid, makeunique = true);
    exp_temp = exp_temp[(exp_temp[:timestamp] .<= exp_temp[:firstsale]), :];
    expdate_1 = exp[:, [:exposureid, :timestamp]];
    expdate_1 = combine(DataFrames.groupby(expdate_1, :exposureid), :timestamp => minimum);
    DataFrames.rename!(expdate_1, [:exposureid, :firstexpo]);
    expdate_2 = combine(DataFrames.groupby(exp_temp[:, [:exposureid, :timestamp]], :exposureid), :timestamp => maximum);
    DataFrames.rename!(expdate_2, [:exposureid, :latestexpo]);
    expdate = DataFrames.outerjoin(expdate_1, expdate_2, on = :exposureid);
    exp1 = DataFrames.innerjoin(exp, expdate, on = :exposureid);
    lastexpo = DataFrames.innerjoin(exp1, purcdate, on = :exposureid);
    lastexpo[:diff] = lastexpo[:firstsale]-lastexpo[:timestamp];
    lastexpo1 = lastexpo[Dates.value.(lastexpo.diff) .>= 0, :];
    expodate = expdate;
    expocnt = combine(DataFrames.groupby(exp1, :exposureid), :imp => sum);
    DataFrames.rename!(expocnt, [:exposureid, :Exposures]);
    bef_Purch = lastexpo1[(lastexpo1[:timestamp] .== lastexpo1[:latestexpo]), [:exposureid, :latestexpo]];
    bef_cnt = combine(DataFrames.groupby(lastexpo1, :exposureid), :imp => sum);
    DataFrames.rename!(bef_cnt, [:exposureid, :Exposures_To_1st_Buy]);
    #Merging first and last exposure, first purchase, purchase occasion information
    combined = DataFrames.innerjoin(expodate[:, [:exposureid, :firstexpo]], expocnt, on = :exposureid);
    combined = DataFrames.innerjoin(combined, purcdate, on = :exposureid);
    combined = DataFrames.leftjoin(combined, purchase, on = :exposureid);
    combined = DataFrames.leftjoin(combined, bef_Purch, on = :exposureid);
    combined = DataFrames.leftjoin(combined, bef_cnt, on = :exposureid);
    #Removing NAs from Exposures_To_1st_Buy
    combined[ismissing.(combined[:Exposures_To_1st_Buy]), :Exposures_To_1st_Buy] = 0;
    #Calculate days/weeks difference between first purchase and last exposure to purchase
    combined[:day_gap] = 0;
    combined[.!ismissing.(combined[:latestexpo]), :day_gap] = Int.(Dates.value.(combined[.!ismissing.(combined[:latestexpo]), :firstsale]-combined[.!ismissing.(combined[:latestexpo]), :latestexpo])/86400000).+1;
    combined[:day_gap_in_weeks] = combined[:day_gap]/7;
    combined[:week_gap] = "Pre";
    combined[(combined[:day_gap_in_weeks] .> 0) .& (combined[:day_gap_in_weeks] .< 2), :week_gap] = "1 Week (or less)";
    combined[(combined[:day_gap_in_weeks] .>= 2 ) .& (combined[:day_gap_in_weeks] .< 3), :week_gap] = "2 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 3) .& (combined[:day_gap_in_weeks] .< 4), :week_gap] = "3 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 4) .& (combined[:day_gap_in_weeks] .< 5), :week_gap] = "4 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 5) .& (combined[:day_gap_in_weeks] .< 6), :week_gap] = "5 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 6) .& (combined[:day_gap_in_weeks] .< 7), :week_gap] = "6 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 7) .& (combined[:day_gap_in_weeks] .< 8), :week_gap] = "7 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 8) .& (combined[:day_gap_in_weeks] .< 9), :week_gap] = "8 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 9) .& (combined[:day_gap_in_weeks] .< 10), :week_gap] = "9 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 10), :week_gap] = "Over 10 Weeks";
    combined = sort!(combined, [order(:exposureid)]);
    combined = hcat(combined, collect(1:size(combined, 1)));
    combined = combined[:, [:x1, :exposureid, :firstexpo, :occ, :firstsale, :Exposures, :latestexpo, :Exposures_To_1st_Buy, :day_gap, :day_gap_in_weeks, :week_gap]];
    DataFrames.rename!(combined, [:Obs, :exposureid, :First_Exposure, :Purchase_Occasions, :First_Purchase_Weekending, :Exposures, :Date_last_exposure_before_1st_buy, :Number_exposure_before_1st_buy, :Days_between_last_exposure_first_buy, :Weeks_between_last_exposure_first_buy, :Time]);

    exp_data2 = deepcopy(hhcounts_date);
    exp_data2 = exp_data2[exp_data2[:brk] .== "frequency_type_dym", :];
    freq_index = unique(exp_data2[:, [:lvl, :frq_index]]);
    freq_index = sort!(freq_index, order(:frq_index));
    DataFrames.rename!(freq_index, [:Exposures, :frq_index]);

    return combined, freq_index, expocnt
end

# DESCRIPTIVES  data prep
function desc_load_input()  # --> Check columns are imported correctly and with right name!!!
    # Load `Brand_upc.csv` file
    brand_upc_hdr_df = CSV.read("./Descriptives/N.csv", DataFrame);
    brand_upc_hdr = Symbol.(convert(Vector{String}, brand_upc_hdr_df[:str_colname]));
    brand_upc = CSV.read("./Descriptives/Brand_upc.csv", DataFrame; header=brand_upc_hdr, threaded=true);

    return brand_upc
end

function desc_base_dataprep(brand_upc::DataFrame, descDump::DataFrame)
    # Merge columns from brand_upc to descDump
    df_upcs_mx = DataFrames.innerjoin(descDump[[:panid, :group]], brand_upc, on = :panid);

    return df_upcs_mx
end

# Module signature function
function viz_dataprep(scored_file::String="./scored.csv", dump_file::String="./dump.csv")
    descDump, df_cdma, df_desc = common_load_input(dump_file);
    brand_data, buyer_week_data, hhcounts_date, imp_week, upc_data, udj_avg_expsd_pst, udj_avg_cntrl_pst = cdma_load_input(scored_file);   # imp_week not needed (it's required for Unify only!)
    df_cdma_dump = cdma_base_dataprep(df_cdma, descDump);
    brand_upc = desc_load_input();
    df_upcs_mx = desc_base_dataprep(brand_upc, descDump);
    combined, freq_index, expocnt = cdma_freq_dataprep(hhcounts_date, buyer_week_data);

    return descDump, df_desc, brand_data, hhcounts_date, upc_data, udj_avg_expsd_pst, udj_avg_cntrl_pst, df_cdma_dump, df_upcs_mx, combined, freq_index, expocnt, imp_week
end

end

using .XRayDataPrep
