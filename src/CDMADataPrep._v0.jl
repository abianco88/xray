#=
# --> NOTE: `imp_week` is never used in the reporting! Maybe remove it?
=#


# Julia v0.5.2
module CDMADataPrep

using Base.Dates, DataFrames, DataStructures, CLib, IRIfunc, XLib   # CLib needed for `rf`; IRIfunc needed for `CFGwf`; XLib needed for `strdf`

function pre_data_check(pre_qc_data::Array{String,1}, cfg::OrderedDict)
    res = true
    for i = 1:length(pre_qc_data)
        if pre_qc_data[i] in ["brand_data", "upc_data", "hhcounts_date", "buyer_week_data"]
            try
                UInt(isfile(dirname(cfg[:files][:orig])*"/CDMA/"*basename(cfg[:files][Symbol(pre_qc_data[i])]))-1)
            catch
                res = false
                println("CDMA :: $(dirname(cfg[:files][:orig])*"/CDMA/"*basename(cfg[:files][Symbol(pre_qc_data[i])])) File/Path is missing")
            end
        else
            try
                UInt(isfile(cfg[:files][Symbol(pre_qc_data[i])])-1)                       
            catch
                res = false
                println("CDMA :: $(cfg[:files][Symbol(pre_qc_data[i])]) File/Path is missing")
            end
        end
    end
    res
end

function qc_check(df_qc_data::DataFrame, qc_mandatory_vars::Array{Symbol,1}, qc_datatype::Array{DataType,1}, data_name::String)
    if size(df_qc_data) < (1, length(qc_mandatory_vars))
        println("CDMA :: Check  $(data_name) file for rows and columns")
        exit();
    end

    if (length(setdiff(qc_mandatory_vars, names(df_qc_data)))) > 0
        println("CDMA :: $(setdiff(qc_mandatory_vars, names(df_qc_data))) variables missing in $data_name file")
        exit();
    end  

    for i in 1:length(qc_mandatory_vars)
        if (eltype(df_qc_data[qc_mandatory_vars][i]) == qc_datatype[i]) .== false
            println("CDMA :: The data type for $(qc_mandatory_vars[i]) in $(data_name) is $(eltype(df_qc_data[qc_mandatory_vars][i])), It should be $(qc_datatype[i])")
        end
    end

    for i in 1:length(qc_mandatory_vars)
        if (eltype(df_qc_data[qc_mandatory_vars][i]) == qc_datatype[i]) .== false
            if qc_datatype[i] == Int64
                try
                    df_qc_data[qc_mandatory_vars[i]] = Int64.(df_qc_data[qc_mandatory_vars[i]]);
                    println("CDMA :: Converted $(qc_mandatory_vars[i]) in $(data_name) in $(qc_datatype[i]) data type")
                catch
                    println("CDMA :: Failed to convert $(qc_mandatory_vars[i]) in $(data_name) in required $(qc_datatype[i]) data type")
                    exit();
                end
            end
            if qc_datatype[i] == Float64
                try
                    df_qc_data[qc_mandatory_vars[i]] = Float64.(df_qc_data[qc_mandatory_vars[i]]);
                    println("CDMA :: Converted $(qc_mandatory_vars[i]) in $(data_name) in $(qc_datatype[i]) data type")
                catch
                    println("CDMA :: Failed to convert $(qc_mandatory_vars[i]) in $(data_name) in required $(qc_datatype[i]) data type")
                    exit();
                end
            end
            if qc_datatype[i] == String
                try
                    df_qc_data[qc_mandatory_vars[i]] = string.(collect(df_qc_data[qc_mandatory_vars[i]]));
                    println("CDMA :: Converted $(qc_mandatory_vars[i]) in $(data_name) in $(qc_datatype[i]) data type")
                catch
                    println("CDMA :: Failed to convert $(qc_mandatory_vars[i]) in $(data_name) in required $(qc_datatype[i]) data type")
                    exit();
                end
            end
        end
    end
end

function qc_pos_data(dfd::DataFrame, brand_data::DataFrame, upc_data::DataFrame, hhcounts_date::DataFrame, imp_week::DataFrame, flag::Int64)
    cols_orig = [:panid, :prd_1_net_pr_pre, :prd_2_net_pr_pre, :prd_3_net_pr_pre, :prd_4_net_pr_pre, :prd_5_net_pr_pre, :prd_6_net_pr_pre, :prd_7_net_pr_pre, :prd_8_net_pr_pre, :prd_9_net_pr_pre, :prd_10_net_pr_pre, :prd_0_net_pr_pos, :prd_1_net_pr_pos, :prd_2_net_pr_pos, :prd_3_net_pr_pos, :prd_4_net_pr_pos, :prd_5_net_pr_pos, :prd_6_net_pr_pos, :prd_7_net_pr_pos, :prd_8_net_pr_pos, :prd_9_net_pr_pos, :prd_10_net_pr_pos, :trps_pos_p1, :buyer_pos_p0, :buyer_pos_p1, :group, :buyer_pre_52w_p1, :buyer_pre_52w_p0];
    type_orig = [Int64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Int64, Int64, Int64, Any, Int64, Int64];
    cols_brand_data = [:product_id, :group_name];
    type_brand_data = [Int64, String];
    cols_upc_data = [:experian_id, :period,:upc, :description, :net_price];
    type_upc_data = [Int64, Int64, Int64, String, Float64];
    cols_hhcounts_date = [:brk, :lvl, :panid, :dte, :impressions];
    type_hhcounts_date = [String ,String, Int64, Int64, Int64];
    cols_imp_week = [:iri_week, :exposure_date, :hhs, :impressions];
    type_imp_week = [Int64, String, Int64, Int64];

    qc_check(dfd, cols_orig, type_orig, "orig_csv");
    qc_check(brand_data, cols_brand_data, type_brand_data, "brand_data");
    qc_check(upc_data, cols_upc_data, type_upc_data, "upc_data");
    qc_check(hhcounts_date, cols_hhcounts_date, type_hhcounts_date, "hhcounts_date");
    if flag == 1
        qc_check(imp_week, cols_imp_week, type_imp_week, "imp_week");
    end
end

function cdma_dataprep(cfg::DataStructures.OrderedDict{Any,Any}, brand_data::DataFrame, upc_data::DataFrame, hhcounts_date::DataFrame, buyer_week_data::DataFrame, imp_week::DataFrame, descDump::DataFrame, src::String, flag::Int64)
    hdr = Array(strdf(rf(cfg[:files][:hdr]), header=false)[:x1]);
    for i in 1:length(hdr) hdr[i] = hdr[i] == "iri_link_id" ? "banner" : hdr[i] end
    for i in 1:length(hdr) hdr[i] = hdr[i] == "proscore" ? "model" : hdr[i] end
    pre_data_check(["orig", "brand_data", "upc_data", "hhcounts_date", "buyer_week_data"], cfg);    # Check if files exists in expected location
    M = MFile(hdr, cfg);
    c = [:panid, :group, :prd_1_net_pr_pre, :prd_2_net_pr_pre, :prd_3_net_pr_pre, :prd_4_net_pr_pre, :prd_5_net_pr_pre, :prd_6_net_pr_pre, :prd_7_net_pr_pre, :prd_8_net_pr_pre, :prd_9_net_pr_pre, :prd_10_net_pr_pre, :prd_0_net_pr_pos, :prd_1_net_pr_pos, :prd_2_net_pr_pos, :prd_3_net_pr_pos, :prd_4_net_pr_pos, :prd_5_net_pr_pos, :prd_6_net_pr_pos, :prd_7_net_pr_pos, :prd_8_net_pr_pos, :prd_9_net_pr_pos, :prd_10_net_pr_pos, :buyer_pos_p1, :buyer_pos_p0, :buyer_pre_52w_p1, :buyer_pre_52w_p0, :trps_pos_p1];
    M2 = MFile(c, M);
    dfmx = creadData([-1], M2, "", src);
    dfd = join(dfmx, descDump[:, [:panid]], on=:panid,  kind=:inner);
    qc_pos_data(dfd, brand_data, upc_data, hhcounts_date, imp_week, flag);    # Check if required files satisfy requirements

    return dfmx, dfd, brand_data, upc_data, hhcounts_date, buyer_week_data, imp_week    # `dfmx` not used by any function, only `dfd` needed
end

function base_dataprep(isenc::Bool, flag::Int64=1)
    # flag = 1 -> Include `imp_week` in data requirements checks
    cfg = CFGwf(isenc);

    brand_data = readtable(cfg[:files][:brand_data]);
    buyer_week_data = readtable(cfg[:files][:buyer_week_data]);
    descDump = readtable(cfg[:files][:Match_dump]);   # Required for `dfd` creation: is it ready before Lift?
    hhcounts_date = readtable(cfg[:files][:hhcounts_date]);
    imp_week = readtable("./CDMA/imp_week.csv");    # Not in the `cfg` dictionary!!!
    upc_data = readtable(cfg[:files][:upc_data]);
    src = rf(cfg[:files][:orig]);

    dfmx, dfd, brand_data, upc_data, hhcounts_date, buyer_week_data, imp_week = cdma_dataprep(cfg, brand_data, upc_data, hhcounts_date, buyer_week_data, imp_week, descDump, src, flag);

    return dfd, brand_data, upc_data, hhcounts_date, buyer_week_data, imp_week
end

function freq_dataprep(hhcounts_date::DataFrame, buyer_week_data::DataFrame) # Need to be reviewed and possibly changed
    outD = Dict();
    buyer_week_data = deepcopy(buyer_week_data);
    exp_data_n1 = hhcounts_date[:, [:panid, :lvl, :impressions]];
    pur_data_id = DataFrame(panid = deepcopy(unique(buyer_week_data[:experian_id])));
    hhcounts_date_new = join(hhcounts_date, pur_data_id, on=:panid, kind=:inner);
    hhcounts_date_new = hhcounts_date_new[:, [:panid, :lvl, :dte, :impressions]];
    names!(hhcounts_date_new, [:exposureid, :Exposures, :iriweek, :imp]);
    hhcounts_date_new[:iriweek] = map(x->string(SubString(string(x), 5, 6), '/', SubString(string(x), 7, 8), '/', SubString(string(x), 1, 4)),hhcounts_date_new[:iriweek]);
    BinSize= ["1" 1 1; "2 to 4" 2 4; "5 to 10" 5 10; "11+" 11 10000000];
    names!(buyer_week_data, [:exposureid, :iriweek]);
    exp_data1 = hhcounts_date_new;
    exp_data1[:time1] = map(x->datetime2unix(DateTime(x, DateFormat("mm/dd/yyyy  HH:MM:SS"))), exp_data1[:iriweek]);
    exp_data1[:timestamp] = map(x->DateTime(x, DateFormat("mm/dd/yyyy  HH:MM:SS")), exp_data1[:iriweek]);
    ##Add additional information to purchase data
    pur_data1 = buyer_week_data;
    pur_data1[:trans_date] = rata2datetime(722694+7*pur_data1[:iriweek]);
    pur_data1[:time1] = datetime2unix(pur_data1[:trans_date]);
    purch = sort!(pur_data1, cols = [order(:exposureid), order(:iriweek), order(:trans_date)]);
    #Creating the first purchase and occasion
    purcdate = purch[:, [:exposureid, :trans_date]];
    purcdate = aggregate(purcdate, :exposureid, minimum);
    names!(purcdate, [:exposureid, :firstsale]);
    purchase = by(purch, [:exposureid], nrow);
    names!(purchase, [:exposureid, :occ]);
    purch1 = join(purch, purcdate, on=:exposureid, kind=:inner);
    #Reading the date values
    exp = sort!(exp_data1, cols=[order(:exposureid), order(:timestamp), order(:Exposures), order(:imp)]);
    #Creating the first exposure and last exposure before purchase
    exp_temp = join(exp, purch1, on=:exposureid, kind=:inner);
    exp_temp = exp_temp[(exp_temp[:timestamp] .<= exp_temp[:firstsale]), :];
    expdate_1 = exp[:, [:exposureid, :timestamp]];
    expdate_1 = aggregate(expdate_1, :exposureid, [minimum]);
    names!(expdate_1, [:exposureid, :firstexpo]);
    expdate_2 = aggregate(exp_temp[:, [:exposureid, :timestamp]], :exposureid, [maximum]);
    names!(expdate_2, [:exposureid, :latestexpo]);
    expdate = join(expdate_1, expdate_2, on=:exposureid, kind=:outer);
    exp1 = join(exp, expdate, on=:exposureid, kind=:inner);
    lastexpo = join(exp1, purcdate, on=:exposureid, kind=:inner);
    lastexpo[:diff] = lastexpo[:firstsale]-lastexpo[:timestamp];
    lastexpo1 = lastexpo[(map(Int, lastexpo[:diff]) .>= 0), :];
    expodate = expdate;
    expocnt = by(exp1, [:exposureid], exp1->sum(exp1[:imp]));
    names!(expocnt, [:exposureid, :Exposures]);
    bef_Purch = lastexpo1[(lastexpo1[:timestamp] .== lastexpo1[:latestexpo]), [:exposureid, :latestexpo]];
    bef_cnt = by(lastexpo1, [:exposureid], lastexpo1->sum(lastexpo1[:imp]));
    names!(bef_cnt, [:exposureid, :Exposures_To_1st_Buy]);
    #Merging first and last exposure, first purchase, purchase occasion information
    combined = join(expodate[:, [:exposureid, :firstexpo]], expocnt, on=:exposureid, kind=:inner);
    combined = join(combined, purcdate, on=:exposureid, kind=:inner);
    combined = join(combined, purchase, on=:exposureid, kind=:left);
    combined = join(combined, bef_Purch, on=:exposureid, kind=:left);
    combined = join(combined, bef_cnt, on=:exposureid, kind=:left);
    #Removing NAs from Exposures_To_1st_Buy
    combined[isna(combined[:Exposures_To_1st_Buy]), :Exposures_To_1st_Buy] = 0;
    #Calculate days/weeks difference between first purchase and last exposure to purchase
    combined[:day_gap] = 0;
    combined[!isna(combined[:latestexpo]), :day_gap] = map(Int, map(Int, (combined[!isna(combined[:latestexpo]), :firstsale]-combined[!isna(combined[:latestexpo]), :latestexpo])/86400000)+1);
    combined[:day_gap_in_weeks] = combined[:day_gap]/7;
    combined[:week_gap] = "Pre";
    combined[(combined[:day_gap_in_weeks] .> 0) & (combined[:day_gap_in_weeks] .< 2), :week_gap] = "1 Week (or less)";
    combined[(combined[:day_gap_in_weeks] .>= 2 ) & (combined[:day_gap_in_weeks] .< 3), :week_gap] = "2 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 3) & (combined[:day_gap_in_weeks] .< 4), :week_gap] = "3 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 4) & (combined[:day_gap_in_weeks] .< 5), :week_gap] = "4 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 5) & (combined[:day_gap_in_weeks] .< 6), :week_gap] = "5 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 6) & (combined[:day_gap_in_weeks] .< 7), :week_gap] = "6 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 7) & (combined[:day_gap_in_weeks] .< 8), :week_gap] = "7 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 8) & (combined[:day_gap_in_weeks] .< 9), :week_gap] = "8 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 9) & (combined[:day_gap_in_weeks] .< 10), :week_gap] = "9 Weeks";
    combined[(combined[:day_gap_in_weeks] .>= 10), :week_gap] = "Over 10 Weeks";
    combined = sort!(combined, cols=[order(:exposureid)]);
    combined = hcat(combined, collect(1:size(combined, 1)));
    combined = combined[:, [:x1, :exposureid, :firstexpo, :occ, :firstsale, :Exposures, :latestexpo, :Exposures_To_1st_Buy, :day_gap, :day_gap_in_weeks, :week_gap]];
    names!(combined, [:Obs, :exposureid, :First_Exposure, :Purchase_Occasions, :First_Purchase_Weekending, :Exposures, :Date_last_exposure_before_1st_buy, :Number_exposure_before_1st_buy, :Days_between_last_exposure_first_buy, :Weeks_between_last_exposure_first_buy, :Time]);

    exp_data2 = hhcounts_date;
    exp_data2 = exp_data2[exp_data2[:brk] .== "frequency_type_dym", :];
    freq_index = unique(exp_data2[:, [:lvl, :frq_index]]);
    freq_index = sort!(freq_index, cols=order(:frq_index));
    names!(freq_index, [:Exposures, :frq_index]);

    return pur_data1, exp_data1, exp_data_n1, combined, freq_index, expocnt # Only `combined`, `freq_index`, `expocnt` needed in other functions
end

end
