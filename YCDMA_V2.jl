
module YCDMA_V2
    
    using DataFrames,DataStructures, Base.Dates, XLib, BinDist, CLib, IRITypes, LG, IRIfunc,QC_log;
	
    function qc_pos_data(dfd::DataFrame,scored::DataFrame,brand_data::DataFrame,upc_data::DataFrame,hhcounts_date::DataFrame,buyer_week_data::DataFrame,imp_week::DataFrame,cfg::DataStructures.OrderedDict{Any,Any},flag::Int64)
        
            qc_check(dfd,   [:panid,:prd_1_net_pr_pre,:prd_2_net_pr_pre,:prd_3_net_pr_pre,:prd_4_net_pr_pre,:prd_5_net_pr_pre,:prd_6_net_pr_pre
                            ,:prd_7_net_pr_pre,:prd_8_net_pr_pre,:prd_9_net_pr_pre,:prd_10_net_pr_pre,:prd_0_net_pr_pos,:prd_1_net_pr_pos
                            ,:prd_2_net_pr_pos,:prd_3_net_pr_pos,:prd_4_net_pr_pos,:prd_5_net_pr_pos,:prd_6_net_pr_pos,:prd_7_net_pr_pos
                            ,:prd_8_net_pr_pos,:prd_9_net_pr_pos,:prd_10_net_pr_pos,:trps_pos_p1,:buyer_pos_p0,:buyer_pos_p1,:group
                            ,:buyer_pre_52w_p1,:buyer_pre_52w_p0], 
                                             [Int64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64
                            ,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int64,Int64,Int64,Any,Int64,Int64],"orig","CDMA")
            qc_check(scored,[:MODEL_DESC,:UDJ_AVG_EXPSD_HH_PRE,:UDJ_AVG_CNTRL_HH_PRE,:UDJ_AVG_EXPSD_HH_PST
                            ,:UDJ_AVG_CNTRL_HH_PST,:UDJ_DOD_EFFCT,:UDJ_DIFF_EFFCT,:ADJ_MEAN_EXPSD_GRP
                                                            ,:ADJ_MEAN_CNTRL_GRP,:ADJ_DOD_EFFCT,:DOL_DIFF],
                                                             [String,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64],"scored","CDMA")
            qc_check(brand_data, [:product_id, :group_name], [Int64,String],"brand_data","CDMA")
            qc_check(upc_data,[:experian_id,:period,:upc,:description,:net_price],[Int64,Int64,Int64,String,Float64],"upc_data","CDMA")
            qc_check(hhcounts_date,[:brk,:lvl,:panid,:dte,:impressions],[String,String,Int64,Int64,Int64],"hhcounts_date","CDMA")
            if flag == 1
                   qc_check(imp_week,[:iri_week,:exposure_date,:hhs,:impressions],[Int64,String,Int64,Int64],"imp_week","CDMA")
            end
    end
     
    function CDMA_dataprep(cfg::DataStructures.OrderedDict{Any,Any},scored::DataFrame, brand_data::DataFrame,upc_data::DataFrame,hhcounts_date::DataFrame,buyer_week_data::DataFrame,imp_week::DataFrame,descDump::DataFrame,src::String,flag::Int64);
               println("Running CDMA")
        hdr             = Array(strdf(rf(cfg[:files][:hdr]),header=false)[:x1] )
        for i in 1:length(hdr) hdr[i] = hdr[i]=="iri_link_id" ? "banner" : hdr[i] end
        for i in 1:length(hdr) hdr[i] = hdr[i]=="proscore" ? "model" : hdr[i] end
        pre_data_check(["orig","scored","brand_data","upc_data","hhcounts_date","buyer_week_data"],cfg,"CDMA");
        M               = MFile(hdr, cfg)
        c               = [:panid, :group,:prd_1_net_pr_pre,:prd_2_net_pr_pre,:prd_3_net_pr_pre,:prd_4_net_pr_pre,:prd_5_net_pr_pre
                          ,:prd_6_net_pr_pre,:prd_7_net_pr_pre,:prd_8_net_pr_pre,:prd_9_net_pr_pre ,:prd_10_net_pr_pre,:prd_0_net_pr_pos,:prd_1_net_pr_pos
                          ,:prd_2_net_pr_pos,:prd_3_net_pr_pos,:prd_4_net_pr_pos,:prd_5_net_pr_pos,:prd_6_net_pr_pos,:prd_7_net_pr_pos, :prd_8_net_pr_pos 
                          ,:prd_9_net_pr_pos,:prd_10_net_pr_pos,:buyer_pos_p1,:buyer_pos_p0,:buyer_pre_52w_p1,:buyer_pre_52w_p0,:trps_pos_p1]
        M2              = MFile(c,M)
        dfmx            = creadData([-1], M2, "",src)        
        dfd             = join(dfmx,descDump[:,[:panid]],on=:panid,kind=:inner);
        qc_pos_data(dfd,scored,brand_data,upc_data,hhcounts_date,buyer_week_data,imp_week,cfg,flag)
               return dfmx,dfd,scored,brand_data,upc_data,hhcounts_date,buyer_week_data,imp_week          
    end;
    
    function genbuyerclass(dfa::DataFrame, scored::DataFrame)
        dfo=DataFrame(reptype=[],id=[],desc=[],val=[],cnt=[])
               ######Exposed buyer class start ######
               lapsed_buyers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 0) & (dfa[:buyer_pre_52w_p1] .== 1),:])
               non_buyers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 0) & (dfa[:buyer_pre_52w_p1] .== 0),:])
               brand_buyers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 1),:])
               new_buyers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0),:])
               repeat_buyers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 1),:])
               categeory_switchers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0) & (dfa[:buyer_pre_52w_p0] .== 0),:])
               brand_switchers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0) & (dfa[:buyer_pre_52w_p0] .== 1),:])
               r=:buyer_exposed
               scored_fac_exp     = scored[(scored[:MODEL_DESC] .== "Total Campaign") & (scored[:dependent_variable] .== "pen"),:UDJ_AVG_EXPSD_HH_PST][1]
               scored_factor_exp  = scored_fac_exp / (brand_buyers/ (lapsed_buyers+non_buyers+brand_buyers))
               push!(dfo, [r,-1,"new buyers",  new_buyers/(lapsed_buyers+non_buyers+brand_buyers)*scored_factor_exp,                Int64(round(new_buyers * scored_factor_exp))] )
               push!(dfo, [r,-1,"repeat buyers", repeat_buyers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_exp,Int64(round(repeat_buyers *scored_factor_exp))] )
               push!(dfo, [r,-1,"category switcher", categeory_switchers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_exp,Int64(round(categeory_switchers * scored_factor_exp))])
               push!(dfo, [r,-1,"brand switcher", brand_switchers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_exp,Int64(round(brand_switchers * scored_factor_exp))])
               push!(dfo, [r,-1,"brand buyers", brand_buyers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_exp ,Int64(round(brand_buyers * scored_factor_exp))])
               push!(dfo, [r,-1,"non buyers", 1 - ( (1- (non_buyers/ (lapsed_buyers+non_buyers+brand_buyers)))*scored_factor_exp), Int64(round((lapsed_buyers+non_buyers+brand_buyers)-((lapsed_buyers+brand_buyers)*scored_factor_exp)))] )
               push!(dfo, [r,-1,"lapsed buyers", lapsed_buyers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_exp, Int64(round(lapsed_buyers * scored_factor_exp))] )
               ###### UNExposed buyer class start ######
               lapsed_buyers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 0) & (dfa[:buyer_pre_52w_p1] .== 1),:])
               non_buyers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 0) & (dfa[:buyer_pre_52w_p1] .== 0),:])
               brand_buyers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 1),:])
               new_buyers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0),:])
               repeat_buyers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 1),:])
               categeory_switchers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0) & (dfa[:buyer_pre_52w_p0] .== 0),:])
               brand_switchers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0) & (dfa[:buyer_pre_52w_p0] .== 1),:])
               r=:buyer_unexposed
               scored_fac_cntrl     = scored[(scored[:MODEL_DESC] .== "Total Campaign") & (scored[:dependent_variable] .== "pen"),:UDJ_AVG_CNTRL_HH_PST][1]
               scored_factor_cntrl  = scored_fac_cntrl / (brand_buyers/ (lapsed_buyers+non_buyers+brand_buyers))
               push!(dfo, [r,-1,"new buyers",  new_buyers/(lapsed_buyers+non_buyers+brand_buyers)*scored_factor_cntrl,  Int64(round(new_buyers * scored_factor_cntrl))] )
               push!(dfo, [r,-1,"repeat buyers", repeat_buyers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_cntrl , Int64(round(repeat_buyers *scored_factor_cntrl) )])
               push!(dfo, [r,-1,"category switcher", categeory_switchers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_cntrl, Int64(round(categeory_switchers * scored_factor_cntrl))])
              push!(dfo, [r,-1,"brand switcher", brand_switchers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_cntrl, Int64(round(brand_switchers * scored_factor_cntrl))] )
               push!(dfo, [r,-1,"brand buyers", brand_buyers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_cntrl , Int64(round(brand_buyers * scored_factor_cntrl))])
               push!(dfo, [r,-1,"non buyers", 1 - ((1- (non_buyers/ (lapsed_buyers+non_buyers+brand_buyers)))*scored_factor_cntrl),  Int64(round((lapsed_buyers+non_buyers+brand_buyers)-((lapsed_buyers+brand_buyers)*scored_factor_cntrl)))] )
               push!(dfo, [r,-1,"lapsed buyers", lapsed_buyers/ (lapsed_buyers+non_buyers+brand_buyers)*scored_factor_cntrl ,  Int64(round(lapsed_buyers * scored_factor_cntrl)) ])              
                              dfo[:desc] = map((x) -> uppercase(x),dfo[:desc])
                              dfo_Exp   = DataFrame(buyer_type=dfo[dfo[:reptype] .== :buyer_exposed,:desc], buyer_percent =dfo[dfo[:reptype] .== :buyer_exposed,:val],CNT = dfo[dfo[:reptype] .== :buyer_exposed,:cnt])
               dfo_UnExp = DataFrame(buyer_type=dfo[dfo[:reptype] .== :buyer_unexposed,:desc], buyer_percent =dfo[dfo[:reptype] .== :buyer_unexposed,:val],CNT = dfo[dfo[:reptype] .== :buyer_unexposed,:cnt])
                              return dfo_Exp,dfo_UnExp
    end
    
    function gentrialRepeat(dfa::DataFrame,scored::DataFrame)
        dfo=DataFrame(reptype=[],id=[],desc=[],val=[],cnt=[])
               trial_buyers_ex = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0),:])
               repeat_triers_ex = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0) & (dfa[:trps_pos_p1] .> 1),:])
               categeory_buyers_ex = nrow(dfa[(dfa[:group] .== 1),:])
               trial_buyers_unex = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0),:])
               repeat_triers_unex = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 1) & (dfa[:buyer_pre_52w_p1] .== 0) & (dfa[:trps_pos_p1] .> 1),:])
               categeory_buyers_unex = nrow(dfa[(dfa[:group] .== 0),:])
               r=:trial_repeat
               lapsed_buyers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 0) & (dfa[:buyer_pre_52w_p1] .== 1),:])
               non_buyers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 0) & (dfa[:buyer_pre_52w_p1] .== 0),:])
               brand_buyers = nrow(dfa[(dfa[:group] .== 1) & (dfa[:buyer_pos_p1] .== 1),:])
               scored_fac_exp     = scored[(scored[:MODEL_DESC] .== "Total Campaign") & (scored[:dependent_variable] .== "pen"),:UDJ_AVG_EXPSD_HH_PST][1]
               scored_factor_exp  = scored_fac_exp / (brand_buyers/ (lapsed_buyers+non_buyers+brand_buyers))
               lapsed_buyers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 0) & (dfa[:buyer_pre_52w_p1] .== 1),:])
               non_buyers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 0) & (dfa[:buyer_pre_52w_p1] .== 0),:])
               brand_buyers = nrow(dfa[(dfa[:group] .== 0) & (dfa[:buyer_pos_p1] .== 1),:])
               scored_fac_cntrl     = scored[(scored[:MODEL_DESC] .== "Total Campaign") & (scored[:dependent_variable] .== "pen"),:UDJ_AVG_CNTRL_HH_PST][1]
               scored_factor_cntrl  = scored_fac_cntrl / (brand_buyers/ (lapsed_buyers+non_buyers+brand_buyers))
               push!(dfo, [r,1,"triers_percent", trial_buyers_ex/categeory_buyers_ex*scored_factor_exp , Int64(round(trial_buyers_ex * scored_factor_exp))] )
               push!(dfo, [r,1,"repeaters_percent", repeat_triers_ex/trial_buyers_ex ,Int64(round(repeat_triers_ex * scored_factor_exp))] )
               push!(dfo, [r,1,"cat_percent", categeory_buyers_ex / categeory_buyers_ex ,Int64(round(categeory_buyers_ex * scored_factor_exp))] )
                              push!(dfo, [r,0,"triers_percent", trial_buyers_unex/categeory_buyers_unex*scored_factor_cntrl,Int64(round(trial_buyers_unex * scored_factor_cntrl)) ] )
               push!(dfo, [r,0,"repeaters_percent", repeat_triers_unex/trial_buyers_unex,Int64(round(repeat_triers_unex * scored_factor_cntrl))] )  
               push!(dfo, [r,0,"cat_percent", categeory_buyers_unex / categeory_buyers_unex ,Int64(round(categeory_buyers_unex * scored_factor_cntrl))] )
                              dfo_f =DataFrame(grouptype=String[],triers_percent=Float64[],repeaters_percent=Float64[],cat_percent=Float64[],triers_cnt=Int64[],repeaters_cnt=Int64[],cat_cnt =Int64[])
                              push!(dfo_f, ["Exposed",
                                            dfo[(dfo[:id] .==1) & (dfo[:desc] .=="triers_percent"),:val][1],
                                            dfo[(dfo[:id] .==1) & (dfo[:desc] .=="repeaters_percent"),:val][1],
                                                                             dfo[(dfo[:id] .==1) & (dfo[:desc] .=="cat_percent"),:val][1],
                                                                             dfo[(dfo[:id] .==1) & (dfo[:desc] .=="triers_percent"),:cnt][1],
                                                                             dfo[(dfo[:id] .==1) & (dfo[:desc] .=="repeaters_percent"),:cnt][1],
                                                                             dfo[(dfo[:id] .==1) & (dfo[:desc] .=="cat_percent"),:cnt][1]])
        push!(dfo_f, ["Unexposed",
                                            dfo[(dfo[:id] .==0) & (dfo[:desc] .=="triers_percent"),:val][1],
                                            dfo[(dfo[:id] .==0) & (dfo[:desc] .=="repeaters_percent"),:val][1],
                                                                             dfo[(dfo[:id] .==0) & (dfo[:desc] .=="cat_percent"),:val][1],
                                                                             dfo[(dfo[:id] .==0) & (dfo[:desc] .=="triers_percent"),:cnt][1],
                                                                             dfo[(dfo[:id] .==0) & (dfo[:desc] .=="repeaters_percent"),:cnt][1],
                                                                             dfo[(dfo[:id] .==0) & (dfo[:desc] .=="cat_percent"),:cnt][1]])
                              return dfo_f
    end
    
    function fair_share(dfa::DataFrame,brand_data::DataFrame);
        println("Export Fair Share Index")
               ######  Fair share Index Start ######     
               dfo=DataFrame(reptype=[],id=[],desc=[],val=[])
               post_sales_p1 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_1_net_pr_pos])
               post_sales_p2 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_2_net_pr_pos])
               post_sales_p3 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_3_net_pr_pos])
               post_sales_p4 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_4_net_pr_pos])
               post_sales_p5 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_5_net_pr_pos])
               post_sales_p6 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_6_net_pr_pos])
               post_sales_p7 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_7_net_pr_pos])
               post_sales_p8 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_8_net_pr_pos])
               post_sales_p9 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_9_net_pr_pos])
               post_sales_p10 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_10_net_pr_pos])
               pre_sales_p1 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_1_net_pr_pre])
               pre_sales_p2 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_2_net_pr_pre])
               pre_sales_p3 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_3_net_pr_pre])
               pre_sales_p4 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_4_net_pr_pre])
               pre_sales_p5 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_5_net_pr_pre])
               pre_sales_p6 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_6_net_pr_pre])
               pre_sales_p7 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_7_net_pr_pre])
               pre_sales_p8 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_8_net_pr_pre])
               pre_sales_p9 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_9_net_pr_pre])
               pre_sales_p10 = sum(dfa[(dfa[:group] .== 1)  & (dfa[:buyer_pos_p1] .== 1),:prd_10_net_pr_pre])
               r=:fair_share 
               push!(dfo, [r,1,"pos_sales", post_sales_p1 ] )
               push!(dfo, [r,1,"pre_sales", pre_sales_p1 ] )
               push!(dfo, [r,2,"pos_sales", post_sales_p2 ] )
               push!(dfo, [r,2,"pre_sales", pre_sales_p2 ] )
               push!(dfo, [r,3,"pos_sales", post_sales_p3 ] )
               push!(dfo, [r,3,"pre_sales", pre_sales_p3 ] )
               push!(dfo, [r,4,"pos_sales", post_sales_p4 ] )
               push!(dfo, [r,4,"pre_sales", pre_sales_p4 ] )
               push!(dfo, [r,5,"pos_sales", post_sales_p5 ] )
               push!(dfo, [r,5,"pre_sales", pre_sales_p5 ] )
               push!(dfo, [r,6,"pos_sales", post_sales_p6 ] )
               push!(dfo, [r,6,"pre_sales", pre_sales_p6 ] )
               push!(dfo, [r,7,"pos_sales", post_sales_p7 ] )
               push!(dfo, [r,7,"pre_sales", pre_sales_p7 ] )
               push!(dfo, [r,8,"pos_sales", post_sales_p8 ] )
               push!(dfo, [r,8,"pre_sales", pre_sales_p8 ] )
               push!(dfo, [r,9,"pos_sales", post_sales_p9 ] )
               push!(dfo, [r,9,"pre_sales", pre_sales_p9 ] )
               push!(dfo, [r,10,"pos_sales", post_sales_p10 ] )
               push!(dfo, [r,10,"pre_sales", pre_sales_p10 ] )
                              for i in 1:10
                                             tpos=sum(dfo[(dfo[:reptype].==r)&(dfo[:desc].=="pos_sales"),:val])
                                             v=dfo[(dfo[:id].==i)&(dfo[:reptype].==r)&(dfo[:desc].=="pos_sales"),:val][1]
                                             totPos=v/tpos
                                             println(i," ~~ ",totPos)
                                             push!(dfo, [r,i,"pos_sales_percent", totPos ] )
                                             tpre=sum(dfo[(dfo[:reptype].==r)&(dfo[:desc].=="pre_sales"),:val])
                                             v=dfo[(dfo[:id].==i)&(dfo[:reptype].==r)&(dfo[:desc].=="pre_sales"),:val][1]
                                             totPre=v/tpre
                                             println(i," ~~ ",totPre)
                                             push!(dfo, [r,i,"pre_sales_percent", totPre ] )
                                             push!(dfo, [r,i,"sales_percent_change",   totPos-totPre    ] )
                                             if i > 1
                                                            trgt = dfo[(dfo[:reptype].==r)&(dfo[:id].==1)&(dfo[:desc].=="sales_percent_change"),:val][1]
                                                            v=dfo[(dfo[:id].==i)&(dfo[:reptype].==r)&(dfo[:desc].=="sales_percent_change"),:val][1]
                                                            ppbrand=-(v/trgt)
                                                            push!(dfo, [r,i,"pp_of_feature_brand", ppbrand ] )
                                                            presales = dfo[(dfo[:id].==i)&(dfo[:reptype].==r)&(dfo[:desc].=="pre_sales"),:val][1]
                                                            sumPreSales = sum(dfo[(dfo[:reptype].==r)&(dfo[:id].!=1)&(dfo[:desc].=="pre_sales"),:val])
                                                            push!(dfo, [r,i,"fair_share_index", (ppbrand/(  presales/sumPreSales  ))*100 ] )
                                             end
                              end
               
               agg_fair_share_index = DataFrame(product_grp_id=Int32[], pos_sales=Float64[], pos_sales_percent=Float64[], pre_sales=Float64[], pre_sales_percent=Float64[], sales_percent_change=Float64[], pp_of_feature_brand=Float64[], fair_share_index=Float64[])
        push!(agg_fair_share_index,[1, mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="pos_sales")&(dfo[:id].==1),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="pos_sales_percent")&(dfo[:id].==1),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="pre_sales")&(dfo[:id].==1),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="pre_sales_percent")&(dfo[:id].==1),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="sales_percent_change")&(dfo[:id].==1),:val]),0,0])
                   for i in (2:10)
                                  push!(agg_fair_share_index,[i,mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="pos_sales")&(dfo[:id].==i),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="pos_sales_percent")&(dfo[:id].==i),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="pre_sales")&(dfo[:id].==i),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="pre_sales_percent")&(dfo[:id].==i),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="sales_percent_change")&(dfo[:id].==i),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="pp_of_feature_brand")&(dfo[:id].==i),:val]),mean(dfo[(dfo[:reptype].==:fair_share)&(dfo[:desc].=="fair_share_index")&(dfo[:id].==i),:val])])
                   end
               agg_fair_share_index = agg_fair_share_index[!((agg_fair_share_index[:pre_sales] .== 0) | (agg_fair_share_index[:pos_sales] .== 0)) ,:]
        rename!(brand_data, :product_id, :product_grp_id)
        rename!(brand_data, :group_name, :product)
        agg_fair_share_index = join(agg_fair_share_index, brand_data , on = :product_grp_id)               
        agg_fair_share_index = agg_fair_share_index[:,[:product_grp_id, :product, :pos_sales, :pos_sales_percent, :pre_sales, :pre_sales_percent, :sales_percent_change, :pp_of_feature_brand, :fair_share_index]]            
               return agg_fair_share_index
    end
    
               
               
               function Share_of_requirements(dfa::DataFrame)
        dfo=DataFrame(reptype=[],id=[],desc=[],val=[])
                              post_sales_p1_ex = sum(dfa[(dfa[:group] .== 1),:prd_1_net_pr_pos])
        post_sales_p0_ex = sum(dfa[(dfa[:group] .== 1),:prd_0_net_pr_pos])
        post_sales_p1_unex = sum(dfa[(dfa[:group] .== 0),:prd_1_net_pr_pos])
        post_sales_p0_unex = sum(dfa[(dfa[:group] .== 0),:prd_0_net_pr_pos])
        r=:share_of_requirement
        push!(dfo, [r,0,"product_group_share", post_sales_p1_unex/post_sales_p0_unex ] )
        push!(dfo, [r,1,"product_group_share", post_sales_p1_ex/post_sales_p0_ex ] )
                              agg_share_of_requirements = DataFrame(exposed_flag=Int32[], product_group_share=Float64[])
        push!(agg_share_of_requirements,[0, mean(dfo[(dfo[:reptype].==:share_of_requirement)&(dfo[:desc].=="product_group_share")&(dfo[:id].==0),:val])])
        push!(agg_share_of_requirements,[1, mean(dfo[(dfo[:reptype].==:share_of_requirement)&(dfo[:desc].=="product_group_share")&(dfo[:id].==1),:val])])
                              return agg_share_of_requirements;
    end

    function upc_growth(dfa::DataFrame,upc_data::DataFrame);   
                   upc_data =deepcopy(upc_data);
        dfupc=DataFrame(DESCRIPTION=[],UPC=[],pos_sales=[],pre_sales=[],pos_sales_share=[],pre_sales_share=[],GROWTH_SALES=[],growth_contribution=[]);
        rename!(upc_data,:experian_id,:panid);
        j = join(upc_data, dfa[dfa[:group] .== 1,[:panid,:group]] , on = :panid);
        upc_1 = by(j, [:period,:upc,:description], d -> DataFrame(sales = sum(d[:net_price])));
        upc_1_1 = upc_1[upc_1[:period] .==1, [:upc, :description, :sales]];
        upc_1_2 = upc_1[upc_1[:period] .==2, [:upc, :description, :sales]];
        upc_1_3 = join(upc_1_1, upc_1_2, on = [:upc,:description], kind = :outer);
        rename!(upc_1_3, :sales, :pre_sales);
        rename!(upc_1_3, :sales_1, :pos_sales);
        upc_1_3[isna(upc_1_3[:pos_sales]),:pos_sales] = 0;
        upc_1_3[isna(upc_1_3[:pre_sales]),:pre_sales] = 0;
        upc_1_3[:pre_sales_share] = upc_1_3[:pre_sales] ./ sum(upc_1_3[:pre_sales]);
        upc_1_3[:pos_sales_share] = upc_1_3[:pos_sales] ./ sum(upc_1_3[:pos_sales]);
        upc_1_3[:growth_contribution] = (upc_1_3[:pos_sales] .- upc_1_3[:pre_sales]) / abs((sum(upc_1_3[:pos_sales]) .- sum(upc_1_3[:pre_sales]))) *100;
        sort!(upc_1_3,cols=[:growth_contribution],rev=true);
               function findchar(a,charac)
                              m=0
                              for i in (1:length(a))
                                             if a[i]==charac
                                                            m=i
                                                            break
                                             end
                              end
                              return m
               end
        upc_1_3[:DESCRIPTION_WO_SIZE] = map(x-> reverse(SubString(SubString(reverse(SubString(x,1,findlast(x,'-')-2)),findchar(reverse(SubString(x,1,findlast(x,'-')-2)),' ')+1,1000),findchar(SubString(reverse(SubString(x,1,findlast(x,'-')-2)),findchar(reverse(SubString(x,1,findlast(x,'-')-2)),' ')+1,1000),' ')+1,1000)), upc_1_3[:description]);
        upc_1_3[:TYPE] = map(x-> reverse(SubString(reverse(SubString(x,1,findlast(x,'-')-2)),1,2)), upc_1_3[:description]);
        upc_1_3[:SIZE] = map(x-> parse(Float64,reverse(SubString(SubString(reverse(SubString(x,1,findlast(x,'-')-2)),4,1000),1,findchar(SubString(reverse(SubString(x,1,findlast(x,'-')-2)),4,1000),' ')-1))),upc_1_3[:description]);
        upc_1_4 = upc_1_3[upc_1_3[:pre_sales] .==0,:];
        upc_1_5 = upc_1_3[upc_1_3[:pre_sales] .!=0,:];
    
        upc_3 = unique(upc_1_5[:,[:SIZE,:TYPE,:DESCRIPTION_WO_SIZE]]);
        sort!(upc_3,cols=[:DESCRIPTION_WO_SIZE,:TYPE,:SIZE]);
        upc_3[:LAG_SIZE] = (upc_3[:SIZE] .- vcat(0,upc_3[:SIZE][1:length(upc_3[:SIZE])-1]));
        upc_3[:PERCENT_LAG] = (upc_3[:LAG_SIZE]) ./ upc_3[:SIZE] *100;
        upc_3[:ROW_NN] = 1:nrow(upc_3);
    
        upc_4=DataFrame();
                   for i in groupby(upc_3,[:DESCRIPTION_WO_SIZE,:TYPE])
                                  i[:ROW_NN] =1:nrow(i)
                                  upc_4=vcat(i,upc_4)
                   end
        upc_4[:LAG_SIZE] = map((x,y,z)-> ifelse(x .== 1,y,z),upc_4[:ROW_NN],upc_4[:SIZE],upc_4[:LAG_SIZE]);
        upc_4[:PERCENT_LAG] = map((x,y)-> ifelse(x .== 1,100,y),upc_4[:ROW_NN],upc_4[:PERCENT_LAG]);
        sort!(upc_4,cols=[:DESCRIPTION_WO_SIZE,:TYPE,:SIZE]);
        upc_4[:NEW_SIZE] = map((x,y)-> ifelse(x > 30,y,0),upc_4[:PERCENT_LAG],upc_4[:SIZE]);
                   for i in 2:size(upc_4,1)
                                  if upc_4[i,:NEW_SIZE]  .== 0
                                             upc_4[i,:NEW_SIZE] = upc_4[i-1,:NEW_SIZE]
                                             end
                   end
    
        sort!(upc_4,cols=[:DESCRIPTION_WO_SIZE, :TYPE, :SIZE]);
        upc_5 = deepcopy(upc_4);
        upc_6 = deepcopy(upc_5);
        upc_6[:ROW_NN] = map(x-> x-1,upc_6[:ROW_NN]);
    
        upc_7 = join(upc_5, upc_6, on =[:ROW_NN,:TYPE,:DESCRIPTION_WO_SIZE],kind=:left);
        upc_7 = upc_7[:,[:SIZE,:TYPE,:DESCRIPTION_WO_SIZE,:LAG_SIZE,:PERCENT_LAG_1,:ROW_NN,:NEW_SIZE]];
        rename!(upc_7,:PERCENT_LAG_1,:PERCENT_LAG);
        sort!(upc_7, cols = [:SIZE],rev=true);
        sort!(upc_7, cols = [:TYPE,:DESCRIPTION_WO_SIZE]);
        upc_7[isna(upc_7[:PERCENT_LAG]),:PERCENT_LAG]=100;
        upc_7[:NEW_SIZE] = map((x,y)-> ifelse(x > 30,y,0),upc_7[:PERCENT_LAG],upc_7[:SIZE]);
                   for i in 2:size(upc_7,1)
                                  if upc_7[i,:NEW_SIZE] .== 0
                                             upc_7[i,:NEW_SIZE] = upc_7[i-1,:NEW_SIZE]
                                  end
                   end
        sort!(upc_7, cols = [:TYPE,:DESCRIPTION_WO_SIZE,:SIZE]);
    
               upc_8 = join(upc_4, upc_7,on =[:TYPE,:DESCRIPTION_WO_SIZE,:ROW_NN]);
               upc_8 = upc_8[:,[:DESCRIPTION_WO_SIZE,:TYPE, :SIZE, :ROW_NN, :NEW_SIZE , :NEW_SIZE_1]];
               rename!(upc_8, :NEW_SIZE, :LOW_SIZE);
               rename!(upc_8, :NEW_SIZE_1, :HIGH_SIZE);
               upc_dummy = DataFrame(TYPE=unique(upc_3[:TYPE]));
               upc_8 = join(upc_8,upc_dummy,on=:TYPE);
               upc_8[:SIZE_LEVEL] = map((x,y,z) -> ifelse(x==y, string(y ," " ,z), string("(",x," ",z," - ",y," ",z,")")),upc_8[:LOW_SIZE],upc_8[:HIGH_SIZE],upc_8[:TYPE]);
               upc_9 = join(upc_1_5,upc_8,on =[:SIZE,:TYPE,:DESCRIPTION_WO_SIZE]);
               upc_9 = by(upc_9,[:DESCRIPTION_WO_SIZE,:SIZE_LEVEL], df -> DataFrame(pre_sales=sum(df[:pre_sales]),pos_sales=sum(df[:pos_sales]), AGG=size(df,1), UPC=minimum(df[:upc])));
               upc_9[:GROWTH_SALES] = upc_9[:pos_sales] .- upc_9[:pre_sales];
        
                              if nrow(upc_1_4) >0
                  upc_10 = deepcopy(upc_1_4);
                  upc_10[:SIZE_LEVEL] = map((x,y) -> string(x," ",y),upc_10[:SIZE],upc_10[:TYPE]);
                  upc_10 = by(upc_10,[:DESCRIPTION_WO_SIZE,:SIZE_LEVEL], df -> DataFrame(pre_sales=sum(df[:pre_sales]),pos_sales=sum(df[:pos_sales]), AGG=size(df,1), UPC=minimum(df[:upc])));
                  upc_10[:GROWTH_SALES] = upc_10[:pos_sales] .- upc_10[:pre_sales];
                  upc_11 = vcat(upc_9,upc_10)
                              else 
                                 upc_11 = upc_9
                              end
               upc_11[:DESCRIPTION] = map((x,y) -> string(x," - ",y), upc_11[:DESCRIPTION_WO_SIZE],upc_11[:SIZE_LEVEL])
               upc_11[:pre_sales_share] = upc_11[:pre_sales] ./ sum(upc_11[:pre_sales]);
               upc_11[:pos_sales_share] = upc_11[:pos_sales] ./ sum(upc_11[:pos_sales]);
    
               upc_12 = upc_11[upc_11[:GROWTH_SALES].>0,:];
               upc_12[:growth_contribution] = (upc_12[:pos_sales] .- upc_12[:pre_sales]) / abs((sum(upc_12[:pos_sales]) .- sum(upc_12[:pre_sales])));
               sort!(upc_12,cols = [:growth_contribution],rev=true);
               upc_12[:UPC] = map((x,y) -> ifelse(x .== 1, string(y) ,string("Aggregation of ",x," UPCS")),upc_12[:AGG], upc_12[:UPC]);
               upc_final = upc_12[:,[:DESCRIPTION,:UPC,:pos_sales,:pre_sales,:pos_sales_share,:pre_sales_share,:GROWTH_SALES,:growth_contribution]];
               
               dfupc = vcat(dfupc, upc_final)
   
               
    
               rename!(dfupc,:DESCRIPTION,:description);
               rename!(dfupc,:UPC,:upc10);
               rename!(dfupc,:pos_sales,:sales_upc_post);
               rename!(dfupc,:pre_sales,:sales_upc_pre);
               rename!(dfupc,:pos_sales_share,:percentage_sales_upc_post);
               rename!(dfupc,:pre_sales_share,:percentage_sales_upc_pre);
               rename!(dfupc,:GROWTH_SALES,:growth_sales);
               dfupc = dfupc[:,[:description,:upc10,:sales_upc_pre,:sales_upc_post,:percentage_sales_upc_pre,:percentage_sales_upc_post,:growth_contribution]];
               upc_growth = deepcopy(dfupc);
               agg_upc_growth = DataFrame(description=[],upc10=[],sales_upc_pre=[],sales_upc_post=[],percentage_sales_upc_pre=[],percentage_sales_upc_post=[],growth_contribution=[]);
                              for i in unique(upc_growth[:description])
                                             push!(agg_upc_growth,[i,
                                             upc_growth[upc_growth[:description] .== i ,:upc10][1],
                                             mean(upc_growth[upc_growth[:description] .== i ,:sales_upc_pre]),
                                             mean(upc_growth[upc_growth[:description] .== i ,:sales_upc_post]),
                                             mean(upc_growth[upc_growth[:description] .== i ,:percentage_sales_upc_pre]),
                                             mean(upc_growth[upc_growth[:description] .== i ,:percentage_sales_upc_post]),
                                             mean(upc_growth[upc_growth[:description] .== i ,:growth_contribution])])
                              end
               sort!(agg_upc_growth,cols=:growth_contribution,rev=true)
               return agg_upc_growth
    end
    
    function genFreq_dataprep(hhcounts_date::DataFrame,buyer_week_data::DataFrame)
               outD=Dict() 
               buyer_week_data = deepcopy(buyer_week_data);
               exp_data_n1 = hhcounts_date[:,[:panid,:lvl,:impressions]];
               pur_data_id = DataFrame(panid = deepcopy(unique(buyer_week_data[:experian_id])))
               hhcounts_date_new = join(hhcounts_date,pur_data_id,on=:panid ,kind=:inner);
               hhcounts_date_new= hhcounts_date_new[ : ,[:panid, :lvl, :dte, :impressions]]
               names!(hhcounts_date_new,[:exposureid,:Exposures, :iriweek, :imp])
               hhcounts_date_new[:iriweek] = map(x -> string(SubString(string(x),5,6),'/',SubString(string(x),7,8),'/',SubString(string(x),1,4)),hhcounts_date_new[:iriweek])               
               BinSize= ["1" 1 1; "2 to 4" 2 4; "5 to 10" 5 10; "11+" 11 10000000] 
               names!(buyer_week_data,[:exposureid,:iriweek])
               exp_data1=hhcounts_date_new
               exp_data1[ :time1]=  map(x-> datetime2unix(DateTime(x, DateFormat("mm/dd/yyyy  HH:MM:SS"))), exp_data1[ :iriweek])
               exp_data1[ :timestamp]= map(x -> DateTime(x, DateFormat("mm/dd/yyyy  HH:MM:SS")),exp_data1[ :iriweek])
               ##Add additional information to purchase data
               pur_data1=buyer_week_data
               pur_data1[ :trans_date]=  rata2datetime(722694 + 7 * pur_data1[ :iriweek])
               pur_data1[ :time1]=  datetime2unix(pur_data1[ :trans_date])
               purch=sort!(pur_data1, cols = [order(:exposureid), order(:iriweek), order(:trans_date)])
               #Creating the first purchase and occasion
               purcdate=purch[:,[:exposureid, :trans_date ]]
               purcdate=aggregate(purcdate, :exposureid,  minimum)
               names!(purcdate, [:exposureid, :firstsale])
               purchase=by(purch, [:exposureid ],nrow)
               names!(purchase, [:exposureid, :occ])
               purch1=join(purch, purcdate, on = :exposureid, kind = :inner)
               #Reading the date values 
               exp=sort!(exp_data1, cols = [order(:exposureid), order(:timestamp), order(:Exposures), order(:imp)])
               #Creating the first exposure and last exposure before purchase
               exp_temp=join(exp, purch1, on = :exposureid, kind = :inner)
               exp_temp=exp_temp[(exp_temp[:timestamp].<=  exp_temp[:firstsale]),:]
               expdate_1=exp[:,[:exposureid, :timestamp ]]
               expdate_1=aggregate(expdate_1, :exposureid,  [minimum])
               names!(expdate_1, [:exposureid, :firstexpo])
               expdate_2=aggregate(exp_temp[:,[:exposureid,:timestamp]], :exposureid,  [maximum])
               names!(expdate_2, [:exposureid, :latestexpo])
               expdate=join(expdate_1, expdate_2, on = :exposureid, kind = :outer)
               exp1=join(exp, expdate, on = :exposureid, kind = :inner)
               lastexpo=join(exp1, purcdate, on = :exposureid, kind = :inner)
               lastexpo[:diff]=lastexpo[:firstsale]-lastexpo[:timestamp]
               lastexpo1=lastexpo[(map(Int,lastexpo[:diff]).>=  0),:]
               expodate=expdate
               expocnt=by(exp1, [:exposureid], exp1->sum(exp1[:imp]))
               names!(expocnt, [:exposureid, :Exposures])
               bef_Purch=lastexpo1[(lastexpo1[:timestamp] .== lastexpo1[:latestexpo] ) , [:exposureid,:latestexpo]] 
               bef_cnt=by(lastexpo1, [:exposureid], lastexpo1->sum(lastexpo1[:imp]))
               names!(bef_cnt, [:exposureid, :Exposures_To_1st_Buy])
               #Merging first and last exposure, first purchase, purchase occasion information
               combined=join(expodate[:,[:exposureid,:firstexpo]],expocnt, on = :exposureid, kind = :inner)
               combined=join(combined,purcdate, on = :exposureid, kind = :inner)
               combined=join(combined,purchase, on = :exposureid, kind = :left)
               combined=join(combined,bef_Purch, on = :exposureid, kind = :left)
               combined=join(combined,bef_cnt, on = :exposureid, kind = :left)
               #Removing NAs from Exposures_To_1st_Buy
               combined[isna(combined[:Exposures_To_1st_Buy]) ,  :Exposures_To_1st_Buy]=0
               #Calculate days/weeks difference between first purchase and last exposure to purchase
               combined[:day_gap]=0
               combined[!isna(combined[:latestexpo]) ,  :day_gap] =map(Int,map(Int,(combined[!isna(combined[:latestexpo]) , :firstsale]-combined[!isna(combined[:latestexpo]) , :latestexpo])/86400000)+1)
               combined[:day_gap_in_weeks]=combined[:day_gap]/7
               combined[:week_gap]="Pre"
               combined[(combined[:day_gap_in_weeks] .> 0) & (combined[:day_gap_in_weeks]  .< 2)  , :week_gap] = "1 Week (or less)"
               combined[(combined[:day_gap_in_weeks] .>=2 ) & (combined[:day_gap_in_weeks]  .< 3)  , :week_gap] = "2 Weeks"
               combined[(combined[:day_gap_in_weeks] .>=3) & (combined[:day_gap_in_weeks]  .< 4)  , :week_gap] = "3 Weeks"
               combined[(combined[:day_gap_in_weeks] .>=4) & (combined[:day_gap_in_weeks]  .< 5)  , :week_gap] = "4 Weeks"
               combined[(combined[:day_gap_in_weeks] .>=5) & (combined[:day_gap_in_weeks]  .< 6)  , :week_gap] = "5 Weeks"
               combined[(combined[:day_gap_in_weeks] .>=6) & (combined[:day_gap_in_weeks]  .< 7)  , :week_gap] = "6 Weeks"
               combined[(combined[:day_gap_in_weeks] .>=7) & (combined[:day_gap_in_weeks]  .< 8)  , :week_gap] = "7 Weeks"
               combined[(combined[:day_gap_in_weeks] .>=8) & (combined[:day_gap_in_weeks]  .< 9)  , :week_gap] = "8 Weeks"
               combined[(combined[:day_gap_in_weeks] .>=9) & (combined[:day_gap_in_weeks]  .< 10)  , :week_gap] = "9 Weeks"
               combined[(combined[:day_gap_in_weeks] .>=10) , :week_gap] = "Over 10 Weeks"    
               combined=sort!(combined, cols = [order(:exposureid)])
               combined=hcat(combined,collect(1:size(combined,1)))
              combined=combined[:,[:x1, :exposureid, :firstexpo, :occ, :firstsale, :Exposures, :latestexpo, :Exposures_To_1st_Buy, :day_gap, :day_gap_in_weeks, :week_gap]]
               names!(combined, [:Obs, :exposureid, :First_Exposure, :Purchase_Occasions, :First_Purchase_Weekending, :Exposures, :Date_last_exposure_before_1st_buy, :Number_exposure_before_1st_buy, :Days_between_last_exposure_first_buy, :Weeks_between_last_exposure_first_buy, :Time])
               
               exp_data2 = hhcounts_date;
                              exp_data2 = exp_data2[exp_data2[:brk] .== "frequency_type_dym", :]
                    freq_index=unique(exp_data2[:,[:lvl,:frq_index]])
                              freq_index=sort!(freq_index, cols = order(:frq_index))
                              names!(freq_index,[:Exposures,:frq_index])
    
               return pur_data1, exp_data1, exp_data_n1,combined, freq_index ,expocnt
    end
    
    function freq_HH_Cum1stpur(combined::DataFrame, freq_index::DataFrame)
            Exposed_Buyer=deepcopy(combined[combined[:Number_exposure_before_1st_buy] .!=0,[:Number_exposure_before_1st_buy]])
            Exposed_Buyer[Exposed_Buyer[:Number_exposure_before_1st_buy] .>=10,:Number_exposure_before_1st_buy]=10
            Exposed_Buyer_1=by(Exposed_Buyer, [:Number_exposure_before_1st_buy], nrow)
            Exposed_Buyer_1[:Cum_1st_purchases_capped]=cumsum( Exposed_Buyer_1[:x1])
            Exposed_Buyer_1[:Percentage_of_total_1st_purchases]=Exposed_Buyer_1[:Cum_1st_purchases_capped]/sum(Exposed_Buyer_1[:x1])
            Exposed_Buyer_1[:Obs]=collect(1:size(Exposed_Buyer_1,1))
            Exposed_Buyer_final=Exposed_Buyer_1[:,[:Obs, :Number_exposure_before_1st_buy, :x1, :Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]]
            names!(Exposed_Buyer_final,[:Obs,:Frequency,:Buying_HHs,:Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases])
            First_Buy_by_Frequency_Digital_std=Exposed_Buyer_final
                              #Calculate 1st Buy by Dynamic Frequency Buckets
                        Exposed_Buyer_dyn=deepcopy(combined[combined[:Number_exposure_before_1st_buy] .!=0,[:Number_exposure_before_1st_buy]])
            
                              Exposed_Buyer_dyn[:Exposures]="Exposures_le_"*string(freq_index[:frq_index][1])
                              
                              for dec in range(1,length(freq_index[:frq_index])-2)
                              Exposed_Buyer_dyn[(Exposed_Buyer_dyn[:Number_exposure_before_1st_buy] .>freq_index[:frq_index][dec]) & (Exposed_Buyer_dyn[:Number_exposure_before_1st_buy] .<=freq_index[:frq_index][dec+1]) & (freq_index[:frq_index][dec] .!= freq_index[:frq_index][dec+1]),:Exposures]="Exposures_g_"*string(freq_index[:frq_index][dec])*"_le_"*string(freq_index[:frq_index][dec+1])
                              end
                              Exposed_Buyer_dyn[(Exposed_Buyer_dyn[:Number_exposure_before_1st_buy] .>freq_index[:frq_index][end-1]) & (freq_index[:frq_index][end-1] .!= freq_index[:frq_index][end-2]),:Exposures]="Exposures_ge_"*string(freq_index[:frq_index][end-1])
                              
            Exposed_Buyer_1_dyn=by(Exposed_Buyer_dyn, [:Exposures], nrow)
                              Exposed_Buyer_1_dyn=join(Exposed_Buyer_1_dyn,freq_index, on = :Exposures, kind = :left)
                              Exposed_Buyer_1_dyn=sort!(Exposed_Buyer_1_dyn, cols = order(:frq_index))
            Exposed_Buyer_1_dyn[:Cum_1st_purchases_capped]=cumsum( Exposed_Buyer_1_dyn[:x1])
            Exposed_Buyer_1_dyn[:Percentage_of_total_1st_purchases]=Exposed_Buyer_1_dyn[:Cum_1st_purchases_capped]/sum(Exposed_Buyer_1_dyn[:x1])
            Exposed_Buyer_1_dyn[:Obs]=collect(1:size(Exposed_Buyer_1_dyn,1))
            Exposed_Buyer_final_dyn=Exposed_Buyer_1_dyn[:,[:Obs, :Exposures, :x1, :Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]]
            names!(Exposed_Buyer_final_dyn,[:Obs,:Frequency,:Buying_HHs,:Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases])
            First_Buy_by_Frequency_Digital_Dyn=Exposed_Buyer_final_dyn
                              return First_Buy_by_Frequency_Digital_std, First_Buy_by_Frequency_Digital_Dyn
    end
               
               function freq_HH_buying(combined::DataFrame)        
                              buyer_freq=deepcopy(combined[:,[:Exposures]])
        buyer_freq[buyer_freq[:Exposures] .>=10,:Exposures]=10
        buyer_freq_1=by(buyer_freq, [:Exposures], nrow)
        buyer_freq_1[:Obs]=collect(1:size(buyer_freq_1,1))
        buyer_freq_1[:Percentage_of_buying_HHs]=buyer_freq_1[:x1]/sum(buyer_freq_1[:x1]) 
        buyer_freq_1=buyer_freq_1[:,[:Obs,:Exposures,:x1,:Percentage_of_buying_HHs]]
        names!(buyer_freq_1,[:Obs,:Exposures,:HHs,:Percentage_of_buying_HHs])
        Buyer_Frequency_Digital=buyer_freq_1
                              return Buyer_Frequency_Digital
    end
    
    function first_buy_last_exp(combined::DataFrame)   
            buyer_exposure=deepcopy(combined[:,[:Time,:Number_exposure_before_1st_buy]])
            buyer_exposure_1=by(buyer_exposure, [:Time], nrow)
            buyer_exposure_1[:Obs]=collect(1:size(buyer_exposure_1,1))
            buyer_exposure_1[:Percentage_of_total_buying_HHs]=0.0
            buyer_exposure_1[buyer_exposure_1[:Time] .!="Pre",:Percentage_of_total_buying_HHs]=buyer_exposure_1[buyer_exposure_1[:Time] .!="Pre",:x1]/sum(buyer_exposure_1[buyer_exposure_1[:Time] .!="Pre",:x1])
            buyer_exposure_1=hcat(buyer_exposure_1,by(buyer_exposure, [:Time], buyer_exposure->mean(buyer_exposure[:Number_exposure_before_1st_buy])))
            buyer_exposure=join(buyer_exposure,by(buyer_exposure, [:Time], buyer_exposure->mean(buyer_exposure[:Number_exposure_before_1st_buy]) .+ 2.35 .* std(buyer_exposure[:Number_exposure_before_1st_buy]) ),on = :Time, kind = :left)
            buyer_exposure_2=by(buyer_exposure, [:Time], buyer_exposure->mean(buyer_exposure[buyer_exposure[:Number_exposure_before_1st_buy] .<=buyer_exposure[:x1],:Number_exposure_before_1st_buy]))
            buyer_exposure_final=join(buyer_exposure_1,buyer_exposure_2, on = :Time, kind = :left)
            buyer_exposure_final=buyer_exposure_final[:,[:Obs, :Time, :x1, :Percentage_of_total_buying_HHs, :x1_1, :x1_2]]
            names!(buyer_exposure_final,[:Obs,:Time,:Buying_HHs,:Percentage_of_total_buying_HHs, :Avg_Exposures_to_1st_buy, :Avg_Exposures_to_1st_buy_without_outliers])
                              return buyer_exposure_final;
    end     

function Total_freq_digital(hhcounts_date::DataFrame)
               exp_data2= hhcounts_date
               exp_data2 = exp_data2[exp_data2[:brk] .== unique(exp_data2[:brk])[1], :]
               expocnt_1=by(exp_data2, [:panid], df -> DataFrame(impressions= sum(df[:impressions])))
               rename!(expocnt_1,:impressions,:Exposures)
               expocnt_1[(expocnt_1[:Exposures] .>=10)   , :Exposures] = 10
               Total_Freq=by(expocnt_1, [:Exposures], nrow)
               names!(Total_Freq, [:Exposures, :HHs])
               Total_Freq[:Percentage_of_Total_HHs]=Total_Freq[:HHs]/sum(Total_Freq[:HHs])
               Total_Freq=hcat(Total_Freq,collect(1:size(Total_Freq,1)))
               Total_Freq=Total_Freq[:,[:x1, :Exposures, :HHs, :Percentage_of_Total_HHs]]
               names!(Total_Freq, [:Obs, :Exposures, :HHs, :Percentage_of_Total_HHs])
               return Total_Freq
       end

               

               function Cum_IMP(expocnt::DataFrame)
                   Cum_IMPs=by(expocnt, [:Exposures], nrow)
        names!(Cum_IMPs, [:Exposures, :HHs])
        Cum_IMPs[:imps_Served]=map(Int,Cum_IMPs[:HHs] .* Cum_IMPs[:Exposures])
        Cum_IMPs[:CUM_IMPs_Served]=cumsum( Cum_IMPs[:imps_Served])
        Cum_IMPs=hcat(Cum_IMPs,collect(1:size(Cum_IMPs,1)))  
        Cum_IMPs[ (Cum_IMPs[:x1] .>=Cum_IMPs[:Exposures]),[:x1,:Exposures ]]
        Cum_IMPs[:imps_served_capped]=sum(Cum_IMPs[:HHs])
        for row in 2:size(Cum_IMPs,1)
              Cum_IMPs[row,:imps_served_capped]=((Cum_IMPs[row,:Exposures])* sum(Cum_IMPs[row:size(Cum_IMPs,1),:HHs]))+Cum_IMPs[row-1,:CUM_IMPs_Served]
        end
        Cum_IMPs=Cum_IMPs[:,[:x1, :Exposures, :HHs, :imps_Served, :CUM_IMPs_Served, :imps_served_capped]]
        names!(Cum_IMPs, [:Obs, :Exposures, :HHs, :imps_Served, :CUM_IMPs_Served, :imps_served_capped])
                              return Cum_IMPs;
               end        
    
               function Buyer_Frequency_Characteristics(hhcounts_date::DataFrame,dfd::DataFrame )
                                             df = Dates.DateFormat("y-m-d");
                                             dt_base = Date("2014-12-28",df);
                                             buyer_exposure=join(dfd, hhcounts_date, on =:panid, kind= :inner)
                                             buyer_exposure= sort!(buyer_exposure[:,[:panid, :buyer_pos_p1 ,:buyer_pre_52w_p1 , :buyer_pre_52w_p0 ,:trps_pos_p1, :dte, :impressions]],cols=[order(:dte)])
                                             buyer_exposure[:dte] = map(x -> string(SubString(string(x),1,4),'-',SubString(string(x),5,6),'-',SubString(string(x),7,8)),buyer_exposure[:dte])
                                             buyer_exposure[:iri_week] = map(x -> convert(Int64, 1843+round(ceil(convert(Int64, (Date(x, df)-dt_base))/7),0)),buyer_exposure[:dte])
                                             buyer_exposure_dates=by(buyer_exposure,:iri_week,buyer_exposure->minimum(buyer_exposure[:dte]))
                                             exposed_buyer_by_week=DataFrame(iri_week=Int64[], WEEK_ID=String[], PCT_LAPSED_BUYERS=Int64[], PCT_BRAND_SWITCH_BUYERS=Int64[], PCT_BRAND_BUYERS=Int64[], PCT_CATEGORY_BUYERS=Int64[])
                                             for l=(1:length(buyer_exposure_dates[:iri_week]))
                                                            merge_table_per_date=buyer_exposure[buyer_exposure[:iri_week].==buyer_exposure_dates[l,:iri_week],:]
                                                                   PCT_LAPSED_BUYERS=sum(merge_table_per_date[(merge_table_per_date[:buyer_pos_p1] .== 0) & (merge_table_per_date[:buyer_pre_52w_p1] .== 1), :impressions])
                                                            PCT_BRAND_SWITCH_BUYERS=sum(merge_table_per_date[(merge_table_per_date[:buyer_pos_p1] .== 1) & (merge_table_per_date[:buyer_pre_52w_p1] .== 0) & (merge_table_per_date[:buyer_pre_52w_p0] .== 1), :impressions])
                                                                   PCT_BRAND_BUYERS=sum(merge_table_per_date[(merge_table_per_date[:buyer_pos_p1] .== 1), :impressions])
                                                            PCT_CATEGORY_BUYERS=sum(merge_table_per_date[(merge_table_per_date[:buyer_pre_52w_p0] .== 1), :impressions])
                                                                           final_pur_data_buyer_temp=[buyer_exposure_dates[l,:iri_week] buyer_exposure_dates[l,:x1] PCT_LAPSED_BUYERS PCT_BRAND_SWITCH_BUYERS PCT_BRAND_BUYERS PCT_CATEGORY_BUYERS]
                                                                           push!(exposed_buyer_by_week,final_pur_data_buyer_temp)
                                             end
											 rowsum = DataFrame(sum(Array(exposed_buyer_by_week[:,collect(3:ncol(exposed_buyer_by_week))]),1));
											 exposed_buyer_by_week_copy = deepcopy(exposed_buyer_by_week);
											 for i in 3:ncol(exposed_buyer_by_week_copy)
                                                   exposed_buyer_by_week_copy[i] = cumsum(exposed_buyer_by_week_copy[i])
                                             end
											 for i in 3:ncol(exposed_buyer_by_week_copy)
                                                    exposed_buyer_by_week_copy[i] = exposed_buyer_by_week_copy[i] ./ rowsum[i-2][1] *100
                                             end
											 cumulative_by_week = deepcopy(exposed_buyer_by_week_copy);

                                             return exposed_buyer_by_week,cumulative_by_week;
               end        
               
               
function FormatUnifyReports(Tm_1st_by_lst_xpsur_Dgtl::DataFrame,dfo_Exp::DataFrame,dfo_UnExp::DataFrame ,dfd_rpt_trl::DataFrame,dfd_fr_shr_ndx::DataFrame,dfd_upc_grwth_cnt::DataFrame,imp_week::DataFrame, exposed_buyer_by_week::DataFrame,shr_rqrmnt::DataFrame,Frst_Buy_Frq_Dgtl_std::DataFrame, cfg, ChannelCode::Int64,flag::Int64)
     	Lift_Buyer_char_template=DataFrame(model_desc = String[], model = String[], time_agg_period = Int64[], start_week = Int64[], end_week = Int64[], characteristics_indicator_flag = String[], cum_num_cat_buyer = Float64[], cum_num_brd_shift_buyer = Float64[], cum_num_brd_buyer = Float64[], cum_num_lapsed_buyer = Float64[], cum_total_buyer = Float64[], pct_tot_hh = Float64[], pct_buy_hh = Float64[], cum_num_new_buyer = Int64[], cum_non_brd = Int64[], cum_num_repeat = Int64[], cum_num_cat_shift_buyer = Int64[], cum_pct_repeat_expsd = Float64[], cum_pct_trail_expsd = Float64[], impression_count = Int64[], cumulatve_hh_count = Int64[], channel_code = Int64[])
    	start_week=parse(Int64,cfg[:start_week])
    	end_week=parse(Int64,cfg[:end_week])
        Time_1st_buy= deepcopy(Tm_1st_by_lst_xpsur_Dgtl)
        FREQ="FRQ"
		
		if cfg[:campaign_type] == :lift || cfg[:campaign_type] == :SamsClub  || cfg[:campaign_type] == :digitallift || cfg[:campaign_type] == :digitalliftuat
    	       for i in 1:(nrow(Time_1st_buy)-1)
    	       	if i == 10
    	       		push!(Lift_Buyer_char_template, [string("10+"),FREQ * string(i),end_week-start_week+1,start_week,start_week+i-1,"BUYER_CHAR_WEEK_FREQUENCY",0,0,0,0,0,Frst_Buy_Frq_Dgtl_std[i,:Percentage_of_total_1st_purchases],Time_1st_buy[i,:Percentage_of_total_buying_HHs],0,0,0,0,0,0,0,0,ChannelCode] )
    	       	else	
    	       		push!(Lift_Buyer_char_template, [string(i),FREQ * string(i),end_week-start_week+1,start_week,start_week+i-1,"BUYER_CHAR_WEEK_FREQUENCY",0,0,0,0,0,Frst_Buy_Frq_Dgtl_std[i,:Percentage_of_total_1st_purchases],Time_1st_buy[i,:Percentage_of_total_buying_HHs],0,0,0,0,0,0,0,0,ChannelCode] )
    	       	end
    	       end
    	       for i in 1:(nrow(Time_1st_buy)-1)
    	       	if i == 10
    	       		push!(Lift_Buyer_char_template, [string("10+"),FREQ * string(i),end_week-start_week+1,start_week,start_week+i-1,"BUYER_CHAR_FREQUENCY",0,0,0,0,0,Frst_Buy_Frq_Dgtl_std[i,:Percentage_of_total_1st_purchases],Time_1st_buy[i,:Percentage_of_total_buying_HHs],0,0,0,0,0,0,0,0,ChannelCode] )
    	       	else
    	       		push!(Lift_Buyer_char_template, [string(i),FREQ * string(i),end_week-start_week+1,start_week,start_week+i-1,"BUYER_CHAR_FREQUENCY",0,0,0,0,0,Frst_Buy_Frq_Dgtl_std[i,:Percentage_of_total_1st_purchases],Time_1st_buy[i,:Percentage_of_total_buying_HHs],0,0,0,0,0,0,0,0,ChannelCode] )
    	       	end
    	       end
	    end
		
		if cfg[:campaign_type] == :google 
		       for i in 1:(nrow(Time_1st_buy)-1)
                              push!(Lift_Buyer_char_template, [string(i),FREQ * string(i),end_week-start_week+1,start_week,start_week+i-1,"BUYER_CHAR_WEEK_FREQUENCY",0,0,0,0,0,0,Time_1st_buy[i,:Percentage_of_total_buying_HHs],0,0,0,0,0,0,0,0,ChannelCode] )
               end
               for i in 1:(nrow(Time_1st_buy)-1)
                              push!(Lift_Buyer_char_template, [string(i),FREQ * string(i),end_week-start_week+1,start_week,start_week+i-1,"BUYER_CHAR_FREQUENCY",0,0,0,0,0,0,Time_1st_buy[i,:Percentage_of_total_buying_HHs],0,0,0,0,0,0,0,0,ChannelCode] )
               end
	    end
		
    	TCP0="TCP0"
		 	if flag == 1 
			  for i in unique(exposed_buyer_by_week[:iri_week])
	    		PCT_LAPSED_BUYERS_T = sum(exposed_buyer_by_week[:PCT_LAPSED_BUYERS])
	    		PCT_BRAND_SWITCH_BUYERS_T = sum(exposed_buyer_by_week[:PCT_BRAND_SWITCH_BUYERS])
	    		PCT_BRAND_BUYERS_T = sum(exposed_buyer_by_week[:PCT_BRAND_BUYERS])
	    		PCT_CATEGORY_BUYERS_T = sum(exposed_buyer_by_week[:PCT_CATEGORY_BUYERS])
	    		push!(Lift_Buyer_char_template, ["Total Campaign",TCP0,i - start_week + 1,i,end_week,"BUYER_CHAR_REACH",sum(exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].<=i,:PCT_CATEGORY_BUYERS])/PCT_CATEGORY_BUYERS_T, sum(exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].<=i,:PCT_BRAND_SWITCH_BUYERS])/PCT_BRAND_SWITCH_BUYERS_T, sum(exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].<=i,:PCT_BRAND_BUYERS])/PCT_BRAND_BUYERS_T, sum(exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].<=i,:PCT_LAPSED_BUYERS])/PCT_LAPSED_BUYERS_T,0,0,0,0,0,0,0,0,0,0,0,ChannelCode] )
	    		push!(Lift_Buyer_char_template, ["Total Campaign",TCP0,i - start_week + 1,i,end_week,"BUYER_CHAR_EXPOSURES",convert(Float64,exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].==i,:PCT_CATEGORY_BUYERS][1]), convert(Float64,exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].==i,:PCT_BRAND_SWITCH_BUYERS][1]), convert(Float64,exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].==i,:PCT_BRAND_BUYERS][1]), convert(Float64,exposed_buyer_by_week[exposed_buyer_by_week[:iri_week].==i,:PCT_LAPSED_BUYERS][1]),0,0,0,0,0,0,0,0,0,0,0,ChannelCode] )
	    	end
      end
    	brand_buyers_unexposed_cnt= deepcopy(dfo_UnExp);
    	push!(Lift_Buyer_char_template, ["Total Campaign",TCP0,end_week-start_week+1,start_week,end_week,"BUYER_CHAR_NON_EXP_BUYER",0,convert(Int32,ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "BRAND SWITCHER",:CNT][1])),convert(Int32,ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "BRAND BUYERS",:CNT][1])),convert(Int32,ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "LAPSED BUYERS",:CNT][1])),0,0,0,convert(Int32,ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "NEW BUYERS",:CNT][1])),convert(Int32,ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "NON BUYERS",:CNT][1])),convert(Int32,ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "REPEAT BUYERS",:CNT][1])),convert(Int32,ceil(brand_buyers_unexposed_cnt[brand_buyers_unexposed_cnt[:buyer_type] .== "CATEGORY SWITCHER",:CNT][1])),0,0,0,0,ChannelCode] )
    	brand_buyers_exposed_cnt= deepcopy(dfo_Exp);
    	push!(Lift_Buyer_char_template, ["Total Campaign",TCP0,end_week-start_week+1,start_week,end_week,"BUYER_CHAR_BUYER",0,convert(Int32,ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "BRAND SWITCHER",:CNT][1])),convert(Int32,ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "BRAND BUYERS",:CNT][1])),convert(Int32,ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "LAPSED BUYERS",:CNT][1])),0,0,0,convert(Int32,ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "NEW BUYERS",:CNT][1])),convert(Int32,ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "NON BUYERS",:CNT][1])),convert(Int32,ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "REPEAT BUYERS",:CNT][1])),convert(Int32,ceil(brand_buyers_exposed_cnt[brand_buyers_exposed_cnt[:buyer_type] .== "CATEGORY SWITCHER",:CNT][1])),0,0,0,0,ChannelCode])
    	csv_trial_repeat= deepcopy(dfd_rpt_trl);
    	push!(Lift_Buyer_char_template, ["Total Campaign",TCP0,end_week-start_week+1,start_week,end_week,"BUYER_CHAR_TRIAL_REPEAT",0,0,0,0,0,0,0,0,0,0,0,csv_trial_repeat[csv_trial_repeat[:grouptype] .== "Exposed",:repeaters_percent][1],csv_trial_repeat[csv_trial_repeat[:grouptype] .== "Exposed",:triers_percent][1],0,0,ChannelCode] )
      push!(Lift_Buyer_char_template, ["Total Campaign",TCP0,end_week-start_week+1,start_week,end_week,"BUYER_CHAR_NON_EXP_TRIAL_REPEAT",0,0,0,0,0,0,0,0,0,0,0,csv_trial_repeat[csv_trial_repeat[:grouptype] .== "Unexposed",:repeaters_percent][1],csv_trial_repeat[csv_trial_repeat[:grouptype] .== "Unexposed",:triers_percent][1],0,0,ChannelCode] )
    	mlift_temp_additional_report=DataFrame(target_competitor_ind = String[], description = String[], upc10 = String[], PRE_sales = String[], POS_sales = String[], VARIATION = String[], time_agg_period = Int64[], channel_code = Int64[], grp_index = Int64[], registration_id = String[], registration_request_id = String[])
    	start_week=parse(Int64,cfg[:start_week])
    	end_week=parse(Int64,cfg[:end_week])
    	time_agg_period=end_week-start_week+1
    	upc_growth_df = deepcopy(dfd_upc_grwth_cnt);
        agg_upc_growth_df = DataFrame(description=String[],upc10=String[],sales_upc_pre=Float64[],sales_upc_post=Float64[],percentage_sales_upc_pre=Float64[],percentage_sales_upc_post=Float64[],growth_contribution=Float64[]);
    	for i in unique(upc_growth_df[:description])
    		push!(agg_upc_growth_df,[upc_growth_df[upc_growth_df[:description] .== i ,:description][1],
    		upc_growth_df[upc_growth_df[:description] .== i ,:upc10][1],
    		mean(upc_growth_df[upc_growth_df[:description] .== i ,:sales_upc_pre]),
    		mean(upc_growth_df[upc_growth_df[:description] .== i ,:sales_upc_post]),
    		mean(upc_growth_df[upc_growth_df[:description] .== i ,:percentage_sales_upc_pre]),
    		mean(upc_growth_df[upc_growth_df[:description] .== i ,:percentage_sales_upc_post]),
    		mean(upc_growth_df[upc_growth_df[:description] .== i ,:growth_contribution])])
    	end
    	sort!(agg_upc_growth_df,cols=:growth_contribution,rev=true)
    	for i in 1:nrow(agg_upc_growth_df)
    		push!(mlift_temp_additional_report, ["",upc_growth_df[i,1],string(upc_growth_df[i,2]),string(convert(BigInt, trunc(upc_growth_df[i,3]))),string(convert(BigInt, trunc(upc_growth_df[i,4]))),"UPC_GROWTH_CONTRIBUTION",time_agg_period,ChannelCode,0,cfg[:reg_id],cfg[:reg_req_id]] )
    	end
    	fair_share_df = deepcopy(dfd_fr_shr_ndx);
    	fair_share_df[:type] ="";
    	for i in 1:nrow(fair_share_df)
    	    if fair_share_df[:product_grp_id][i] == 1   fair_share_df[:type][i] = "advertised" else  fair_share_df[:type][i] = "competitor" end
    	end
    	for i in 1:nrow(fair_share_df)
    		push!(mlift_temp_additional_report, [fair_share_df[i,10],string(fair_share_df[i,2]),"",string(convert(BigInt, trunc(fair_share_df[i,3]))),string(convert(BigInt, trunc(fair_share_df[i,5]))),"FAIR_SHARE_INDEX",time_agg_period,ChannelCode,fair_share_df[i,1],cfg[:reg_id],cfg[:reg_req_id]] )
    	end
    	#################Share of Requirement
		if cfg[:campaign_type] == :lift || cfg[:campaign_type] == :SamsClub  || cfg[:campaign_type] == :digitallift || cfg[:campaign_type] == :digitalliftuat
    	    push!(mlift_temp_additional_report, ["","","",string(1 - shr_rqrmnt[shr_rqrmnt[:exposed_flag].==1,:product_group_share][1]),string(shr_rqrmnt[shr_rqrmnt[:exposed_flag].==1,:product_group_share][1]),"BRAND_SHARE_EXPOSED",time_agg_period,ChannelCode,0,cfg[:reg_id],cfg[:reg_req_id]] )
    	    push!(mlift_temp_additional_report, ["","","",string(1 - shr_rqrmnt[shr_rqrmnt[:exposed_flag].==0,:product_group_share][1]),string(shr_rqrmnt[shr_rqrmnt[:exposed_flag].==0,:product_group_share][1]),"BRAND_SHARE_NONEXPOSED",time_agg_period,ChannelCode,0,cfg[:reg_id],cfg[:reg_req_id]] )
    	end
		
	    if cfg[:campaign_type] == :lift || cfg[:campaign_type] == :SamsClub  || cfg[:campaign_type] == :digitallift || cfg[:campaign_type] == :digitalliftuat
			  for i in sort(imp_week[:iri_week])
	    		CumulativeHhs = sum(imp_week[imp_week[:iri_week].<=i,:hhs])
				push!(Lift_Buyer_char_template, [string(i-start_week+1),TCP0,i-start_week+1,start_week,i,"BUYER_CHAR_IMP_HH_COUNTS",0,0,0,0,0,0,0,0,0,0,0,0,0,imp_week[imp_week[:iri_week].==i,:impressions][1],CumulativeHhs,ChannelCode] )
			  end	
	    end		  
        for i in 1:ncol(Lift_Buyer_char_template)
             if  eltype(Lift_Buyer_char_template[i]) == Float64
                  Lift_Buyer_char_template[i] = round(Lift_Buyer_char_template[i],4)
             end
        end
	    for i in 1:ncol(mlift_temp_additional_report)
             if  eltype(mlift_temp_additional_report[i]) == Float64
                  mlift_temp_additional_report[i] = round(mlift_temp_additional_report[i],4)
             end
        end

    	return mlift_temp_additional_report,Lift_Buyer_char_template
end
    
    function run(cfg::DataStructures.OrderedDict{Any,Any},scored::DataFrame, brand_data::DataFrame,upc_data::DataFrame,hhcounts_date::DataFrame,buyer_week_data::DataFrame,descDump::DataFrame,src::String,flag::Int64,imp_week::DataFrame = DataFrame(iri_week=Int64[],exposure_date=String[],hhs=Int64[],impressions=Int64[]))       
    lgqc(0,600,1001,2000);                dfmx,dfd,scored,brand_data,upc_data,hhcounts_date,buyer_week_data =  CDMA_dataprep(cfg,scored,brand_data,upc_data,hhcounts_date,buyer_week_data,imp_week,descDump,src,flag)
    lgqc(0,600,1002,2000);                dfo_Exp,dfo_UnExp = genbuyerclass(dfd,scored)
    lgqc(0,600,1003,2000);                dfd_rpt_trl       = gentrialRepeat(dfd,scored)
    lgqc(0,600,1004,2000);                dfd_fr_shr_ndx    = fair_share(dfd,brand_data)
    lgqc(0,600,1005,2000);                shr_rqrmnt        = Share_of_requirements(dfd)
    lgqc(0,600,1006,2000);                dfd_upc_grwth_cnt = upc_growth(dfd,upc_data)
    lgqc(0,600,1007,2000);                pur_data1, exp_data1, exp_data_n1, combined, freq_index,expocnt = genFreq_dataprep(hhcounts_date,buyer_week_data)
    lgqc(0,600,1008,2000);                Frst_Buy_Frq_Dgtl_std, Frst_Buy_Frq_Dgtl_Dyn = freq_HH_Cum1stpur(combined, freq_index)
    lgqc(0,600,1009,2000);                Byr_Frq_Dgtl             = freq_HH_buying(combined)
    lgqc(0,600,1010,2000);                Tm_1st_by_lst_xpsur_Dgtl = first_buy_last_exp(combined);
    lgqc(0,600,1011,2000);                Tot_Frq_Dgtl    = Total_freq_digital(hhcounts_date);
    lgqc(0,600,1012,2000);                dfd_Cum_IMP         = Cum_IMP(expocnt);
    lgqc(0,600,1013,2000);                exposed_buyer_by_week, cumulative_by_week = Buyer_Frequency_Characteristics(hhcounts_date::DataFrame,dfd::DataFrame );
    lgqc(0,600,1014,2000);                mlft_tmp_add_rprt,Lft_Byr_chr_tmp=FormatUnifyReports(Tm_1st_by_lst_xpsur_Dgtl,dfo_Exp,dfo_UnExp ,dfd_rpt_trl,dfd_fr_shr_ndx,dfd_upc_grwth_cnt,imp_week,exposed_buyer_by_week,shr_rqrmnt,Frst_Buy_Frq_Dgtl_std,cfg, 1,flag);
    return    dfo_Exp,dfo_UnExp,dfd_rpt_trl, dfd_fr_shr_ndx,shr_rqrmnt, dfd_upc_grwth_cnt, Frst_Buy_Frq_Dgtl_std, Frst_Buy_Frq_Dgtl_Dyn,Byr_Frq_Dgtl, Tm_1st_by_lst_xpsur_Dgtl, Tot_Frq_Dgtl,dfd_Cum_IMP,exposed_buyer_by_week,cumulative_by_week, mlft_tmp_add_rprt,Lft_Byr_chr_tmp ;
    end
       
end
 


