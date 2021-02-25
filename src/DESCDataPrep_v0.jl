# Julia v0.5.2
module DESCDataPrep

using DataFrames, CLib, IRIfunc, XLib   # CLib needed for `rf`; IRIfunc needed for `CFGwf`; XLib needed for `strdf`

function base_dataprep(isenc::Bool)
    cfg = CFGwf(isenc);
    hdr = Array(strdf(rf(cfg[:files][:hdr]), header=false)[:x1]);
    for i in 1:length(hdr) hdr[i] = hdr[i] == "iri_link_id" ? "banner" : hdr[i] end
    for i in 1:length(hdr) hdr[i] = hdr[i] == "proscore" ? "model" : hdr[i] end
    M = MFile(hdr, cfg);
    src = rf(cfg[:files][:orig]);
    c = [:panid, :group, :buyer_pos_p1, :buyer_pos_p0, :trps_pos_p1, :buyer_pre_p1, :buyer_pre_p0, :dol_per_trip_pre_p1, :dol_per_trip_pre_p0, :dol_per_trip_pos_p1, :dol_per_trip_pos_p0, :trps_pos_p0, :trps_pre_p1, :trps_pre_p0, :banner, :model];
    M2 = MFile(c, M);
    dfmx = creadData([-1], M2, "", src);
    descDump = readtable(cfg[:files][:Match_dump]);
    println("Read Brand UPC")
    src_upcs = rf("./Descriptives/Brand_upc.csv");
    N = readtable("./Descriptives/N.csv", header=true); # The file N.csv is the equivalent of the M data frame
    df_upcs = creadData([-1], N, "", src_upcs);
    println("Processing Brand UPC")
    df_upcs_mx = join(descDump[[:panid, :group]], df_upcs, on=:panid, kind=:inner);
    println("Done with Brand UPC")

    return dfmx, descDump, df_upcs_mx
end

end
