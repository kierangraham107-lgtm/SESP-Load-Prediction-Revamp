#!/usr/bin/env julia
#=
  residential.jl — Residential Load Model
  ────────────────────────────────────────
  Replicates the logic in Load_Disaggregation_V10.xlsx → Hourly_Load_Res tab.
  Three components per hour:
    1. Electric heating = max(slope×HDH + intercept, 0) / COP × HP_penetration
    2. Cooling = (cool_sf×SF + cool_lr×LR + cool_mh×MH) × AC_pen / 1000
    3. Plug load = base_plug_2023 × plug_growth_factor
  Total Residential = heating + cooling + plug

  Can run standalone:  julia src/residential.jl [config.toml]
  Or include from pipeline:  include("residential.jl"); res = run_residential(cfg)
=#

if !@isdefined(load_config)
    include(joinpath(@__DIR__, "common.jl"))
end

function run_residential(cfg::Dict; verbose::Bool=true)
    d    = cfg["_derived"]
    hreg = cfg["heating_regression"]
    elec = cfg["electrification"]

    verbose && println("\n▸ Running residential load model...")

    base_dir = get(cfg["paths"], "data_dir", "data")

    oat_2023_df = load_hourly_csv(joinpath(base_dir, "ottawa_oat_2023.csv"); value_col="oat_2023")
    oat_2050_df = load_hourly_csv(joinpath(base_dir, "ottawa_oat_2050_rcp45.csv"); value_col="oat_2050")
    plug_df     = load_hourly_csv(joinpath(base_dir, "base_plug_2023.csv"); value_col="plug_mw")
    cool_df     = CSV.read(joinpath(base_dir, "cooling_per_dwelling.csv"), DataFrame; silencewarnings=true, stringtype=String)

    # Parse cooling timestamps (handle fractional seconds from Excel drift)
    raw_ts = string.(cool_df[!, names(cool_df)[1]])
    parsed = DateTime[]
    for s in raw_ts
        s = strip(s)
        s_clean = replace(s, r"\.\d+$" => "")
        dt = nothing
        for fmt in [dateformat"yyyy-mm-dd HH:MM:SS", dateformat"yyyy-mm-dd HH:MM"]
            try dt = DateTime(s_clean, fmt); break; catch; end
        end
        dt === nothing && error("Cannot parse: '$s'")
        push!(parsed, dt)
    end
    cool_df[!, :datetime] = round.(parsed, Dates.Hour)

    cool_sf = [ismissing(v) ? 0.0 : Float64(v) for v in cool_df.cool_sf_kw]
    cool_lr = [ismissing(v) ? 0.0 : Float64(v) for v in cool_df.cool_lr_kw]
    cool_mh = [ismissing(v) ? 0.0 : Float64(v) for v in cool_df.cool_mh_kw]

    cop_curve = load_cop_curve(cfg["paths"]["cop_csv"])

    n = nrow(oat_2023_df)
    n == 8760 || @warn "Expected 8760 rows, got $n"

    datetimes = oat_2023_df.datetime
    oat_2023  = Float64.(oat_2023_df.oat_2023)
    oat_2050  = Float64.(oat_2050_df.oat_2050)
    plug_2023 = Float64.(plug_df.plug_mw)

    slope     = hreg["slope_mw_per_hdh"]
    intercept = hreg["intercept_mw"]
    hp_2023   = elec["hp_penetration_2023"]
    hp_2050   = d["hp_penetration_2050"]
    ac_2023   = elec["ac_penetration_2023"]
    ac_2050   = elec["ac_penetration_2050"]
    plug_factor = d["plug_growth_factor"]

    sf_2023 = d["sf_2023"];  lr_2023 = d["lr_2023"];  mh_2023 = d["mh_2023"]
    sf_2050 = d["sf_2050"];  lr_2050 = d["lr_2050"];  mh_2050 = d["mh_2050"]

    hdh_2023 = max.(18.0 .- oat_2023, 0.0)
    hdh_2050 = max.(18.0 .- oat_2050, 0.0)
    cdh_2023 = max.(oat_2023 .- 18.0, 0.0)
    cdh_2050 = max.(oat_2050 .- 18.0, 0.0)

    cop_2023 = lookup_cop(cop_curve, oat_2023)
    cop_2050 = lookup_cop(cop_curve, oat_2050)

    thermal_2023 = max.(slope .* hdh_2023 .+ intercept, 0.0)
    thermal_2050 = max.(slope .* hdh_2050 .+ intercept, 0.0)

    elec_heat_2023 = thermal_2023 ./ cop_2023 .* hp_2023
    elec_heat_2050 = thermal_2050 ./ cop_2050 .* hp_2050

    cool_2023 = (cool_sf .* sf_2023 .+ cool_lr .* lr_2023 .+ cool_mh .* mh_2023) .* ac_2023 ./ 1000.0
    cool_2050 = (cool_sf .* sf_2050 .+ cool_lr .* lr_2050 .+ cool_mh .* mh_2050) .* ac_2050 ./ 1000.0

    plug_2050 = plug_2023 .* plug_factor

    total_2023 = elec_heat_2023 .+ cool_2023 .+ plug_2023
    total_2050 = elec_heat_2050 .+ cool_2050 .+ plug_2050

    result = DataFrame(
        datetime=datetimes, oat_2023=oat_2023, oat_2050=oat_2050,
        hdh_2023=hdh_2023, hdh_2050=hdh_2050, cdh_2023=cdh_2023, cdh_2050=cdh_2050,
        cop_2023=cop_2023, cop_2050=cop_2050,
        thermal_2023_mw=thermal_2023, thermal_2050_mw=thermal_2050,
        heat_elec_2023=elec_heat_2023, heat_elec_2050=elec_heat_2050,
        cool_2023_mw=cool_2023, cool_2050_mw=cool_2050,
        plug_2023_mw=plug_2023, plug_2050_mw=plug_2050,
        total_res_2023=total_2023, total_res_2050=total_2050,
    )

    if verbose
        println("  ✓ Residential model complete ($(nrow(result)) hours)")
        println(@sprintf("    2023: %.3f TWh, peak %.1f MW", compute_annual_energy_twh(total_2023), compute_peak_mw(total_2023)))
        println(@sprintf("    2050: %.3f TWh, peak %.1f MW", compute_annual_energy_twh(total_2050), compute_peak_mw(total_2050)))
        println(@sprintf("    Heating 2050: %.3f TWh (peak %.1f MW)", compute_annual_energy_twh(elec_heat_2050), compute_peak_mw(elec_heat_2050)))
        println(@sprintf("    Cooling 2050: %.3f TWh (peak %.1f MW)", compute_annual_energy_twh(cool_2050), compute_peak_mw(cool_2050)))
        println(@sprintf("    Plug    2050: %.3f TWh (peak %.1f MW)", compute_annual_energy_twh(plug_2050), compute_peak_mw(plug_2050)))
    end
    return result
end

if abspath(PROGRAM_FILE) == @__FILE__
    cfg = load_config(length(ARGS) > 0 ? ARGS[1] : "config.toml")
    println("="^70)
    println("  SESP Ottawa 2050 — Residential Load Model")
    println("="^70)
    print_config_summary(cfg)
    res = run_residential(cfg)
    mkpath(cfg["paths"]["results"])
    CSV.write(joinpath(cfg["paths"]["results"], "residential_hourly.csv"), res)
    println("\n  ✓ Saved to $(cfg["paths"]["results"])/residential_hourly.csv")
end
