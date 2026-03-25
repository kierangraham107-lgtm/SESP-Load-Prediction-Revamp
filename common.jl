#!/usr/bin/env julia
#=
  common.jl — Shared utilities for the SESP Ottawa 2050 pipeline
  Include this file first: include("common.jl")
=#

using TOML, CSV, DataFrames, Dates, Printf, Statistics

# ══════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════

function load_config(path::String="config.toml")
    isfile(path) || error("Config not found: $path — run from the SESP_Ottawa_2050/ directory")
    cfg = TOML.parsefile(path)

    # Compute derived values and attach them
    yrs = cfg["census"]["years_from_2021"]
    dwell_rate = cfg["census"]["dwelling_growth_rate"]
    cfg["_derived"] = Dict{String,Any}()

    cfg["_derived"]["dwelling_growth_factor"] = (1 + dwell_rate)^yrs
    cfg["_derived"]["pop_growth_factor"] =
        (cfg["population"]["pop_2021"] / cfg["population"]["pop_2016"])^(yrs / 5)

    # Dwelling counts 2023 and 2050
    dgf = cfg["_derived"]["dwelling_growth_factor"]
    sf_frac = cfg["census"]["sf_dwellings_2021"] / cfg["census"]["total_dwellings_2021"]
    lr_frac = cfg["census"]["lr_dwellings_2021"] / cfg["census"]["total_dwellings_2021"]
    mh_frac = cfg["census"]["mh_dwellings_2021"] / cfg["census"]["total_dwellings_2021"]

    d2023 = cfg["census"]["total_dwellings_2021"] * (1 + dwell_rate)^2
    d2050 = cfg["census"]["total_dwellings_2021"] * dgf

    cfg["_derived"]["sf_2023"] = d2023 * sf_frac
    cfg["_derived"]["lr_2023"] = d2023 * lr_frac
    cfg["_derived"]["mh_2023"] = d2023 * mh_frac
    cfg["_derived"]["sf_2050"] = d2050 * sf_frac
    cfg["_derived"]["lr_2050"] = d2050 * lr_frac
    cfg["_derived"]["mh_2050"] = d2050 * mh_frac

    # Active scenario values
    active_hp = cfg["scenarios"]["active_hp"]
    active_plug = cfg["scenarios"]["active_plug"]
    cfg["_derived"]["hp_penetration_2050"] = cfg["hp_scenarios"][active_hp]
    plug_per_unit = cfg["plug_scenarios"][active_plug]
    cfg["_derived"]["plug_growth_factor"] = plug_per_unit * dgf

    # Residual multiplier
    active_res = cfg["scenarios"]["active_residual"]
    if active_res == "M1"
        cfg["_derived"]["residual_multiplier"] = cfg["residual_methods"]["M1"]
    elseif active_res == "M2"
        cfg["_derived"]["residual_multiplier"] = dgf
    elseif active_res == "M3"
        cfg["_derived"]["residual_multiplier"] = cfg["_derived"]["pop_growth_factor"]
    else
        error("Unknown residual method: $active_res")
    end

    return cfg
end

function print_config_summary(cfg::Dict)
    d = cfg["_derived"]
    s = cfg["scenarios"]
    println("  ┌─────────────────────────────────────────────┐")
    println("  │ Active scenarios                            │")
    println(@sprintf("  │   EV:         %-10s                     │", s["active_ev"]))
    println(@sprintf("  │   Commercial: %-10s                     │", s["active_commercial"]))
    println(@sprintf("  │   HP:         %-10s (pen=%.0f%%)           │", s["active_hp"], d["hp_penetration_2050"]*100))
    println(@sprintf("  │   Plug:       %-10s (factor=%.4f)     │", s["active_plug"], d["plug_growth_factor"]))
    println(@sprintf("  │   Residual:   %-10s (mult=%.4f)       │", s["active_residual"], d["residual_multiplier"]))
    println("  ├─────────────────────────────────────────────┤")
    println(@sprintf("  │ Dwelling growth factor:  %.4f            │", d["dwelling_growth_factor"]))
    println(@sprintf("  │ Population growth factor: %.4f           │", d["pop_growth_factor"]))
    println(@sprintf("  │ SF 2050: %8.0f  LR: %7.0f  MH: %7.0f │", d["sf_2050"], d["lr_2050"], d["mh_2050"]))
    println("  └─────────────────────────────────────────────┘")
end

# ══════════════════════════════════════════════════════════════
#  COP LOOKUP
# ══════════════════════════════════════════════════════════════

struct COPCurve
    lower_bounds::Vector{Float64}
    upper_bounds::Vector{Float64}
    cop_values::Vector{Float64}      # COP1 (heating) — the active column
end

function load_cop_curve(path::String)
    df = CSV.read(path, DataFrame; silencewarnings=true)
    lowers = Float64.(df.OAT_Lower_C)
    uppers = Vector{Float64}(undef, nrow(df))
    cops   = Float64.(df.COP1_Heating)
    for i in 1:nrow(df)
        if i < nrow(df)
            uppers[i] = Float64(df.OAT_Upper_C[i])
        else
            uppers[i] = 100.0  # last bin extends to +100°C
        end
    end
    return COPCurve(lowers, uppers, cops)
end

function lookup_cop(curve::COPCurve, oat::Float64)
    for i in eachindex(curve.lower_bounds)
        if oat >= curve.lower_bounds[i] && oat < curve.upper_bounds[i]
            return curve.cop_values[i]
        end
    end
    # Fallback: clamp to nearest bin
    return oat < curve.lower_bounds[1] ? curve.cop_values[1] : curve.cop_values[end]
end

# Vectorized version
function lookup_cop(curve::COPCurve, oats::Vector{<:Number})
    return [lookup_cop(curve, Float64(t)) for t in oats]
end

# ══════════════════════════════════════════════════════════════
#  DATA LOADING HELPERS
# ══════════════════════════════════════════════════════════════

"""Load a 2-column CSV (timestamp, value) into a DataFrame with proper datetime parsing."""
function load_hourly_csv(path::String; value_col::String="value")
    df = CSV.read(path, DataFrame; silencewarnings=true, stringtype=String)
    cols = names(df)

    # Parse timestamps
    ts_col = cols[1]
    if eltype(df[!, ts_col]) <: Union{Missing, DateTime}
        df[!, :datetime] = DateTime.(df[!, ts_col])
    elseif eltype(df[!, ts_col]) <: Union{Missing, Date}
        df[!, :datetime] = DateTime.(df[!, ts_col])
    else
        raw = string.(df[!, ts_col])
        parsed = DateTime[]
        for s in raw
            s = strip(s)
            dt = nothing
            for fmt in [dateformat"yyyy-mm-dd HH:MM:SS", dateformat"yyyy-mm-dd HH:MM:SS.s",
                        dateformat"yyyy-mm-dd HH:MM"]
                try dt = DateTime(s, fmt); break; catch; end
            end
            # Handle fractional seconds like "01:59:59.712000"
            if dt === nothing && occursin(r"\.\d+$", s)
                s_clean = replace(s, r"\.\d+$" => "")
                for fmt in [dateformat"yyyy-mm-dd HH:MM:SS", dateformat"yyyy-mm-dd HH:MM"]
                    try dt = DateTime(s_clean, fmt); break; catch; end
                end
            end
            dt === nothing && error("Cannot parse datetime: '$s'")
            push!(parsed, dt)
        end
        df[!, :datetime] = parsed
    end

    # Round timestamps to nearest hour (fix Excel floating-point drift)
    df[!, :datetime] = round.(df.datetime, Dates.Hour)

    # Rename value column
    val_col = cols[2]
    if val_col != value_col
        rename!(df, val_col => value_col)
    end

    select!(df, :datetime, Symbol(value_col))
    return df
end

# ══════════════════════════════════════════════════════════════
#  ANALYSIS HELPERS
# ══════════════════════════════════════════════════════════════

function compute_annual_energy_twh(load_mw::Vector{<:Number})
    return sum(load_mw) / 1e6  # MW × 1 hour = MWh, ÷ 1e6 = TWh
end

function compute_peak_mw(load_mw::Vector{<:Number})
    return maximum(load_mw)
end
