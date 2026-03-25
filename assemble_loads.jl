#!/usr/bin/env julia
#=
  assemble_loads.jl — Total Load Assembly
  ────────────────────────────────────────
  Replicates the logic in Total Loads + Residual_Hourly sheets:
    Predicted_2023 = Res_2023 + Comm_2023 + EV_2023
    Residual_2023  = Real_2023 − Predicted_2023
    Predicted_2050_BU = Res_2050 + Comm_2050 + EV_2050
    Residual_2050  = Residual_2023 × multiplier
    Total_2050     = Predicted_2050_BU + Residual_2050

  Can run standalone:  julia src/assemble_loads.jl [config.toml]
  Or include from pipeline:  include("assemble_loads.jl"); total = run_assembly(cfg, res, comm, ev)
=#

if !@isdefined(load_config)
    include(joinpath(@__DIR__, "common.jl"))
end
if !@isdefined(run_residential)
    include(joinpath(@__DIR__, "residential.jl"))
end
if !@isdefined(run_commercial)
    include(joinpath(@__DIR__, "commercial.jl"))
end

function run_assembly(cfg::Dict, res_df::DataFrame, comm_load::Vector{Float64},
                       ev_load_2050::Vector{Float64}; verbose::Bool=true)
    verbose && println("\n▸ Assembling total load...")

    d = cfg["_derived"]
    base_dir = get(cfg["paths"], "data_dir", "data")

    # Load observed data
    real_2023_df = load_hourly_csv(joinpath(base_dir, "real_load_2023.csv"); value_col="real_mw")
    real_2023 = Float64.(real_2023_df.real_mw)

    n = length(real_2023)
    @assert n == nrow(res_df) == length(comm_load) "Row count mismatch: real=$n, res=$(nrow(res_df)), comm=$(length(comm_load))"

    datetimes = res_df.datetime

    # 2023 predicted (for residual calculation)
    # We need 2023 commercial and EV to compute predicted_2023
    # Commercial 2023 ≈ 10% of Raw Ontario Load — this is already in the Excel
    # For now, use the residual calculation: residual = real - predicted_2023
    # where predicted_2023 = total_res_2023 + commercial_2023 + ev_2023
    # We don't have separate 2023 comm/EV vectors from the extracted data,
    # but we can compute predicted_2023 from what the Excel already tells us:
    #   Total Loads col C (Predicted 2023) = Hourly_Load_Res!U + Commercial!C + EV!B
    # Since we extracted the total_res_2023 from residential.jl, and the residual
    # is real - predicted, we need the predicted.
    #
    # Simpler approach: use the pre-computed residual from the Excel.
    # OR: compute it from total_res_2023 + a comm_2023 proxy + ev_2023 proxy.
    #
    # The cleanest path: the "residual" is the difference between real observed load
    # and the bottom-up prediction. Since we're replicating the Excel, we compute:
    #   predicted_2023 = total_res_2023 + comm_2023_load + ev_2023_load
    # We can extract comm_2023 and ev_2023 from the workbook, or approximate:
    #   comm_2023 ≈ read from Commercial tab col C (already extracted? no — we only extracted 2050 scenarios)
    #   ev_2023 ≈ read from EV tab col B

    # For a fully self-contained pipeline, we need these files.
    # Let's check if they exist, and fall back to computing from the available data.

    comm_2023_path = joinpath(base_dir, "commercial_2023.csv")
    ev_2023_path = joinpath(base_dir, "ev_load_2023.csv")

    if isfile(comm_2023_path) && isfile(ev_2023_path)
        comm_2023 = Float64.(load_hourly_csv(comm_2023_path; value_col="commercial_mw").commercial_mw)
        ev_2023 = Float64.(load_hourly_csv(ev_2023_path; value_col="ev_mw").ev_mw)
    else
        @warn "commercial_2023.csv and/or ev_load_2023.csv not found in $base_dir. " *
              "Using fallback: extracting residual directly from (real − total_res_2023 − estimated_comm − estimated_ev)."

        # Fallback: use the commercial 2023 share (~10% of Ontario) and EV 2023
        # from the Excel values we know. The 2023 comm was ~0.83 TWh / 8760 ≈ ~94.7 MW avg.
        # The 2023 EV was ~0.238 TWh / 8760 ≈ ~27.2 MW avg.
        # But these are hourly profiles, not flat. Without the actual profiles,
        # we approximate using ratios from the active scenario.

        # Better fallback: compute predicted_2023 as (real - 46.5% of real),
        # since we know residual was ~46.5% of real load.
        # But that's circular.

        # Best option: just compute it directly from what we have.
        # predicted_2023 = Res_2023 + Comm_2023 + EV_2023
        # We DO have Res_2023 from residential model.
        # For Comm and EV 2023, let's scale the 2050 values by the known ratios:
        #   Comm_2023 / Comm_2050 ≈ 0.83 / 0.93 ≈ 0.89
        #   EV_2023 / EV_2050 ≈ 0.238 / 0.638 ≈ 0.373
        # These are rough but defensible for computing the residual.

        @info "  Using scaled 2050 profiles as 2023 proxy (comm×0.89, ev×0.37)"
        comm_2023 = comm_load .* 0.89
        ev_2023 = ev_load_2050 .* 0.373
    end

    total_res_2023 = Float64.(res_df.total_res_2023)
    total_res_2050 = Float64.(res_df.total_res_2050)

    # Predicted 2023
    predicted_2023 = total_res_2023 .+ comm_2023 .+ ev_2023

    # Residual 2023
    residual_2023 = real_2023 .- predicted_2023

    # Predicted 2050 bottom-up
    predicted_2050_bu = total_res_2050 .+ comm_load .+ ev_load_2050

    # Residual 2050 (three methods)
    res_mult = d["residual_multiplier"]
    residual_2050 = residual_2023 .* res_mult

    # Active total 2050
    active_total_2050 = predicted_2050_bu .+ residual_2050

    # Build output
    result = DataFrame(
        datetime             = datetimes,
        real_2023_mw         = real_2023,
        predicted_2023_mw    = predicted_2023,
        residual_2023_mw     = residual_2023,
        res_2050_mw          = total_res_2050,
        comm_2050_mw         = comm_load,
        ev_2050_mw           = ev_load_2050,
        predicted_2050_bu_mw = predicted_2050_bu,
        residual_2050_mw     = residual_2050,
        active_total_2050_mw = active_total_2050,
    )

    if verbose
        println("  ✓ Total load assembly complete")
        println(@sprintf("    Real 2023:       %.3f TWh, peak %.1f MW", compute_annual_energy_twh(real_2023), compute_peak_mw(real_2023)))
        println(@sprintf("    Predicted 2023:  %.3f TWh, peak %.1f MW", compute_annual_energy_twh(predicted_2023), compute_peak_mw(predicted_2023)))
        println(@sprintf("    Residual 2023:   %.3f TWh (%.1f%% of real)", compute_annual_energy_twh(residual_2023), sum(residual_2023)/sum(real_2023)*100))
        println(@sprintf("    BU 2050:         %.3f TWh, peak %.1f MW", compute_annual_energy_twh(predicted_2050_bu), compute_peak_mw(predicted_2050_bu)))
        println(@sprintf("    Residual 2050:   %.3f TWh (×%.4f)", compute_annual_energy_twh(residual_2050), res_mult))
        println(@sprintf("    ACTIVE TOTAL:    %.3f TWh, peak %.1f MW", compute_annual_energy_twh(active_total_2050), compute_peak_mw(active_total_2050)))
    end

    return result
end

if abspath(PROGRAM_FILE) == @__FILE__
    cfg = load_config(length(ARGS) > 0 ? ARGS[1] : "config.toml")
    println("="^70)
    println("  SESP Ottawa 2050 — Load Assembly")
    println("="^70)
    print_config_summary(cfg)

    res = run_residential(cfg)
    comm = run_commercial(cfg)

    # For standalone, we need an EV vector. Check if one exists in results.
    ev_path = joinpath(cfg["paths"]["results"], "ev_load_2050.csv")
    if isfile(ev_path)
        ev_df = load_hourly_csv(ev_path; value_col="ev_mw")
        ev_load = Float64.(ev_df.ev_mw)
    else
        @warn "No EV load file found at $ev_path. Using zeros (run ev_model.jl first)."
        ev_load = zeros(Float64, 8760)
    end

    total = run_assembly(cfg, res, comm.active_load, ev_load)

    mkpath(cfg["paths"]["results"])
    CSV.write(joinpath(cfg["paths"]["results"], "total_loads.csv"), total)
    println("\n  ✓ Saved to $(cfg["paths"]["results"])/total_loads.csv")
end
