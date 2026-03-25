#!/usr/bin/env julia
#=
  commercial.jl — Commercial Load Loader
  ───────────────────────────────────────
  Reads the 5 commercial scenario CSVs produced by the buildings team
  EnergyPlus retrofit model. Returns the active scenario as a vector.

  Can run standalone:  julia src/commercial.jl [config.toml]
  Or include from pipeline:  include("commercial.jl"); comm = run_commercial(cfg)
=#

if !@isdefined(load_config)
    include(joinpath(@__DIR__, "common.jl"))
end

const COMMERCIAL_SCENARIOS = ["NG", "NRG", "CNG", "CNRG", "CST"]

function run_commercial(cfg::Dict; verbose::Bool=true)
    verbose && println("\n▸ Loading commercial load scenarios...")

    comm_dir = cfg["paths"]["commercial_dir"]
    active = cfg["scenarios"]["active_commercial"]
    active in COMMERCIAL_SCENARIOS || error("Unknown commercial scenario: $active. Options: $COMMERCIAL_SCENARIOS")

    # Load all scenarios
    scenarios = Dict{String, DataFrame}()
    for s in COMMERCIAL_SCENARIOS
        path = joinpath(comm_dir, "comm_2050_$(s).csv")
        isfile(path) || (@warn "Missing: $path"; continue)
        scenarios[s] = load_hourly_csv(path; value_col="commercial_mw")
    end

    active_df = scenarios[active]

    if verbose
        println("  ✓ Loaded $(length(scenarios)) commercial scenarios")
        for (name, df) in sort(collect(scenarios); by=first)
            vals = Float64.(df.commercial_mw)
            marker = name == active ? " ← ACTIVE" : ""
            println(@sprintf("    %-5s: %.3f TWh, peak %.1f MW%s",
                name, compute_annual_energy_twh(vals), compute_peak_mw(vals), marker))
        end
    end

    return (
        active_load = Float64.(active_df.commercial_mw),
        active_name = active,
        datetime = active_df.datetime,
        all_scenarios = scenarios,
    )
end

if abspath(PROGRAM_FILE) == @__FILE__
    cfg = load_config(length(ARGS) > 0 ? ARGS[1] : "config.toml")
    println("="^70)
    println("  SESP Ottawa 2050 — Commercial Load")
    println("="^70)
    print_config_summary(cfg)
    run_commercial(cfg)
end
