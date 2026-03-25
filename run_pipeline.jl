#!/usr/bin/env julia
#=
  run_pipeline.jl — SESP Ottawa 2050 Main Pipeline
  ─────────────────────────────────────────────────
  Runs all modules in order to produce the full 2050 load projection
  and FSA disaggregation from a single command.

  Usage:  julia src/run_pipeline.jl [config.toml]

  Steps:
    1. Load config and compute derived parameters
    2. Run residential model (heating / cooling / plug)
    3. Load commercial scenarios
    4. Load EV profile (pre-computed by ev_model.jl — must run separately)
    5. Assemble total load = Res + Comm + EV + scaled Residual
    6. Load FSA HourMonth weights (pre-computed by fsa_weights.jl — must run separately)
    7. Disaggregate total load to 45 FSAs
    8. Write outputs: CSVs + analysis summary

  Prerequisites (run once, outputs are cached):
    julia src/fsa_weights.jl     # trains weights, needs internet for IESO data
    julia src/ev_model.jl        # EV simulation, needs data/ev_inputs/

  All other modules run inline from this script.
=#

include(joinpath(@__DIR__, "common.jl"))
include(joinpath(@__DIR__, "residential.jl"))
include(joinpath(@__DIR__, "commercial.jl"))
include(joinpath(@__DIR__, "assemble_loads.jl"))

config_path = length(ARGS) > 0 ? ARGS[1] : "config.toml"
cfg = load_config(config_path)

println("="^70)
println("  SESP Ottawa 2050 — Full Pipeline")
println("  $(Dates.now())")
println("="^70)
print_config_summary(cfg)

out_dir = cfg["paths"]["results"]
mkpath(out_dir)

# ══════════════════════════════════════════════════════════════
#  STEP 1: Residential model
# ══════════════════════════════════════════════════════════════

res = run_residential(cfg)
CSV.write(joinpath(out_dir, "residential_hourly.csv"), res)

# ══════════════════════════════════════════════════════════════
#  STEP 2: Commercial load
# ══════════════════════════════════════════════════════════════

comm = run_commercial(cfg)

# ══════════════════════════════════════════════════════════════
#  STEP 3: EV load (pre-computed)
# ══════════════════════════════════════════════════════════════

println("\n▸ Loading EV profile...")

ev_path = joinpath(out_dir, "ev_load_2050.csv")
if !isfile(ev_path)
    # Try alternate locations
    alt_paths = ["ev_load_2050_MW.csv", "ev_load_2025_MW.csv"]
    for p in alt_paths
        if isfile(p)
            ev_path = p
            break
        end
    end
end

if isfile(ev_path)
    ev_df = CSV.read(ev_path, DataFrame; silencewarnings=true)
    # Handle different column names
    ev_col = nothing
    for c in names(ev_df)
        cl = lowercase(string(c))
        if contains(cl, "ev") && contains(cl, "mw")
            ev_col = c; break
        end
        if contains(cl, "load") || contains(cl, "mw")
            ev_col = c; break
        end
    end
    ev_col === nothing && (ev_col = names(ev_df)[end])
    ev_load_2050 = Float64.(ev_df[!, ev_col])
    @assert length(ev_load_2050) == 8760 "EV load must be 8760 hours, got $(length(ev_load_2050))"
    println("  ✓ Loaded EV profile from $ev_path")
    println(@sprintf("    %.3f TWh, peak %.1f MW",
        compute_annual_energy_twh(ev_load_2050), compute_peak_mw(ev_load_2050)))
else
    @warn "No EV load file found. Using zeros. Run src/ev_model.jl first."
    ev_load_2050 = zeros(Float64, 8760)
end

# ══════════════════════════════════════════════════════════════
#  STEP 4: Assemble total load
# ══════════════════════════════════════════════════════════════

total = run_assembly(cfg, res, comm.active_load, ev_load_2050)
CSV.write(joinpath(out_dir, "total_loads.csv"), total)

# ══════════════════════════════════════════════════════════════
#  STEP 5: FSA disaggregation
# ══════════════════════════════════════════════════════════════

println("\n▸ Disaggregating to FSAs...")

weights_dir = cfg["paths"]["weights_dir"]
if !isdir(weights_dir)
    @warn "Weights directory not found: $weights_dir. Run src/fsa_weights.jl first. Skipping FSA disaggregation."
else
    # Load HourMonth weights
    weight_files = filter(f -> endswith(lowercase(f), ".csv"), readdir(weights_dir; join=true))
    fsa_weights = Dict{String, Matrix{Float64}}()
    for path in weight_files
        fsa = uppercase(splitext(basename(path))[1])
        df = CSV.read(path, DataFrame; silencewarnings=true)
        mat = zeros(Float64, 24, 12)
        for row in eachrow(df)
            h = Int(row.Hour)
            for m in 1:12
                col = Symbol("M$m")
                hasproperty(row, col) && (mat[h+1, m] = Float64(row[col]))
            end
        end
        fsa_weights[fsa] = mat
    end
    fsa_list = sort(collect(keys(fsa_weights)))
    println("  Loaded weights for $(length(fsa_list)) FSAs")

    # Disaggregate
    active_total = Float64.(total.active_total_2050_mw)
    datetimes = total.datetime

    fsa_df = DataFrame(datetime=datetimes, active_total_mw=active_total)
    for fsa in fsa_list
        fsa_load = Vector{Float64}(undef, 8760)
        for i in 1:8760
            h  = Dates.hour(datetimes[i])
            mo = Dates.month(datetimes[i])
            fsa_load[i] = fsa_weights[fsa][h+1, mo] * active_total[i]
        end
        fsa_df[!, fsa] = fsa_load
    end

    CSV.write(joinpath(out_dir, "fsa_loads_2050.csv"), fsa_df)
    println("  ✓ FSA disaggregation: $(nrow(fsa_df)) hours × $(length(fsa_list)) FSAs")
    println("    Saved to $(out_dir)/fsa_loads_2050.csv")

    # Spot check: verify weights sum to ~1.0
    sample_sum = sum(fsa_df[1, fsa] for fsa in fsa_list) / active_total[1]
    println(@sprintf("    Weight sum check (hour 1): %.4f", sample_sum))
end

# ══════════════════════════════════════════════════════════════
#  STEP 6: Analysis summary
# ══════════════════════════════════════════════════════════════

println("\n▸ Generating analysis summary...")

d = cfg["_derived"]
real_2023 = Float64.(total.real_2023_mw)
at_2050   = Float64.(total.active_total_2050_mw)

summary_rows = [
    ("2023 Res Heating",   compute_annual_energy_twh(Float64.(res.heat_elec_2023)), "TWh"),
    ("2023 Res Cooling",   compute_annual_energy_twh(Float64.(res.cool_2023_mw)),   "TWh"),
    ("2023 Res Plug",      compute_annual_energy_twh(Float64.(res.plug_2023_mw)),    "TWh"),
    ("2023 Commercial",    compute_annual_energy_twh(Float64.(total.predicted_2023_mw) .- Float64.(res.total_res_2023)), "TWh"),
    ("2023 Total Real",    compute_annual_energy_twh(real_2023), "TWh"),
    ("2023 Residual",      compute_annual_energy_twh(Float64.(total.residual_2023_mw)), "TWh"),
    ("2023 Residual %",    sum(Float64.(total.residual_2023_mw))/sum(real_2023)*100, "%"),
    ("","",""),
    ("2050 Res Heating",   compute_annual_energy_twh(Float64.(res.heat_elec_2050)), "TWh"),
    ("2050 Res Cooling",   compute_annual_energy_twh(Float64.(res.cool_2050_mw)),   "TWh"),
    ("2050 Res Plug",      compute_annual_energy_twh(Float64.(res.plug_2050_mw)),    "TWh"),
    ("2050 Commercial",    compute_annual_energy_twh(comm.active_load), "TWh"),
    ("2050 EV",            compute_annual_energy_twh(ev_load_2050), "TWh"),
    ("2050 Residual",      compute_annual_energy_twh(Float64.(total.residual_2050_mw)), "TWh"),
    ("2050 Total",         compute_annual_energy_twh(at_2050), "TWh"),
    ("","",""),
    ("2050 Peak Total MW", compute_peak_mw(at_2050), "MW"),
    ("2050 Avg Load MW",   mean(at_2050), "MW"),
    ("2050/2023 Ratio",    sum(at_2050)/sum(real_2023), "×"),
]

println("\n" * "="^55)
println("  ANALYSIS SUMMARY")
println("="^55)
for (label, val, unit) in summary_rows
    if label == ""
        println("  " * "-"^50)
    else
        println(@sprintf("  %-25s  %10.3f  %s", label, val, unit))
    end
end
println("="^55)

analysis_df = DataFrame(
    metric = [r[1] for r in summary_rows if r[1] != ""],
    value  = [r[2] for r in summary_rows if r[1] != ""],
    unit   = [r[3] for r in summary_rows if r[1] != ""],
)
CSV.write(joinpath(out_dir, "analysis_summary.csv"), analysis_df)

# ══════════════════════════════════════════════════════════════
#  DONE
# ══════════════════════════════════════════════════════════════

println("\n" * "="^70)
println("  ✓ Pipeline complete. All outputs in: $out_dir/")
println("="^70)
out_files = filter(f -> endswith(f, ".csv"), readdir(out_dir))
for f in sort(out_files)
    sz = round(filesize(joinpath(out_dir, f)) / 1e3, digits=1)
    println("    $f  ($(sz) KB)")
end
println("="^70)
