// measles_abm - an agent-based model of measles transmission in a community.
//
//   measles_abm                            run the baseline scenario
//   measles_abm --config config/low_coverage.cfg
//   measles_abm --mode r0 --replicates 30  check the implied R0
//   measles_abm --mode sweep --sweep coverage_dose1=0.95,0.90,0.85
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "config.hpp"
#include "model.hpp"
#include "params.hpp"
#include "population.hpp"
#include "reporter.hpp"
#include "rng.hpp"

namespace {

using namespace measles;

void print_usage() {
  std::cout <<
      "measles_abm - agent-based measles transmission model\n"
      "\n"
      "Usage: measles_abm [options]\n"
      "\n"
      "  --config PATH        read parameters from a key=value file\n"
      "  --set KEY=VALUE      override one parameter (repeatable)\n"
      "  --seed N             random seed\n"
      "  --days N             days to simulate\n"
      "  --pop N              community size\n"
      "  --out DIR            output directory (default: results)\n"
      "  --mode MODE          run (default) | r0 | sweep\n"
      "  --replicates N       replicates for r0 / sweep modes\n"
      "  --sweep KEY=V1,V2,.. parameter values to sweep (mode sweep)\n"
      "  --no-linelist        skip writing the per-case line list\n"
      "  --quiet              only print the final summary\n"
      "  --help               show this message\n"
      "\n"
      "Any parameter accepted in a config file can also be given with --set,\n"
      "for example --set beta.classroom=0.012 --set coverage_dose1=0.80.\n";
}

bool next_value(int argc, char** argv, int& i, const char* flag, std::string& out) {
  if (i + 1 >= argc) {
    std::cerr << "error: " << flag << " needs a value\n";
    return false;
  }
  out = argv[++i];
  return true;
}

std::vector<std::string> split(const std::string& s, char sep) {
  std::vector<std::string> parts;
  std::string current;
  std::istringstream in(s);
  while (std::getline(in, current, sep)) parts.push_back(current);
  return parts;
}

int run_simulation(const Params& p, bool quiet) {
  const auto t0 = std::chrono::steady_clock::now();
  Rng rng(p.seed);
  World world = build_world(p, rng);
  if (!quiet) std::cout << describe_world(world, p);

  Model model(p, world, rng);
  model.run();
  const Summary s = model.summarize();

  std::string error;
  if (!ensure_directory(p.output_dir, error)) {
    std::cerr << "error: " << error << "\n";
    return 1;
  }
  const std::string base = p.output_dir.empty() ? std::string() : p.output_dir + "/";
  const std::string report = describe_world(world, p) + format_summary(s, p);
  bool ok = write_timeseries(base + "timeseries.csv", model.history(), error) &&
            write_setting_table(base + "infections_by_setting.csv", s, error) &&
            write_age_table(base + "infections_by_age.csv", s, error) &&
            write_text(base + "summary.txt", report, error);
  if (ok && p.write_linelist) ok = write_linelist(base + "linelist.csv", model, error);
  if (!ok) {
    std::cerr << "error: " << error << "\n";
    return 1;
  }

  std::cout << format_summary(s, p);
  const auto t1 = std::chrono::steady_clock::now();
  const double secs = std::chrono::duration<double>(t1 - t0).count();
  std::cout << "\nWrote timeseries.csv, infections_by_setting.csv, infections_by_age.csv"
            << (p.write_linelist ? ", linelist.csv" : "") << " and summary.txt to "
            << (p.output_dir.empty() ? "." : p.output_dir) << " in " << std::fixed
            << std::setprecision(1) << secs << "s\n";
  return 0;
}

int run_r0(const Params& p, int replicates) {
  std::cout << "Estimating R0: " << replicates
            << " index cases in a fully susceptible community of " << p.population << "...\n";
  const R0Estimate est = estimate_r0(p, replicates);
  std::cout << std::fixed << std::setprecision(2);
  std::cout << "  R0 = " << est.mean << " (sd " << est.sd << ", n = " << est.replicates << ")\n";
  std::cout << "  Secondary infections per index case, by setting:\n";
  for (int t = 0; t < kGroupTypeCount; ++t) {
    if (est.by_setting[t] <= 0.0) continue;
    std::cout << "    " << std::setw(18) << std::left
              << group_type_name(static_cast<GroupType>(t)) << std::right << std::setw(7)
              << est.by_setting[t] << "\n";
  }
  std::cout << "\n  Measles R0 is usually quoted as 12-18 in a fully susceptible population.\n";
  return 0;
}

int run_sweep(const Params& base, const std::string& spec, int replicates) {
  const std::size_t eq = spec.find('=');
  if (eq == std::string::npos) {
    std::cerr << "error: --sweep expects KEY=V1,V2,...\n";
    return 1;
  }
  const std::string key = spec.substr(0, eq);
  const std::vector<std::string> values = split(spec.substr(eq + 1), ',');
  if (values.empty()) {
    std::cerr << "error: --sweep needs at least one value\n";
    return 1;
  }

  std::string error;
  if (!ensure_directory(base.output_dir, error)) {
    std::cerr << "error: " << error << "\n";
    return 1;
  }
  std::ostringstream csv;
  csv << key << ",replicate,seed,infections,attack_rate_susceptible,hospitalizations,deaths,"
      << "peak_incidence,peak_day,doses,susceptible_at_baseline\n";

  std::cout << std::fixed;
  std::cout << "\nSweeping " << key << " over " << values.size() << " values, " << replicates
            << " replicate(s) each\n\n";
  std::cout << std::setw(14) << std::left << key << std::right << std::setw(12) << "infections"
            << std::setw(12) << "attack%" << std::setw(10) << "hosp" << std::setw(9) << "deaths"
            << std::setw(15) << "P(>50 cases)" << "\n";

  for (const std::string& value : values) {
    double sum_inf = 0.0, sum_ar = 0.0, sum_hosp = 0.0, sum_deaths = 0.0, big = 0.0;
    for (int r = 0; r < replicates; ++r) {
      Config over;
      std::string err;
      if (!over.set_from_assignment(key + "=" + value, err)) {
        std::cerr << "error: " << err << "\n";
        return 1;
      }
      Params p = base;
      p.apply(over);
      p.seed = base.seed + 7919ULL * static_cast<unsigned long long>(r);
      Rng rng(p.seed);
      World world = build_world(p, rng);
      Model model(p, world, rng);
      model.run();
      const Summary s = model.summarize();
      sum_inf += static_cast<double>(s.infections);
      sum_ar += s.attack_rate_susceptible;
      sum_hosp += static_cast<double>(s.hospitalizations);
      sum_deaths += static_cast<double>(s.deaths);
      if (s.infections > 50) big += 1.0;
      csv << value << ',' << r << ',' << p.seed << ',' << s.infections << ','
          << s.attack_rate_susceptible << ',' << s.hospitalizations << ',' << s.deaths << ','
          << s.peak_incidence << ',' << s.peak_incidence_day << ',' << s.doses_given << ','
          << s.susceptible_at_start << "\n";
    }
    const double n = std::max(1, replicates);
    std::cout << std::setw(14) << std::left << value << std::right << std::setprecision(0)
              << std::setw(12) << (sum_inf / n) << std::setprecision(1) << std::setw(12)
              << (100.0 * sum_ar / n) << std::setprecision(0) << std::setw(10) << (sum_hosp / n)
              << std::setw(9) << (sum_deaths / n) << std::setprecision(2) << std::setw(15)
              << (big / n) << "\n";
  }

  const std::string path =
      (base.output_dir.empty() ? std::string() : base.output_dir + "/") + "sweep.csv";
  if (!write_text(path, csv.str(), error)) {
    std::cerr << "error: " << error << "\n";
    return 1;
  }
  std::cout << "\nWrote " << path << "\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg;
  std::string mode = "run";
  std::string sweep_spec;
  int replicates = 20;
  bool no_linelist = false;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    std::string value;
    std::string error;
    if (arg == "--help" || arg == "-h") {
      print_usage();
      return 0;
    } else if (arg == "--config") {
      if (!next_value(argc, argv, i, "--config", value)) return 2;
      if (!cfg.load_file(value, error)) {
        std::cerr << "error: " << error << "\n";
        return 2;
      }
    } else if (arg == "--set") {
      if (!next_value(argc, argv, i, "--set", value)) return 2;
      if (!cfg.set_from_assignment(value, error)) {
        std::cerr << "error: " << error << "\n";
        return 2;
      }
    } else if (arg == "--seed") {
      if (!next_value(argc, argv, i, "--seed", value)) return 2;
      cfg.set("seed", value);
    } else if (arg == "--days") {
      if (!next_value(argc, argv, i, "--days", value)) return 2;
      cfg.set("days", value);
    } else if (arg == "--pop" || arg == "--population") {
      if (!next_value(argc, argv, i, arg.c_str(), value)) return 2;
      cfg.set("population", value);
    } else if (arg == "--out") {
      if (!next_value(argc, argv, i, "--out", value)) return 2;
      cfg.set("output_dir", value);
    } else if (arg == "--mode") {
      if (!next_value(argc, argv, i, "--mode", mode)) return 2;
    } else if (arg == "--replicates") {
      if (!next_value(argc, argv, i, "--replicates", value)) return 2;
      replicates = std::atoi(value.c_str());
    } else if (arg == "--sweep") {
      if (!next_value(argc, argv, i, "--sweep", sweep_spec)) return 2;
    } else if (arg == "--no-linelist") {
      no_linelist = true;
    } else if (arg == "--quiet") {
      cfg.set("quiet", "1");
    } else {
      std::cerr << "error: unknown argument '" << arg << "' (try --help)\n";
      return 2;
    }
  }

  Params params;
  params.apply(cfg);
  if (no_linelist) params.write_linelist = false;
  const std::string problem = params.validate();
  if (!problem.empty()) {
    std::cerr << "error: invalid parameters: " << problem << "\n";
    return 2;
  }
  for (const std::string& key : cfg.unused_keys()) {
    std::cerr << "warning: unrecognised parameter '" << key << "' was ignored\n";
  }
  if (replicates < 1) {
    std::cerr << "error: --replicates must be at least 1\n";
    return 2;
  }

  if (mode == "run") return run_simulation(params, params.quiet);
  if (mode == "r0") return run_r0(params, replicates);
  if (mode == "sweep") {
    if (sweep_spec.empty()) {
      std::cerr << "error: --mode sweep requires --sweep KEY=V1,V2,...\n";
      return 2;
    }
    return run_sweep(params, sweep_spec, replicates);
  }
  std::cerr << "error: unknown mode '" << mode << "' (run, r0 or sweep)\n";
  return 2;
}
