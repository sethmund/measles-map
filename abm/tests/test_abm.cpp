// Self-contained checks for the measles ABM. Build with `make test` or
// `ctest`; a non-zero exit status means something regressed.
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "config.hpp"
#include "model.hpp"
#include "params.hpp"
#include "population.hpp"
#include "rng.hpp"

namespace {

int failures = 0;
int checks = 0;

void check(bool condition, const std::string& what) {
  ++checks;
  if (!condition) {
    ++failures;
    std::cout << "  FAIL: " << what << "\n";
  }
}

void check_near(double actual, double lo, double hi, const std::string& what) {
  ++checks;
  if (!(actual >= lo && actual <= hi)) {
    ++failures;
    std::cout << "  FAIL: " << what << " (got " << actual << ", expected " << lo << ".." << hi
              << ")\n";
  }
}

using namespace measles;

Params small_params() {
  Params p;
  p.population = 12000;
  p.days = 120;
  p.seed = 424242;
  p.initial_infections = 3;
  p.importation_per_day = 0.0;
  p.output_dir = "";
  p.write_linelist = false;
  return p;
}

Summary run_once(const Params& p) {
  Rng rng(p.seed);
  World w = build_world(p, rng);
  Model m(p, w, rng);
  m.run();
  return m.summarize();
}

void test_config() {
  std::cout << "config parsing\n";
  const std::string path = "test_config_tmp.cfg";
  {
    std::ofstream out(path);
    out << "# a comment\n\npopulation = 1234  ; trailing comment\n"
        << "coverage_dose1=0.5\nbeta.classroom = 0.02\n";
  }
  Config cfg;
  std::string err;
  check(cfg.load_file(path, err), "config file loads: " + err);
  check(cfg.get_int("population", 0) == 1234, "int value parsed");
  check(std::fabs(cfg.get_double("coverage_dose1", 0.0) - 0.5) < 1e-12, "double value parsed");
  check(cfg.get_int("missing", 77) == 77, "fallback returned for missing key");

  Params p;
  p.apply(cfg);
  check(p.population == 1234, "params pick up population");
  check(std::fabs(p.beta[static_cast<int>(GroupType::Classroom)] - 0.02) < 1e-12,
        "per-setting beta override applied");
  check(cfg.unused_keys().empty(), "all supplied keys were consumed");

  Config typo;
  check(typo.set_from_assignment("populatoin=10", err), "assignment parses");
  Params q;
  q.apply(typo);
  check(typo.unused_keys().size() == 1, "misspelled key is reported as unused");
  check(!typo.set_from_assignment("nonsense", err), "malformed assignment rejected");
  std::remove(path.c_str());
}

void test_validation() {
  std::cout << "parameter validation\n";
  Params p;
  check(p.validate().empty(), "defaults validate");
  p.population = 10;
  check(!p.validate().empty(), "tiny population rejected");
  Params q;
  q.ve_two_dose = 1.7;
  check(!q.validate().empty(), "out-of-range efficacy rejected");
  Params r;
  r.beta[static_cast<int>(GroupType::Household)] = -1.0;
  check(!r.validate().empty(), "negative beta rejected");
}

void test_population_structure() {
  std::cout << "synthetic population\n";
  Params p = small_params();
  Rng rng(p.seed);
  World w = build_world(p, rng);

  check(static_cast<int>(w.agents.size()) == p.population, "population size honoured");
  check(!w.schools.empty(), "at least one school exists");
  check(!w.hospitals.empty(), "at least one hospital exists");
  check(!w.urgent_cares.empty(), "at least one urgent care exists");
  check(!w.ltcfs.empty(), "at least one long-term care facility exists");
  check(!w.workplaces.empty(), "at least one workplace exists");
  check(!w.venues.empty(), "at least one community venue exists");

  long long students = 0, workers = 0, hcw = 0, ltcf_staff = 0, residents = 0, teachers = 0;
  for (const Agent& a : w.agents) {
    if (a.role == Role::Student) ++students;
    if (a.role == Role::Worker) ++workers;
    if (a.role == Role::HealthcareWorker) ++hcw;
    if (a.role == Role::LtcfStaff) ++ltcf_staff;
    if (a.role == Role::LtcfResident) ++residents;
    if (a.role == Role::Teacher) ++teachers;

    if (a.role == Role::Student) {
      check(a.daytime_group >= 0 && a.secondary_group >= 0, "student has classroom and school");
    }
    if (a.role == Role::LtcfResident) {
      check(a.household < 0, "long-term care resident left the private household");
      check(a.daytime_group >= 0, "resident belongs to a unit");
    } else {
      check(a.household >= 0, "everyone else lives in a household");
    }
    check(a.community_hub >= 0, "everyone has a community venue");
    check(a.home_hospital >= 0 && a.home_urgent_care >= 0, "everyone has a source of care");
  }
  check(students > 0 && workers > 0 && hcw > 0 && ltcf_staff > 0 && residents > 0 && teachers > 0,
        "all occupational roles are populated");

  // Group membership and agent pointers agree.
  for (std::size_t g = 0; g < w.groups.size(); ++g) {
    for (int member : w.groups[g].members) {
      check(member >= 0 && member < static_cast<int>(w.agents.size()), "member id in range");
    }
  }

  // Age structure should be broadly plausible for a North American community.
  long long children = 0, elderly = 0;
  for (const Agent& a : w.agents) {
    if (a.age < 18) ++children;
    if (a.age >= 65) ++elderly;
  }
  const double n = static_cast<double>(w.agents.size());
  check_near(children / n, 0.15, 0.32, "share under 18");
  check_near(elderly / n, 0.10, 0.25, "share 65 and older");

  // Baseline immunity should be high but not universal at default coverage.
  long long susceptible = 0;
  for (const Agent& a : w.agents) {
    if (a.state == HealthState::Susceptible) ++susceptible;
  }
  check_near(susceptible / n, 0.03, 0.20, "baseline susceptible fraction");
}

void test_determinism() {
  std::cout << "determinism\n";
  Params p = small_params();
  const Summary a = run_once(p);
  const Summary b = run_once(p);
  check(a.infections == b.infections && a.deaths == b.deaths &&
            a.hospitalizations == b.hospitalizations,
        "identical seeds reproduce identical runs");

  Params q = p;
  q.seed = p.seed + 1;
  const Summary c = run_once(q);
  check(a.infections != c.infections, "a different seed gives a different trajectory");
}

void test_no_transmission_when_immune() {
  std::cout << "fully immune community\n";
  Params p = small_params();
  p.coverage_dose1 = 1.0;
  p.coverage_hesitant_dose1 = 1.0;
  p.coverage_dose2_given_dose1 = 1.0;
  p.adult_1957_1989_two_dose = 1.0;
  p.adult_1957_1989_one_dose = 0.0;
  p.adult_post1989_two_dose = 1.0;
  p.pre1957_natural_immunity = 1.0;
  p.maternal_protection_under6mo = 1.0;
  p.maternal_protection_6to11mo = 1.0;
  p.ve_one_dose = 1.0;
  p.ve_two_dose = 1.0;
  const Summary s = run_once(p);
  check(s.infections == 0, "no infection can be seeded when nobody is susceptible");

  Params q = small_params();
  for (int t = 0; t < kGroupTypeCount; ++t) q.beta[t] = 0.0;
  const Summary z = run_once(q);
  check(z.infections == q.initial_infections, "zero beta means index cases infect nobody");
}

void test_epidemic_in_susceptible_population() {
  std::cout << "unprotected community\n";
  Params p = small_params();
  p.coverage_dose1 = 0.0;
  p.coverage_hesitant_dose1 = 0.0;
  p.adult_1957_1989_one_dose = 0.0;
  p.adult_1957_1989_two_dose = 0.0;
  p.adult_post1989_two_dose = 0.0;
  p.pre1957_natural_immunity = 0.0;
  p.maternal_protection_under6mo = 0.0;
  p.maternal_protection_6to11mo = 0.0;
  p.response_enabled = false;
  p.prob_case_detected = 0.0;
  p.days = 240;
  const Summary s = run_once(p);
  check_near(s.attack_rate_susceptible, 0.6, 1.0,
             "an unmitigated measles epidemic infects most susceptibles");
  check(s.peak_incidence_day > 0 && s.peak_incidence_day < s.last_case_day,
        "the epidemic peaks before it ends");
  check(s.hospitalizations > 0 && s.peak_hospital_census > 0, "admissions occur");
  long long by_setting = 0;
  for (int t = 0; t < kGroupTypeCount; ++t) by_setting += s.infections_by_setting[t];
  check(by_setting == s.infections, "setting attribution accounts for every infection");
  long long by_age = 0;
  for (int i = 0; i < 5; ++i) by_age += s.infections_by_age[i];
  check(by_age == s.infections, "age attribution accounts for every infection");
}

void test_natural_history_timeline() {
  std::cout << "natural history\n";
  Params p = small_params();
  p.initial_infections = 0;
  Rng rng(p.seed);
  World w = build_world(p, rng);
  Model m(p, w, rng);

  int index = -1;
  for (std::size_t i = 0; i < w.agents.size(); ++i) {
    if (w.agents[i].state == HealthState::Susceptible) {
      index = static_cast<int>(i);
      break;
    }
  }
  check(index >= 0, "a susceptible agent exists to infect");
  check(m.infect(index, -1, GroupType::Household, true), "infection succeeds");
  check(!m.infect(index, -1, GroupType::Household, true), "an infected agent cannot be reinfected");

  const Agent& a = w.agents[static_cast<std::size_t>(index)];
  check(a.state == HealthState::Exposed, "starts in the latent state");
  check_near(a.day_prodromal, 6, 16, "latent period length");
  check(a.day_rash - a.day_prodromal >= 3, "prodrome lasts about four days");
  check(a.day_recover - a.day_rash >= 3, "infectious for about four days after rash");

  std::vector<HealthState> seen;
  for (int d = 0; d < 30; ++d) {
    m.step();
    if (seen.empty() || seen.back() != a.state) seen.push_back(a.state);
  }
  check(a.state == HealthState::Recovered || a.state == HealthState::Dead,
        "the case resolves within a month");
  check(seen.size() >= 3, "the case passes through prodrome and rash");
}

void test_interventions_reduce_burden() {
  std::cout << "public health response\n";
  Params base = small_params();
  base.coverage_dose1 = 0.80;  // leave enough susceptibles for an outbreak
  base.coverage_hesitant_dose1 = 0.05;
  base.days = 220;

  long long with_response = 0, without_response = 0;
  const int replicates = 4;
  for (int r = 0; r < replicates; ++r) {
    Params on = base;
    on.seed = base.seed + static_cast<unsigned long long>(r);
    with_response += run_once(on).infections;

    Params off = on;
    off.response_enabled = false;
    off.school_exclusion = false;
    off.household_quarantine = false;
    off.isolation_compliance = 0.0;
    off.prob_case_detected = 0.0;
    without_response += run_once(off).infections;
  }
  check(with_response < without_response,
        "detection, isolation and outbreak vaccination reduce the final size");
}

void test_r0_is_plausible() {
  std::cout << "basic reproduction number\n";
  Params p = small_params();
  p.population = 20000;
  const R0Estimate est = estimate_r0(p, 12);
  check(est.replicates == 12, "every replicate produced an estimate");
  check_near(est.mean, 8.0, 24.0, "R0 in a fully susceptible population");
}

}  // namespace

int main() {
  test_config();
  test_validation();
  test_population_structure();
  test_determinism();
  test_no_transmission_when_immune();
  test_epidemic_in_susceptible_population();
  test_natural_history_timeline();
  test_interventions_reduce_burden();
  test_r0_is_plausible();

  std::cout << "\n" << (checks - failures) << "/" << checks << " checks passed\n";
  if (failures > 0) {
    std::cout << failures << " FAILURE(S)\n";
    return 1;
  }
  std::cout << "all good\n";
  return 0;
}
