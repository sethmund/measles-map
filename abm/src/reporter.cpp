#include "reporter.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace measles {
namespace {

const char* kAgeGroups[5] = {"<1", "1-4", "5-17", "18-64", "65+"};

std::string pct(double x, int digits = 1) {
  std::ostringstream os;
  os.setf(std::ios::fixed);
  os << std::setprecision(digits) << (100.0 * x) << "%";
  return os.str();
}

}  // namespace

bool ensure_directory(const std::string& dir, std::string& error) {
  if (dir.empty()) return true;
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  if (ec && !std::filesystem::exists(dir)) {
    error = "cannot create output directory '" + dir + "': " + ec.message();
    return false;
  }
  return true;
}

bool write_text(const std::string& path, const std::string& body, std::string& error) {
  std::ofstream out(path);
  if (!out) {
    error = "cannot write " + path;
    return false;
  }
  out << body;
  return true;
}

bool write_timeseries(const std::string& path, const std::vector<DayRecord>& history,
                      std::string& error) {
  std::ofstream out(path);
  if (!out) {
    error = "cannot write " + path;
    return false;
  }
  out << "day,susceptible,exposed,prodromal,rash,infectious,immune,dead,hospitalized,"
         "isolated,quarantined,new_infections,new_rash,new_detected,new_admissions,new_deaths,"
         "doses,cum_infections,cum_detected,cum_admissions,cum_deaths,response_active";
  for (int t = 0; t < kGroupTypeCount; ++t) {
    out << ",infections_" << group_type_name(static_cast<GroupType>(t));
  }
  out << "\n";
  for (const DayRecord& r : history) {
    out << r.day << ',' << r.susceptible << ',' << r.exposed << ',' << r.prodromal << ','
        << r.rash << ',' << (r.prodromal + r.rash) << ',' << r.recovered << ',' << r.dead << ','
        << r.hospitalized << ',' << r.isolated << ',' << r.quarantined << ',' << r.incidence << ','
        << r.new_rash << ',' << r.new_detected << ',' << r.new_admissions << ',' << r.new_deaths
        << ',' << r.doses << ',' << r.cum_cases << ',' << r.cum_detected << ','
        << r.cum_admissions << ',' << r.cum_deaths << ',' << (r.response_active ? 1 : 0);
    for (int t = 0; t < kGroupTypeCount; ++t) out << ',' << r.incidence_by_setting[t];
    out << "\n";
  }
  return true;
}

bool write_linelist(const std::string& path, const Model& model, std::string& error) {
  std::ofstream out(path);
  if (!out) {
    error = "cannot write " + path;
    return false;
  }
  out << "case_id,agent_id,age,role,immunity,day_infected,day_rash,infector,generation,"
         "acquired_in,hospitalized,died,detected,imported,hesitant_household,"
         "secondary_infections\n";
  const World& w = model.world();
  int n = 0;
  for (const CaseRecord& c : model.cases()) {
    const Agent& a = w.agents[static_cast<std::size_t>(c.id)];
    out << n++ << ',' << c.id << ',' << c.age << ',' << role_name(c.role) << ','
        << immunity_name(c.immunity) << ',' << c.day_infected << ',' << c.day_rash << ','
        << c.infector << ',' << c.generation << ',' << group_type_name(c.setting) << ','
        << (c.hospitalized ? 1 : 0) << ',' << (c.died ? 1 : 0) << ',' << (c.detected ? 1 : 0)
        << ',' << (c.imported ? 1 : 0) << ',' << (a.hesitant_household ? 1 : 0) << ','
        << a.secondary_infections << "\n";
  }
  return true;
}

bool write_setting_table(const std::string& path, const Summary& s, std::string& error) {
  std::ofstream out(path);
  if (!out) {
    error = "cannot write " + path;
    return false;
  }
  out << "setting,infections,share\n";
  const double total = std::max<long long>(1, s.infections);
  for (int t = 0; t < kGroupTypeCount; ++t) {
    out << group_type_name(static_cast<GroupType>(t)) << ',' << s.infections_by_setting[t] << ','
        << (s.infections_by_setting[t] / total) << "\n";
  }
  return true;
}

bool write_age_table(const std::string& path, const Summary& s, std::string& error) {
  std::ofstream out(path);
  if (!out) {
    error = "cannot write " + path;
    return false;
  }
  out << "age_group,population,infections,incidence_per_1000,never_infected_susceptible\n";
  for (int i = 0; i < 5; ++i) {
    const double denom = std::max<long long>(1, s.population_by_age[i]);
    out << kAgeGroups[i] << ',' << s.population_by_age[i] << ',' << s.infections_by_age[i] << ','
        << (1000.0 * s.infections_by_age[i] / denom) << ',' << s.susceptible_by_age[i] << "\n";
  }
  return true;
}

std::string format_summary(const Summary& s, const Params& p) {
  std::ostringstream os;
  os.setf(std::ios::fixed);
  const double pop = static_cast<double>(std::max<long long>(1, s.population));

  os << "\n=== Outbreak summary ===\n";
  os << "  population                : " << s.population << "\n";
  os << "  susceptible at baseline   : " << s.susceptible_at_start << " ("
     << pct(s.susceptible_at_start / pop) << " of the community)\n";
  os << "  infections                : " << s.infections << " (" << s.imported
     << " imported/seeded)\n";
  os << "  attack rate (susceptibles): " << pct(s.attack_rate_susceptible) << "\n";
  os << "  reported cases            : " << s.detected << "\n";
  os << "  hospital admissions       : " << s.hospitalizations << "  peak inpatient census "
     << s.peak_hospital_census << " of " << s.hospital_beds << " hospital beds\n";
  if (s.days_over_hospital_capacity > 0) {
    os << "  days a hospital was over capacity: " << s.days_over_hospital_capacity << "\n";
  }
  os << "  deaths                    : " << s.deaths << "\n";
  os << "  outbreak-response doses   : " << s.doses_given;
  if (s.response_day >= 0) os << " (response began on day " << s.response_day << ")";
  else os << " (response never triggered)";
  os << "\n";
  os << "  peak daily incidence      : " << s.peak_incidence << " on day " << s.peak_incidence_day
     << "\n";
  os << "  last infection on day     : " << s.last_case_day << " of " << p.days << "\n";
  os << std::setprecision(2);
  os << "  mean offspring, first 3 generations: " << s.r0_effective_first_generations << "\n";
  os << "  mean generation interval  : " << s.mean_generation_interval << " days\n";

  os << "\n  Where infections happened\n";
  std::vector<std::pair<long long, int>> ranked;
  for (int t = 0; t < kGroupTypeCount; ++t) ranked.push_back({s.infections_by_setting[t], t});
  std::sort(ranked.rbegin(), ranked.rend());
  const double total = static_cast<double>(std::max<long long>(1, s.infections));
  for (const auto& kv : ranked) {
    if (kv.first == 0) continue;
    os << "    " << std::setw(18) << std::left << group_type_name(static_cast<GroupType>(kv.second))
       << std::right << std::setw(7) << kv.first << "  " << pct(kv.first / total) << "\n";
  }

  os << "\n  Who was infected\n";
  for (int i = 0; i < 5; ++i) {
    const double denom = std::max<long long>(1, s.population_by_age[i]);
    os << "    age " << std::setw(6) << std::left << kAgeGroups[i] << std::right << std::setw(7)
       << s.infections_by_age[i] << "  " << std::setprecision(1)
       << (1000.0 * s.infections_by_age[i] / denom) << " per 1000\n";
    os.precision(2);
  }
  os << "    health-care workers    : " << s.infections_healthcare_workers << "\n";
  os << "    long-term care residents: " << s.infections_ltcf_residents << "\n";
  os << "    school/daycare children : " << s.infections_school_children << "\n";
  os << "    in vaccine-hesitant households: " << s.infections_in_hesitant_households << " ("
     << pct(s.infections_in_hesitant_households / total) << " of all infections)\n";
  return os.str();
}

}  // namespace measles
