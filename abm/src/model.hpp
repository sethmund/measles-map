// The simulation engine: daily disease progression, movement of agents between
// venues, airborne transmission, case detection, and the public health
// response.
#pragma once

#include <string>
#include <vector>

#include "params.hpp"
#include "population.hpp"
#include "rng.hpp"
#include "types.hpp"

namespace measles {

// One row of the daily time series.
struct DayRecord {
  int day = 0;
  long long susceptible = 0;
  long long exposed = 0;
  long long prodromal = 0;
  long long rash = 0;
  long long recovered = 0;
  long long dead = 0;
  long long hospitalized = 0;
  long long isolated = 0;
  long long quarantined = 0;
  long long incidence = 0;        // new infections today
  long long new_rash = 0;         // new clinical cases today
  long long new_detected = 0;     // cases reported to public health today
  long long new_admissions = 0;
  long long new_deaths = 0;
  long long doses = 0;            // outbreak-response doses given today
  long long cum_cases = 0;
  long long cum_detected = 0;
  long long cum_admissions = 0;
  long long cum_deaths = 0;
  long long incidence_by_setting[kGroupTypeCount] = {};
  bool response_active = false;
};

// One row per infection, written to the line list.
struct CaseRecord {
  int id = -1;
  int age = 0;
  Role role = Role::HomeAdult;
  ImmunityStatus immunity = ImmunityStatus::None;
  int day_infected = -1;
  int day_rash = -1;
  int infector = -1;
  int generation = 0;
  GroupType setting = GroupType::Household;
  bool hospitalized = false;
  bool died = false;
  bool detected = false;
  bool imported = false;
};

// End-of-run aggregates.
struct Summary {
  long long population = 0;
  long long susceptible_at_start = 0;
  long long infections = 0;
  long long imported = 0;
  long long detected = 0;
  long long hospitalizations = 0;
  long long deaths = 0;
  long long doses_given = 0;
  long long infections_by_setting[kGroupTypeCount] = {};
  long long infections_by_age[5] = {};      // <1, 1-4, 5-17, 18-64, 65+
  long long population_by_age[5] = {};
  long long susceptible_by_age[5] = {};
  long long infections_healthcare_workers = 0;
  long long infections_ltcf_residents = 0;
  long long infections_school_children = 0;
  long long infections_in_hesitant_households = 0;
  int peak_incidence_day = -1;
  long long peak_incidence = 0;
  int peak_hospital_census = 0;
  int hospital_beds = 0;
  int days_over_hospital_capacity = 0;
  int last_case_day = -1;
  int response_day = -1;
  double mean_generation_interval = 0.0;
  double attack_rate_susceptible = 0.0;
  double r0_effective_first_generations = 0.0;
};

class Model {
 public:
  Model(const Params& params, World& world, Rng& rng);

  // Runs the full simulation, returning when `params.days` have elapsed or the
  // outbreak has died out and no importations remain possible.
  void run();

  // Advances exactly one day (day index `day_`). Exposed for testing.
  void step();

  int current_day() const { return day_; }
  bool extinct() const;

  const std::vector<DayRecord>& history() const { return history_; }
  const std::vector<CaseRecord>& cases() const { return cases_; }
  const World& world() const { return world_; }
  Summary summarize() const;

  // Infects `agent_id` on the current day. Public so scenario code (index case
  // seeding, importation, R0 estimation) can use it.
  bool infect(int agent_id, int infector, GroupType setting, bool imported);

 private:
  struct Presence {
    int agent;
    float weight;  // share of the block spent in this group
  };

  void progress_disease();
  void handle_detection(int agent_id);
  void apply_case_control(const Agent& source);
  void offer_pep(Agent& contact);
  void run_response();
  void plan_care_seeking();
  void schedule_routine_visits();
  void run_block(Block block);
  void build_presence(Block block, bool weekend);
  void add_presence(int group_id, int agent_id, float weight);
  void transmit(Block block);
  void seed_importations();
  void record_day();

  double infectiousness_of(const Agent& a, GroupType where) const;
  double beta_for(GroupType type) const;
  bool at_work_today(const Agent& a, bool weekend);
  int home_group(const Agent& a) const;

  const Params& p_;
  World& world_;
  Rng& rng_;

  int day_ = 0;
  bool keep_today_ = false;   // day 0 carries the seeded index cases
  bool response_active_ = false;
  int response_day_ = -1;
  double response_intensity_ = 0.0;

  std::vector<std::vector<Presence>> presence_;
  std::vector<int> active_groups_;
  std::vector<char> group_active_;

  std::vector<int> catchup_queue_;   // shuffled susceptible unvaccinated agents
  std::size_t catchup_cursor_ = 0;
  std::vector<std::vector<int>> pending_immunity_;  // day -> agents seroconverting

  std::vector<CaseRecord> cases_;
  std::vector<int> case_index_;      // agent id -> index into cases_
  std::vector<DayRecord> history_;

  DayRecord today_;
  long long cum_cases_ = 0;
  long long cum_detected_ = 0;
  long long cum_admissions_ = 0;
  long long cum_deaths_ = 0;
  long long doses_total_ = 0;
  long long imported_total_ = 0;
  int peak_census_ = 0;
  int days_over_capacity_ = 0;
  int last_case_day_ = -1;
};

// Estimates the basic reproduction number implied by the current parameters by
// seeding single index cases into an otherwise fully susceptible copy of the
// community and counting their direct offspring.
struct R0Estimate {
  double mean = 0.0;
  double sd = 0.0;
  int replicates = 0;
  double by_setting[kGroupTypeCount] = {};
};
R0Estimate estimate_r0(const Params& params, int replicates);

}  // namespace measles
