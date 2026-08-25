#include "model.hpp"

#include <algorithm>
#include <cmath>

namespace measles {
namespace {

int age_bucket(int age) {
  if (age < 1) return 0;
  if (age < 5) return 1;
  if (age < 18) return 2;
  if (age < 65) return 3;
  return 4;
}

inline int gi(GroupType t) { return static_cast<int>(t); }

}  // namespace

Model::Model(const Params& params, World& world, Rng& rng)
    : p_(params), world_(world), rng_(rng) {
  presence_.resize(world_.groups.size());
  group_active_.assign(world_.groups.size(), 0);
  active_groups_.reserve(world_.groups.size());
  pending_immunity_.resize(static_cast<std::size_t>(p_.days) + 40);
  case_index_.assign(world_.agents.size(), -1);
  history_.reserve(static_cast<std::size_t>(p_.days));
}

double Model::beta_for(GroupType type) const {
  double b = p_.beta[gi(type)];
  if (!response_active_) return b;
  // Once measles is recognised in the community, health care switches to
  // airborne precautions (masking, negative pressure, triage away from the
  // waiting room) and long-term care restricts communal activity.
  switch (type) {
    case GroupType::HospitalWaiting:
    case GroupType::HospitalWard:
    case GroupType::UrgentCareWaiting:
      return b * (1.0 - response_intensity_ * (1.0 - p_.airborne_precaution_reduction));
    case GroupType::LtcfUnit:
    case GroupType::LtcfCommon:
      return b * (1.0 - response_intensity_ * (1.0 - p_.ltcf_lockdown_reduction));
    default:
      return b;
  }
}

double Model::infectiousness_of(const Agent& a, GroupType where) const {
  double f = (a.state == HealthState::Prodromal) ? p_.infectiousness_prodromal
                                                 : p_.infectiousness_rash;
  if (a.isolated && where == GroupType::Household) f *= p_.isolation_household_reduction;
  // A recognised case in hospital is nursed under airborne isolation.
  if (a.hospitalized && a.detected) f *= p_.airborne_precaution_reduction;
  return f;
}

int Model::home_group(const Agent& a) const {
  if (a.household >= 0) {
    return world_.household_group_of[static_cast<std::size_t>(a.household)];
  }
  return a.daytime_group;  // long-term care residents live in their unit
}

bool Model::infect(int agent_id, int infector, GroupType setting, bool imported) {
  Agent& a = world_.agents[static_cast<std::size_t>(agent_id)];
  if (a.state != HealthState::Susceptible) return false;

  a.state = HealthState::Exposed;
  a.day_exposed = day_;
  const int latent = static_cast<int>(std::lround(rng_.normal_trunc(
      p_.latent_days_mean, p_.latent_days_sd, p_.latent_days_min, p_.latent_days_max)));
  a.day_prodromal = day_ + std::max(1, latent);
  a.day_rash = a.day_prodromal + std::max(1, static_cast<int>(std::lround(p_.prodrome_days)));
  a.day_recover = a.day_rash + std::max(1, static_cast<int>(std::lround(p_.rash_infectious_days)));
  a.infector = infector;
  a.acquired_in = setting;

  // Severity rises at both ends of the age range.
  double hosp = p_.hosp_prob_20to64;
  switch (age_bucket(a.age)) {
    case 0: hosp = p_.hosp_prob_under1; break;
    case 1: hosp = p_.hosp_prob_1to4; break;
    case 2: hosp = p_.hosp_prob_5to19; break;
    case 3: hosp = p_.hosp_prob_20to64; break;
    default: hosp = p_.hosp_prob_65plus; break;
  }
  // A dose that failed to take still tends to blunt the clinical course.
  if (a.immunity == ImmunityStatus::OneDose || a.immunity == ImmunityStatus::TwoDose) {
    hosp *= p_.hosp_prob_vaccinated_mult;
  }
  a.severe = rng_.bernoulli(hosp);
  if (a.severe) {
    a.assigned_hospital = a.home_hospital;
    a.day_admitted = a.day_rash + rng_.uniform_int(1, 3);
    a.day_discharge = a.day_admitted +
                      std::max(1, static_cast<int>(std::lround(rng_.normal_trunc(
                                      p_.hospital_stay_days, 2.0, 1.0, 30.0))));
    double cfr = p_.cfr_20to64;
    const int b = age_bucket(a.age);
    if (b <= 1) cfr = p_.cfr_under5;
    else if (b == 2) cfr = p_.cfr_5to19;
    else if (b == 3) cfr = p_.cfr_20to64;
    else cfr = p_.cfr_65plus;
    // The case fatality ratio applies to all cases; deaths occur among the
    // severe ones, so rescale by the probability of being severe.
    const double conditional = (hosp > 0.0) ? std::min(1.0, cfr / hosp) : 0.0;
    a.will_die = rng_.bernoulli(conditional);
  }

  if (rng_.bernoulli(p_.prob_case_detected)) {
    a.detected = true;
    a.day_detected = a.day_rash + static_cast<int>(std::lround(rng_.normal_trunc(
                                       p_.detection_delay_days, 1.0, 0.0, 7.0)));
  }

  if (infector >= 0) {
    Agent& src = world_.agents[static_cast<std::size_t>(infector)];
    ++src.secondary_infections;
    a.generation = src.generation + 1;
  } else {
    a.generation = 0;
  }

  CaseRecord rec;
  rec.id = agent_id;
  rec.age = a.age;
  rec.role = a.role;
  rec.immunity = a.immunity;
  rec.day_infected = day_;
  rec.day_rash = a.day_rash;
  rec.infector = infector;
  rec.generation = a.generation;
  rec.setting = setting;
  rec.detected = a.detected;
  rec.imported = imported;
  case_index_[static_cast<std::size_t>(agent_id)] = static_cast<int>(cases_.size());
  cases_.push_back(rec);

  ++today_.incidence;
  ++today_.incidence_by_setting[gi(setting)];
  ++cum_cases_;
  if (imported) ++imported_total_;
  last_case_day_ = day_;
  return true;
}

void Model::progress_disease() {
  for (int id : pending_immunity_[static_cast<std::size_t>(day_)]) {
    Agent& a = world_.agents[static_cast<std::size_t>(id)];
    if (a.state == HealthState::Susceptible) {
      a.state = HealthState::Recovered;
      a.protected_by_immunity = true;
      if (a.immunity == ImmunityStatus::None || a.immunity == ImmunityStatus::Maternal) {
        a.immunity = ImmunityStatus::OneDose;
      }
    }
  }

  for (Agent& a : world_.agents) {
    if (a.state == HealthState::Dead) continue;

    if (a.state == HealthState::Exposed && day_ >= a.day_prodromal) {
      a.state = HealthState::Prodromal;
    }
    if (a.state == HealthState::Prodromal && day_ >= a.day_rash) {
      a.state = HealthState::Rash;
      ++today_.new_rash;
    }
    if (a.state == HealthState::Rash && day_ >= a.day_recover) {
      a.state = HealthState::Recovered;
    }

    if (a.detected && day_ == a.day_detected) handle_detection(a.id);

    if (a.severe && !a.hospitalized && a.day_admitted == day_ && a.state != HealthState::Dead) {
      a.hospitalized = true;
      a.isolated = false;  // isolation is now provided by the hospital
      ++today_.new_admissions;
      ++cum_admissions_;
      Hospital& h = world_.hospitals[static_cast<std::size_t>(a.assigned_hospital)];
      ++h.occupancy;
      a.pre_admission_group = a.daytime_group;
      const int ward = h.wards[static_cast<std::size_t>(rng_.uniform_int(
          0, static_cast<int>(h.wards.size()) - 1))];
      a.daytime_group = ward;  // patients mix with ward staff for their stay
      cases_[static_cast<std::size_t>(case_index_[static_cast<std::size_t>(a.id)])].hospitalized =
          true;
    }
    if (a.hospitalized && day_ >= a.day_discharge) {
      a.hospitalized = false;
      Hospital& h = world_.hospitals[static_cast<std::size_t>(a.assigned_hospital)];
      if (h.occupancy > 0) --h.occupancy;
      if (a.will_die) {
        a.state = HealthState::Dead;
        ++today_.new_deaths;
        ++cum_deaths_;
        cases_[static_cast<std::size_t>(case_index_[static_cast<std::size_t>(a.id)])].died = true;
      }
      a.daytime_group = a.pre_admission_group;  // back to school, work or unit
    }

    if (a.isolated && day_ > a.isolation_until) a.isolated = false;
  }
}

void Model::handle_detection(int agent_id) {
  Agent& a = world_.agents[static_cast<std::size_t>(agent_id)];
  ++today_.new_detected;
  ++cum_detected_;
  if (!a.hospitalized && rng_.bernoulli(p_.isolation_compliance)) {
    a.isolated = true;
    a.isolation_until = a.day_rash + static_cast<int>(std::lround(p_.rash_infectious_days));
  }
  a.visit_group_today = -1;
  apply_case_control(a);
}

void Model::offer_pep(Agent& contact) {
  // Post-exposure MMR (or immune globulin) for a susceptible household contact.
  if (contact.protected_by_immunity || contact.state != HealthState::Susceptible) return;
  if (!rng_.bernoulli(p_.pep_coverage)) return;
  ++today_.doses;
  ++doses_total_;
  if (rng_.bernoulli(p_.pep_effectiveness)) {
    const std::size_t when = std::min(pending_immunity_.size() - 1,
                                      static_cast<std::size_t>(day_ + 3));
    pending_immunity_[when].push_back(contact.id);
    contact.vaccine_effective_day = static_cast<int>(when);
    contact.vaccinated_during_response = true;
  }
}

void Model::apply_case_control(const Agent& source) {
  // A case already in hospital has no household group to trace here; a
  // long-term care resident's "household" is their unit, which is exactly the
  // group public health would place under precautions.
  int hg = home_group(source);
  if (source.hospitalized && source.household < 0) hg = -1;
  if (hg >= 0 && p_.household_quarantine) {
    for (int member : world_.groups[static_cast<std::size_t>(hg)].members) {
      if (member == source.id) continue;
      Agent& c = world_.agents[static_cast<std::size_t>(member)];
      if (!c.alive() || c.protected_by_immunity) continue;
      if (rng_.bernoulli(p_.quarantine_compliance)) {
        c.quarantine_until = std::max(c.quarantine_until, day_ + p_.quarantine_days);
      }
      offer_pep(c);
    }
  }

  // Susceptible classmates and co-workers of a case are excluded for one
  // incubation period - the standard school response to a measles case.
  const bool in_school = source.role == Role::Student || source.role == Role::Teacher ||
                         source.role == Role::DaycareChild;
  if (p_.school_exclusion && in_school && source.daytime_group >= 0) {
    for (int member : world_.groups[static_cast<std::size_t>(source.daytime_group)].members) {
      Agent& c = world_.agents[static_cast<std::size_t>(member)];
      if (!c.alive() || c.protected_by_immunity) continue;
      if (c.immunity != ImmunityStatus::None && c.immunity != ImmunityStatus::Maternal) continue;
      if (rng_.bernoulli(p_.quarantine_compliance)) {
        c.quarantine_until = std::max(c.quarantine_until, day_ + p_.quarantine_days);
      }
    }
  }
}

void Model::run_response() {
  if (!p_.response_enabled) return;
  if (!response_active_) {
    if (cum_detected_ < p_.response_trigger_cases) return;
    response_active_ = true;
    response_day_ = day_;
    catchup_queue_.clear();
    for (const Agent& a : world_.agents) {
      if (a.state != HealthState::Susceptible) continue;
      if (a.immunity != ImmunityStatus::None && a.immunity != ImmunityStatus::Maternal) continue;
      if (a.age < 1 && a.age_months < 6) continue;  // too young even for an outbreak dose
      catchup_queue_.push_back(a.id);
    }
    rng_.shuffle(catchup_queue_);
    catchup_cursor_ = 0;
  }
  response_intensity_ =
      std::min(1.0, (day_ - response_day_ + 1) / std::max(1.0, p_.response_ramp_days));

  const std::size_t remaining = catchup_queue_.size() - catchup_cursor_;
  if (remaining == 0) return;
  const double rate = p_.catchup_vaccination_daily * response_intensity_;
  int reach = rng_.binomial(static_cast<int>(remaining), std::min(1.0, rate));
  while (reach-- > 0 && catchup_cursor_ < catchup_queue_.size()) {
    Agent& a = world_.agents[static_cast<std::size_t>(catchup_queue_[catchup_cursor_++])];
    if (a.state != HealthState::Susceptible) continue;
    if (a.hesitant_household && !rng_.bernoulli(p_.catchup_vaccination_hesitant_mult)) continue;
    ++today_.doses;
    ++doses_total_;
    if (rng_.bernoulli(p_.ve_one_dose)) {
      const std::size_t when = std::min(pending_immunity_.size() - 1,
                                        static_cast<std::size_t>(day_ + 14));
      pending_immunity_[when].push_back(a.id);
      a.vaccine_effective_day = static_cast<int>(when);
    }
    a.vaccinated_during_response = true;
  }
}

void Model::plan_care_seeking() {
  for (Agent& a : world_.agents) {
    a.visit_group_today = -1;
    if (!a.alive() || a.hospitalized) continue;
    if (a.role == Role::LtcfResident) continue;  // seen in place by facility staff
    if (a.state == HealthState::Prodromal) {
      // Prodromal measles looks like any winter virus, so these visits happen
      // before anyone suspects measles - and seed the waiting room.
      const double daily = p_.prob_urgent_care_prodrome / std::max(1.0, p_.prodrome_days);
      if (rng_.bernoulli(daily)) {
        a.visit_group_today =
            world_.urgent_cares[static_cast<std::size_t>(a.home_urgent_care)].waiting_group;
      }
    } else if (a.state == HealthState::Rash && !a.isolated) {
      const int rash_day = day_ - a.day_rash;
      if (rash_day >= 0 && rash_day <= 1 && rng_.bernoulli(p_.prob_seek_care_rash / 2.0)) {
        if (rng_.bernoulli(p_.share_rash_visits_to_hospital)) {
          a.visit_group_today =
              world_.hospitals[static_cast<std::size_t>(a.home_hospital)].waiting_group;
        } else {
          a.visit_group_today =
              world_.urgent_cares[static_cast<std::size_t>(a.home_urgent_care)].waiting_group;
        }
      }
    }
    if (a.visit_group_today >= 0) {
      a.visit_block_today = rng_.bernoulli(0.6) ? Block::Daytime : Block::Evening;
    }
  }
}

void Model::schedule_routine_visits() {
  const int n = static_cast<int>(world_.agents.size());
  const double daily = p_.routine_care_visits_per_year / 365.0;
  int visits = rng_.poisson(daily * n * 1.25);
  while (visits-- > 0) {
    const int id = rng_.uniform_int(0, n - 1);
    Agent& a = world_.agents[static_cast<std::size_t>(id)];
    if (!a.alive() || a.hospitalized || a.isolated) continue;
    if (a.role == Role::LtcfResident) continue;
    if (a.visit_group_today >= 0) continue;
    // The very young and the old attend far more often than everyone else.
    const bool high_risk = a.age < 5 || a.age >= 65;
    if (!high_risk && !rng_.bernoulli(0.6)) continue;
    if (rng_.bernoulli(p_.hospital_share_of_routine_visits)) {
      a.visit_group_today =
          world_.hospitals[static_cast<std::size_t>(a.home_hospital)].waiting_group;
    } else {
      a.visit_group_today =
          world_.urgent_cares[static_cast<std::size_t>(a.home_urgent_care)].waiting_group;
    }
    a.visit_block_today = rng_.bernoulli(0.6) ? Block::Daytime : Block::Evening;
  }
}

bool Model::at_work_today(const Agent& a, bool weekend) {
  Rng& rng = rng_;
  switch (a.role) {
    case Role::HealthcareWorker:
    case Role::LtcfStaff:
      return rng.bernoulli(weekend ? p_.healthcare_weekend_prob : p_.healthcare_workday_prob);
    case Role::Worker:
      return weekend ? rng.bernoulli(p_.weekend_worker_prob) : true;
    case Role::Student:
    case Role::Teacher:
    case Role::DaycareChild:
      return !weekend;
    default:
      return false;
  }
}

void Model::add_presence(int group_id, int agent_id, float weight) {
  if (group_id < 0 || weight <= 0.0f) return;
  const std::size_t g = static_cast<std::size_t>(group_id);
  if (!group_active_[g]) {
    group_active_[g] = 1;
    active_groups_.push_back(group_id);
  }
  presence_[g].push_back(Presence{agent_id, weight});
}

void Model::build_presence(Block block, bool weekend) {
  for (int g : active_groups_) {
    presence_[static_cast<std::size_t>(g)].clear();
    group_active_[static_cast<std::size_t>(g)] = 0;
  }
  active_groups_.clear();

  for (Agent& a : world_.agents) {
    if (!a.alive()) continue;

    if (a.hospitalized) {
      add_presence(a.daytime_group, a.id, 1.0f);
      continue;
    }

    const int home = home_group(a);
    if (a.visit_group_today >= 0 && block == a.visit_block_today) {
      // Part of the block in a waiting room, the rest at home.
      add_presence(a.visit_group_today, a.id, 0.35f);
      add_presence(home, a.id, 0.65f);
      continue;
    }

    const bool confined = a.isolated || a.quarantine_until >= day_;

    switch (block) {
      case Block::Night:
      case Block::EarlyDay:
        add_presence(home, a.id, 1.0f);
        break;

      case Block::Daytime: {
        if (a.role == Role::LtcfResident) {
          add_presence(a.daytime_group, a.id, 1.0f);
          break;
        }
        if (confined || !at_work_today(a, weekend) || a.daytime_group < 0) {
          add_presence(home, a.id, 1.0f);
          break;
        }
        if (a.secondary_group >= 0) {
          // Time is split between the immediate group (classroom, team, unit)
          // and the shared spaces of the site.
          const float primary = (a.role == Role::Student || a.role == Role::Teacher) ? 0.75f : 0.80f;
          add_presence(a.daytime_group, a.id, primary);
          add_presence(a.secondary_group, a.id, 1.0f - primary);
        } else {
          add_presence(a.daytime_group, a.id, 1.0f);
        }
        break;
      }

      case Block::Evening: {
        if (a.role == Role::LtcfResident) {
          add_presence(a.secondary_group >= 0 ? a.secondary_group : a.daytime_group, a.id, 1.0f);
          break;
        }
        if (confined) {
          add_presence(home, a.id, 1.0f);
          break;
        }
        if (rng_.bernoulli(p_.prob_community_evening)) {
          add_presence(a.community_hub, a.id, 1.0f);
        } else if (a.age >= 18 && !world_.ltcfs.empty() &&
                   rng_.bernoulli(p_.prob_ltcf_visit_evening)) {
          const int f = rng_.uniform_int(0, static_cast<int>(world_.ltcfs.size()) - 1);
          add_presence(world_.ltcfs[static_cast<std::size_t>(f)].common_group, a.id, 1.0f);
        } else {
          add_presence(home, a.id, 1.0f);
        }
        break;
      }
      default:
        break;
    }
  }
}

void Model::transmit(Block block) {
  const double hours = kBlockHours[static_cast<int>(block)];
  const double decay = std::pow(0.5, hours / p_.environment_half_life_hours);

  std::vector<int> shedders;
  std::vector<double> shed_weight;

  for (int g : active_groups_) {
    ContactGroup& cg = world_.groups[static_cast<std::size_t>(g)];
    const std::vector<Presence>& here = presence_[static_cast<std::size_t>(g)];
    shedders.clear();
    shed_weight.clear();

    double pressure = 0.0;
    for (const Presence& e : here) {
      const Agent& a = world_.agents[static_cast<std::size_t>(e.agent)];
      if (!a.infectious()) continue;
      const double contribution = infectiousness_of(a, cg.type) * e.weight;
      if (contribution <= 0.0) continue;
      pressure += contribution;
      shedders.push_back(e.agent);
      shed_weight.push_back(contribution);
    }

    const double total_force = pressure + cg.environment;
    if (total_force > 0.0 && !here.empty()) {
      const double denom = std::max(1.0, static_cast<double>(here.size()) - 1.0);
      // In a big room you share air with a bounded number of people, not with
      // everyone, so the per-person contact rate saturates.
      const double contact_fraction = std::min(1.0, p_.contact_cap[gi(cg.type)] / denom);
      const double beta = beta_for(cg.type);
      for (const Presence& e : here) {
        Agent& a = world_.agents[static_cast<std::size_t>(e.agent)];
        if (a.state != HealthState::Susceptible) continue;
        const double lambda =
            beta * hours * e.weight * contact_fraction * total_force * a.susceptibility;
        if (lambda <= 0.0) continue;
        if (!rng_.bernoulli(-std::expm1(-lambda))) continue;
        int source = -1;
        if (!shedders.empty()) {
          source = shedders[rng_.weighted_choice(shed_weight)];
        }
        if (infect(a.id, source, cg.type, false)) ++cg.infections_here;
      }
    }

    // Measles aerosol lingers in a room after the case has left; this is what
    // lets a waiting room infect the next patient through the door.
    cg.environment = (cg.environment + p_.environment_shed * pressure) * decay;
  }
}

void Model::seed_importations() {
  int n = rng_.poisson(p_.importation_per_day);
  const int total = static_cast<int>(world_.agents.size());
  while (n-- > 0) {
    for (int attempt = 0; attempt < 64; ++attempt) {
      const int id = rng_.uniform_int(0, total - 1);
      if (world_.agents[static_cast<std::size_t>(id)].state == HealthState::Susceptible) {
        infect(id, -1, GroupType::CommunityHub, true);
        break;
      }
    }
  }
}

void Model::record_day() {
  DayRecord& r = today_;
  r.day = day_;
  int census = 0;
  for (const Agent& a : world_.agents) {
    switch (a.state) {
      case HealthState::Susceptible: ++r.susceptible; break;
      case HealthState::Exposed: ++r.exposed; break;
      case HealthState::Prodromal: ++r.prodromal; break;
      case HealthState::Rash: ++r.rash; break;
      case HealthState::Recovered: ++r.recovered; break;
      case HealthState::Dead: ++r.dead; break;
      default: break;
    }
    if (a.hospitalized) { ++r.hospitalized; ++census; }
    if (a.isolated) ++r.isolated;
    if (a.alive() && a.quarantine_until >= day_) ++r.quarantined;
  }
  peak_census_ = std::max(peak_census_, census);
  for (const Hospital& h : world_.hospitals) {
    if (h.occupancy > h.beds) {
      ++days_over_capacity_;
      break;
    }
  }
  r.cum_cases = cum_cases_;
  r.cum_detected = cum_detected_;
  r.cum_admissions = cum_admissions_;
  r.cum_deaths = cum_deaths_;
  r.response_active = response_active_;
  history_.push_back(r);
}

void Model::step() {
  if (!keep_today_) today_ = DayRecord();
  keep_today_ = false;
  today_.day = day_;

  progress_disease();
  run_response();
  plan_care_seeking();
  schedule_routine_visits();

  const bool weekend = (day_ % 7) >= 5;
  for (int b = 0; b < kBlockCount; ++b) {
    const Block block = static_cast<Block>(b);
    build_presence(block, weekend);
    transmit(block);
  }
  seed_importations();

  // A room's aerosol does not survive until the next morning.
  for (ContactGroup& g : world_.groups) g.environment = 0.0;

  record_day();
  ++day_;
}

bool Model::extinct() const {
  for (const Agent& a : world_.agents) {
    if (a.state == HealthState::Exposed || a.infectious()) return false;
  }
  return true;
}

void Model::run() {
  const int total = static_cast<int>(world_.agents.size());
  today_ = DayRecord();
  for (int i = 0; i < p_.initial_infections; ++i) {
    for (int attempt = 0; attempt < 512; ++attempt) {
      const int id = rng_.uniform_int(0, total - 1);
      if (world_.agents[static_cast<std::size_t>(id)].state == HealthState::Susceptible) {
        // Index cases are treated as importations into the community.
        infect(id, -1, GroupType::CommunityHub, true);
        break;
      }
    }
  }
  keep_today_ = true;  // fold the seeded cases into day 0

  while (day_ < p_.days) {
    step();
    if (extinct() && p_.importation_per_day <= 0.0) break;
  }
}

Summary Model::summarize() const {
  Summary s;
  s.population = static_cast<long long>(world_.agents.size());
  for (const Agent& a : world_.agents) {
    ++s.population_by_age[age_bucket(a.age)];
    const bool ever_infected = a.day_exposed >= 0;
    if (!ever_infected && a.state == HealthState::Susceptible) {
      ++s.susceptible_by_age[age_bucket(a.age)];
    }
  }
  // Susceptibles at the start = never-infected susceptibles + everyone infected
  // (all infections start from a susceptible) minus outbreak-dose recipients.
  s.susceptible_at_start = 0;
  for (const Agent& a : world_.agents) {
    if (a.day_exposed >= 0) { ++s.susceptible_at_start; continue; }
    if (a.state == HealthState::Susceptible) { ++s.susceptible_at_start; continue; }
    if (a.vaccinated_during_response) ++s.susceptible_at_start;
  }

  s.infections = static_cast<long long>(cases_.size());
  s.imported = imported_total_;
  s.detected = cum_detected_;
  s.hospitalizations = cum_admissions_;
  s.deaths = cum_deaths_;
  s.doses_given = doses_total_;
  s.peak_hospital_census = peak_census_;
  s.days_over_hospital_capacity = days_over_capacity_;
  for (const Hospital& h : world_.hospitals) s.hospital_beds += h.beds;
  s.last_case_day = last_case_day_;
  s.response_day = response_day_;

  double generation_sum = 0.0;
  long long generation_n = 0;
  for (const CaseRecord& c : cases_) {
    ++s.infections_by_setting[gi(c.setting)];
    ++s.infections_by_age[age_bucket(c.age)];
    if (c.role == Role::HealthcareWorker) ++s.infections_healthcare_workers;
    if (c.role == Role::LtcfResident) ++s.infections_ltcf_residents;
    if (c.role == Role::Student || c.role == Role::DaycareChild) ++s.infections_school_children;
    if (world_.agents[static_cast<std::size_t>(c.id)].hesitant_household) {
      ++s.infections_in_hesitant_households;
    }
    if (c.infector >= 0) {
      const int idx = case_index_[static_cast<std::size_t>(c.infector)];
      if (idx >= 0) {
        generation_sum += c.day_infected - cases_[static_cast<std::size_t>(idx)].day_infected;
        ++generation_n;
      }
    }
  }
  if (generation_n > 0) s.mean_generation_interval = generation_sum / generation_n;
  if (s.susceptible_at_start > 0) {
    s.attack_rate_susceptible =
        static_cast<double>(s.infections) / static_cast<double>(s.susceptible_at_start);
  }

  // Mean offspring of the first three generations, before depletion and the
  // public health response bite: a rough effective reproduction number.
  long long early_parents = 0, early_offspring = 0;
  for (const CaseRecord& c : cases_) {
    if (c.generation > 2) continue;
    ++early_parents;
    early_offspring += world_.agents[static_cast<std::size_t>(c.id)].secondary_infections;
  }
  if (early_parents > 0) {
    s.r0_effective_first_generations =
        static_cast<double>(early_offspring) / static_cast<double>(early_parents);
  }

  for (const DayRecord& r : history_) {
    if (r.incidence > s.peak_incidence) {
      s.peak_incidence = r.incidence;
      s.peak_incidence_day = r.day;
    }
  }
  return s;
}

R0Estimate estimate_r0(const Params& params, int replicates) {
  // A fully susceptible community with no detection, isolation or response:
  // the textbook setting in which R0 is defined.
  Params q = params;
  q.coverage_dose1 = 0.0;
  q.coverage_hesitant_dose1 = 0.0;
  q.coverage_dose2_given_dose1 = 0.0;
  q.adult_1957_1989_one_dose = 0.0;
  q.adult_1957_1989_two_dose = 0.0;
  q.adult_post1989_two_dose = 0.0;
  q.pre1957_natural_immunity = 0.0;
  q.healthcare_worker_two_dose = 0.0;
  q.ltcf_staff_two_dose = 0.0;
  q.maternal_protection_under6mo = 0.0;
  q.maternal_protection_6to11mo = 0.0;
  q.prob_case_detected = 0.0;
  q.response_enabled = false;
  q.importation_per_day = 0.0;
  q.initial_infections = 0;
  q.days = 20;  // long enough for one index case to complete its infectious period

  Rng setup(q.seed);
  const World base = build_world(q, setup);

  R0Estimate est;
  std::vector<double> offspring;
  offspring.reserve(static_cast<std::size_t>(replicates));
  for (int r = 0; r < replicates; ++r) {
    World w = base;
    Rng rng(q.seed + 1000ULL * static_cast<unsigned long long>(r + 1));
    Model m(q, w, rng);
    int index = -1;
    for (int attempt = 0; attempt < 512; ++attempt) {
      const int id = rng.uniform_int(0, static_cast<int>(w.agents.size()) - 1);
      if (w.agents[static_cast<std::size_t>(id)].state == HealthState::Susceptible) {
        index = id;
        break;
      }
    }
    if (index < 0) continue;
    m.infect(index, -1, GroupType::CommunityHub, true);
    for (int d = 0; d < q.days; ++d) m.step();
    const int direct = w.agents[static_cast<std::size_t>(index)].secondary_infections;
    offspring.push_back(direct);
    for (const CaseRecord& c : m.cases()) {
      if (c.infector == index) est.by_setting[gi(c.setting)] += 1.0;
    }
  }
  est.replicates = static_cast<int>(offspring.size());
  if (est.replicates == 0) return est;
  double sum = 0.0;
  for (double v : offspring) sum += v;
  est.mean = sum / est.replicates;
  double var = 0.0;
  for (double v : offspring) var += (v - est.mean) * (v - est.mean);
  est.sd = std::sqrt(var / std::max(1, est.replicates - 1));
  for (int t = 0; t < kGroupTypeCount; ++t) est.by_setting[t] /= est.replicates;
  return est;
}

}  // namespace measles
