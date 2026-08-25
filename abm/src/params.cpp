#include "params.hpp"

#include <sstream>

namespace measles {
namespace {

inline int gi(GroupType t) { return static_cast<int>(t); }

}  // namespace

Params::Params() {
  // Per-hour hazard per infectious contact sharing the venue. The household
  // value is anchored to the ~90% secondary attack rate reported among
  // susceptible household members; the rest were calibrated so that the whole
  // model reproduces a basic reproduction number near 15 (measles is usually
  // quoted at 12-18), with schools, workplaces, households, waiting rooms and
  // community venues contributing roughly 34/18/12/12/24 percent of an index
  // case's offspring. Re-run `measles_abm --mode r0` after changing any of
  // them.
  beta[gi(GroupType::Household)] = 0.0230;
  beta[gi(GroupType::DaycareRoom)] = 0.1400;
  beta[gi(GroupType::Classroom)] = 0.0900;
  beta[gi(GroupType::SchoolCommon)] = 0.0280;
  beta[gi(GroupType::WorkTeam)] = 0.0600;
  beta[gi(GroupType::WorkplaceCommon)] = 0.0200;
  beta[gi(GroupType::HospitalWaiting)] = 0.1100;   // crowded, poorly ventilated
  beta[gi(GroupType::HospitalWard)] = 0.0320;
  beta[gi(GroupType::UrgentCareWaiting)] = 0.2100;  // small room, long waits
  beta[gi(GroupType::LtcfUnit)] = 0.0600;
  beta[gi(GroupType::LtcfCommon)] = 0.0300;
  beta[gi(GroupType::CommunityHub)] = 0.0100;

  // Measles is airborne, so the relevant contact set is everyone sharing the
  // room's air, not just close contacts - these caps are correspondingly wide.
  // They bound the number of distinct people reachable in one venue, which
  // matters only where the venue is larger than the cap.
  contact_cap[gi(GroupType::Household)] = 8;
  contact_cap[gi(GroupType::DaycareRoom)] = 12;
  contact_cap[gi(GroupType::Classroom)] = 25;
  contact_cap[gi(GroupType::SchoolCommon)] = 40;
  contact_cap[gi(GroupType::WorkTeam)] = 10;
  contact_cap[gi(GroupType::WorkplaceCommon)] = 30;
  contact_cap[gi(GroupType::HospitalWaiting)] = 25;
  contact_cap[gi(GroupType::HospitalWard)] = 15;
  contact_cap[gi(GroupType::UrgentCareWaiting)] = 20;
  contact_cap[gi(GroupType::LtcfUnit)] = 20;
  contact_cap[gi(GroupType::LtcfCommon)] = 35;
  contact_cap[gi(GroupType::CommunityHub)] = 40;
}

void Params::apply(const Config& c) {
  population = c.get_int("population", population);
  days = c.get_int("days", days);
  seed = c.get_uint64("seed", seed);
  initial_infections = c.get_int("initial_infections", initial_infections);
  importation_per_day = c.get_double("importation_per_day", importation_per_day);

  latent_days_mean = c.get_double("latent_days_mean", latent_days_mean);
  latent_days_sd = c.get_double("latent_days_sd", latent_days_sd);
  latent_days_min = c.get_double("latent_days_min", latent_days_min);
  latent_days_max = c.get_double("latent_days_max", latent_days_max);
  prodrome_days = c.get_double("prodrome_days", prodrome_days);
  rash_infectious_days = c.get_double("rash_infectious_days", rash_infectious_days);
  rash_total_days = c.get_double("rash_total_days", rash_total_days);
  infectiousness_prodromal = c.get_double("infectiousness_prodromal", infectiousness_prodromal);
  infectiousness_rash = c.get_double("infectiousness_rash", infectiousness_rash);

  for (int t = 0; t < kGroupTypeCount; ++t) {
    const std::string name = group_type_name(static_cast<GroupType>(t));
    beta[t] = c.get_double("beta." + name, beta[t]);
    contact_cap[t] = c.get_double("contacts." + name, contact_cap[t]);
  }
  environment_shed = c.get_double("environment_shed", environment_shed);
  environment_half_life_hours =
      c.get_double("environment_half_life_hours", environment_half_life_hours);

  ve_one_dose = c.get_double("ve_one_dose", ve_one_dose);
  ve_two_dose = c.get_double("ve_two_dose", ve_two_dose);
  coverage_dose1 = c.get_double("coverage_dose1", coverage_dose1);
  coverage_dose2_given_dose1 =
      c.get_double("coverage_dose2_given_dose1", coverage_dose2_given_dose1);
  hesitancy_mean = c.get_double("hesitancy_mean", hesitancy_mean);
  hesitancy_clustering = c.get_double("hesitancy_clustering", hesitancy_clustering);
  coverage_hesitant_dose1 = c.get_double("coverage_hesitant_dose1", coverage_hesitant_dose1);
  adult_1957_1989_two_dose = c.get_double("adult_1957_1989_two_dose", adult_1957_1989_two_dose);
  adult_1957_1989_one_dose = c.get_double("adult_1957_1989_one_dose", adult_1957_1989_one_dose);
  adult_post1989_two_dose = c.get_double("adult_post1989_two_dose", adult_post1989_two_dose);
  pre1957_natural_immunity = c.get_double("pre1957_natural_immunity", pre1957_natural_immunity);
  healthcare_worker_two_dose =
      c.get_double("healthcare_worker_two_dose", healthcare_worker_two_dose);
  ltcf_staff_two_dose = c.get_double("ltcf_staff_two_dose", ltcf_staff_two_dose);
  maternal_protection_under6mo =
      c.get_double("maternal_protection_under6mo", maternal_protection_under6mo);
  maternal_protection_6to11mo =
      c.get_double("maternal_protection_6to11mo", maternal_protection_6to11mo);

  hosp_prob_under1 = c.get_double("hosp_prob_under1", hosp_prob_under1);
  hosp_prob_1to4 = c.get_double("hosp_prob_1to4", hosp_prob_1to4);
  hosp_prob_5to19 = c.get_double("hosp_prob_5to19", hosp_prob_5to19);
  hosp_prob_20to64 = c.get_double("hosp_prob_20to64", hosp_prob_20to64);
  hosp_prob_65plus = c.get_double("hosp_prob_65plus", hosp_prob_65plus);
  hosp_prob_vaccinated_mult =
      c.get_double("hosp_prob_vaccinated_mult", hosp_prob_vaccinated_mult);
  cfr_under5 = c.get_double("cfr_under5", cfr_under5);
  cfr_5to19 = c.get_double("cfr_5to19", cfr_5to19);
  cfr_20to64 = c.get_double("cfr_20to64", cfr_20to64);
  cfr_65plus = c.get_double("cfr_65plus", cfr_65plus);
  hospital_stay_days = c.get_double("hospital_stay_days", hospital_stay_days);

  prob_urgent_care_prodrome = c.get_double("prob_urgent_care_prodrome", prob_urgent_care_prodrome);
  prob_seek_care_rash = c.get_double("prob_seek_care_rash", prob_seek_care_rash);
  routine_care_visits_per_year =
      c.get_double("routine_care_visits_per_year", routine_care_visits_per_year);
  hospital_share_of_routine_visits =
      c.get_double("hospital_share_of_routine_visits", hospital_share_of_routine_visits);
  share_rash_visits_to_hospital =
      c.get_double("share_rash_visits_to_hospital", share_rash_visits_to_hospital);

  prob_case_detected = c.get_double("prob_case_detected", prob_case_detected);
  detection_delay_days = c.get_double("detection_delay_days", detection_delay_days);
  isolation_compliance = c.get_double("isolation_compliance", isolation_compliance);
  isolation_household_reduction =
      c.get_double("isolation_household_reduction", isolation_household_reduction);
  quarantine_compliance = c.get_double("quarantine_compliance", quarantine_compliance);
  quarantine_days = c.get_int("quarantine_days", quarantine_days);
  school_exclusion = c.get_bool("school_exclusion", school_exclusion);
  household_quarantine = c.get_bool("household_quarantine", household_quarantine);

  response_trigger_cases = c.get_int("response_trigger_cases", response_trigger_cases);
  response_ramp_days = c.get_double("response_ramp_days", response_ramp_days);
  airborne_precaution_reduction =
      c.get_double("airborne_precaution_reduction", airborne_precaution_reduction);
  ltcf_lockdown_reduction = c.get_double("ltcf_lockdown_reduction", ltcf_lockdown_reduction);
  catchup_vaccination_daily =
      c.get_double("catchup_vaccination_daily", catchup_vaccination_daily);
  catchup_vaccination_hesitant_mult =
      c.get_double("catchup_vaccination_hesitant_mult", catchup_vaccination_hesitant_mult);
  pep_coverage = c.get_double("pep_coverage", pep_coverage);
  pep_effectiveness = c.get_double("pep_effectiveness", pep_effectiveness);
  response_enabled = c.get_bool("response_enabled", response_enabled);

  daycare_attendance = c.get_double("daycare_attendance", daycare_attendance);
  employment_rate_18_64 = c.get_double("employment_rate_18_64", employment_rate_18_64);
  employment_rate_65_74 = c.get_double("employment_rate_65_74", employment_rate_65_74);
  classroom_size = c.get_int("classroom_size", classroom_size);
  daycare_room_size = c.get_int("daycare_room_size", daycare_room_size);
  work_team_size = c.get_int("work_team_size", work_team_size);
  school_size_elementary = c.get_int("school_size_elementary", school_size_elementary);
  school_size_middle = c.get_int("school_size_middle", school_size_middle);
  school_size_high = c.get_int("school_size_high", school_size_high);
  daycare_center_size = c.get_int("daycare_center_size", daycare_center_size);
  workplace_mean_size = c.get_double("workplace_mean_size", workplace_mean_size);
  workplace_size_sigma = c.get_double("workplace_size_sigma", workplace_size_sigma);
  people_per_hospital = c.get_int("people_per_hospital", people_per_hospital);
  people_per_urgent_care = c.get_int("people_per_urgent_care", people_per_urgent_care);
  people_per_ltcf = c.get_int("people_per_ltcf", people_per_ltcf);
  ltcf_unit_size = c.get_int("ltcf_unit_size", ltcf_unit_size);
  ltcf_residency_rate_80plus =
      c.get_double("ltcf_residency_rate_80plus", ltcf_residency_rate_80plus);
  ltcf_residency_rate_65to79 =
      c.get_double("ltcf_residency_rate_65to79", ltcf_residency_rate_65to79);
  ltcf_staff_per_resident = c.get_double("ltcf_staff_per_resident", ltcf_staff_per_resident);
  hospital_beds_per_1000 = c.get_double("hospital_beds_per_1000", hospital_beds_per_1000);
  hospital_staff_per_bed = c.get_double("hospital_staff_per_bed", hospital_staff_per_bed);
  urgent_care_staff = c.get_double("urgent_care_staff", urgent_care_staff);
  people_per_community_hub = c.get_int("people_per_community_hub", people_per_community_hub);
  prob_community_evening = c.get_double("prob_community_evening", prob_community_evening);
  prob_ltcf_visit_evening = c.get_double("prob_ltcf_visit_evening", prob_ltcf_visit_evening);
  weekend_worker_prob = c.get_double("weekend_worker_prob", weekend_worker_prob);
  healthcare_workday_prob = c.get_double("healthcare_workday_prob", healthcare_workday_prob);
  healthcare_weekend_prob = c.get_double("healthcare_weekend_prob", healthcare_weekend_prob);

  output_dir = c.get_string("output_dir", output_dir);
  write_linelist = c.get_bool("write_linelist", write_linelist);
  quiet = c.get_bool("quiet", quiet);
}

std::string Params::validate() const {
  std::ostringstream err;
  auto in_unit = [&err](const char* name, double v) {
    if (v < 0.0 || v > 1.0) err << name << " must be in [0,1] (got " << v << "); ";
  };
  if (population < 100) err << "population must be at least 100; ";
  if (days < 1) err << "days must be positive; ";
  if (initial_infections < 0) err << "initial_infections must be non-negative; ";
  if (latent_days_min > latent_days_max) err << "latent_days_min exceeds latent_days_max; ";
  if (prodrome_days <= 0 || rash_infectious_days <= 0)
    err << "infectious period durations must be positive; ";
  if (environment_half_life_hours <= 0) err << "environment_half_life_hours must be positive; ";
  if (classroom_size < 2 || work_team_size < 2 || ltcf_unit_size < 2)
    err << "group sizes must be at least 2; ";
  if (people_per_hospital < 1 || people_per_urgent_care < 1 || people_per_ltcf < 1 ||
      people_per_community_hub < 1)
    err << "people_per_* denominators must be positive; ";
  in_unit("ve_one_dose", ve_one_dose);
  in_unit("ve_two_dose", ve_two_dose);
  in_unit("coverage_dose1", coverage_dose1);
  in_unit("coverage_dose2_given_dose1", coverage_dose2_given_dose1);
  in_unit("hesitancy_mean", hesitancy_mean);
  in_unit("prob_case_detected", prob_case_detected);
  in_unit("isolation_compliance", isolation_compliance);
  in_unit("quarantine_compliance", quarantine_compliance);
  in_unit("pep_coverage", pep_coverage);
  in_unit("pep_effectiveness", pep_effectiveness);
  in_unit("prob_community_evening", prob_community_evening);
  in_unit("employment_rate_18_64", employment_rate_18_64);
  for (int t = 0; t < kGroupTypeCount; ++t) {
    if (beta[t] < 0.0)
      err << "beta." << group_type_name(static_cast<GroupType>(t)) << " must be non-negative; ";
    if (contact_cap[t] < 1.0)
      err << "contacts." << group_type_name(static_cast<GroupType>(t)) << " must be at least 1; ";
  }
  return err.str();
}

}  // namespace measles
