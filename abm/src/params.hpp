// Every tunable quantity in the model, with literature-motivated defaults.
#pragma once

#include <string>

#include "config.hpp"
#include "types.hpp"

namespace measles {

struct Params {
  // ---- Simulation control -------------------------------------------------
  int population = 60000;
  int days = 365;
  unsigned long long seed = 2026ULL;
  int initial_infections = 3;
  double importation_per_day = 0.01;   // externally acquired cases per day

  // ---- Natural history ----------------------------------------------------
  // Exposure -> rash averages ~14 days; the infectious window is the four days
  // before rash through the four days after.
  double latent_days_mean = 10.0;      // exposure -> start of prodrome
  double latent_days_sd = 1.8;
  double latent_days_min = 6.0;
  double latent_days_max = 16.0;
  double prodrome_days = 4.0;          // infectious, pre-rash
  double rash_infectious_days = 4.0;   // infectious, post-rash
  double rash_total_days = 7.0;        // rash duration for isolation purposes
  double infectiousness_prodromal = 1.0;
  double infectiousness_rash = 0.85;

  // ---- Transmission -------------------------------------------------------
  // Per-hour, per-contact hazard in each venue type, and the number of distinct
  // people an agent actually shares close air with there.
  double beta[kGroupTypeCount] = {};
  double contact_cap[kGroupTypeCount] = {};

  // Measles aerosol remains infectious in a room for up to two hours; this is
  // the fraction of an infectious person's shedding that is left behind for the
  // next block, and the per-hour decay applied to it.
  double environment_shed = 0.35;
  double environment_half_life_hours = 1.2;

  // ---- Vaccination --------------------------------------------------------
  double ve_one_dose = 0.93;
  double ve_two_dose = 0.97;
  double coverage_dose1 = 0.93;              // among non-hesitant children >=1y
  double coverage_dose2_given_dose1 = 0.95;  // among non-hesitant children >=5y
  double hesitancy_mean = 0.06;              // mean household refusal rate
  double hesitancy_clustering = 12.0;        // Beta concentration; lower = more clustered
  double coverage_hesitant_dose1 = 0.10;     // hesitant households rarely start
  double adult_1957_1989_two_dose = 0.35;    // one-dose era birth cohorts
  double adult_1957_1989_one_dose = 0.55;
  double adult_post1989_two_dose = 0.92;
  double pre1957_natural_immunity = 0.97;    // pre-elimination birth cohorts
  double healthcare_worker_two_dose = 0.97;  // occupational requirement
  double ltcf_staff_two_dose = 0.93;
  double maternal_protection_under6mo = 0.85;
  double maternal_protection_6to11mo = 0.25;

  // ---- Severity and care seeking -----------------------------------------
  double hosp_prob_under1 = 0.42;
  double hosp_prob_1to4 = 0.28;
  double hosp_prob_5to19 = 0.11;
  double hosp_prob_20to64 = 0.20;
  double hosp_prob_65plus = 0.34;
  double hosp_prob_vaccinated_mult = 0.35;   // modified measles is milder
  double cfr_under5 = 0.0020;
  double cfr_5to19 = 0.0010;
  double cfr_20to64 = 0.0012;
  double cfr_65plus = 0.0090;
  double hospital_stay_days = 5.0;

  // A prodromal case looks like any other viral illness, so a share of them
  // walk into an urgent care and seed the waiting room.
  double prob_urgent_care_prodrome = 0.14;
  double prob_seek_care_rash = 0.62;
  // Background demand for care: without it the waiting rooms would contain
  // nobody for an undiagnosed case to infect.
  double routine_care_visits_per_year = 3.0;
  double hospital_share_of_routine_visits = 0.40;
  double share_rash_visits_to_hospital = 0.45;  // rest go to urgent care

  // ---- Detection and control ---------------------------------------------
  double prob_case_detected = 0.80;      // rash case reported to public health
  double detection_delay_days = 1.5;     // rash onset -> report
  double isolation_compliance = 0.85;
  double isolation_household_reduction = 0.70;  // residual household hazard
  double quarantine_compliance = 0.75;
  int quarantine_days = 21;              // one incubation period for contacts
  bool school_exclusion = true;          // exclude susceptible classmates
  bool household_quarantine = true;

  // ---- Outbreak response --------------------------------------------------
  int response_trigger_cases = 5;        // detected cases that activate response
  double response_ramp_days = 5.0;
  double airborne_precaution_reduction = 0.20;  // residual hazard in healthcare
  double ltcf_lockdown_reduction = 0.45;        // residual hazard in LTCF common areas
  double catchup_vaccination_daily = 0.02;      // eligible unvaccinated reached/day
  double catchup_vaccination_hesitant_mult = 0.15;
  double pep_coverage = 0.55;            // household contacts offered PEP
  double pep_effectiveness = 0.83;       // MMR within 72h of exposure
  bool response_enabled = true;

  // ---- Synthetic population -----------------------------------------------
  double daycare_attendance = 0.55;
  double employment_rate_18_64 = 0.74;
  double employment_rate_65_74 = 0.22;
  int classroom_size = 23;
  int daycare_room_size = 10;
  int work_team_size = 9;
  int school_size_elementary = 420;
  int school_size_middle = 620;
  int school_size_high = 1100;
  int daycare_center_size = 60;
  double workplace_mean_size = 24.0;     // lognormal median-ish
  double workplace_size_sigma = 1.1;
  int people_per_hospital = 120000;
  int people_per_urgent_care = 30000;
  int people_per_ltcf = 30000;
  int ltcf_unit_size = 28;
  double ltcf_residency_rate_80plus = 0.075;
  double ltcf_residency_rate_65to79 = 0.010;
  double ltcf_staff_per_resident = 0.45;
  double hospital_beds_per_1000 = 2.4;
  double hospital_staff_per_bed = 3.2;
  double urgent_care_staff = 9.0;
  int people_per_community_hub = 1400;
  double prob_community_evening = 0.45;
  double prob_ltcf_visit_evening = 0.006;  // per adult per evening: visiting a resident
  double weekend_worker_prob = 0.15;       // ordinary workers on a weekend shift
  double healthcare_workday_prob = 0.85;   // clinical staff rostered on a weekday
  double healthcare_weekend_prob = 0.70;   // ... and at the weekend

  // ---- Output -------------------------------------------------------------
  std::string output_dir = "results";
  bool write_linelist = true;
  bool quiet = false;

  // Fills in the beta / contact_cap tables with the defaults below.
  Params();

  // Overrides any field whose key appears in `cfg`.
  void apply(const Config& cfg);

  // Returns a human readable problem description, or "" when consistent.
  std::string validate() const;
};

}  // namespace measles
