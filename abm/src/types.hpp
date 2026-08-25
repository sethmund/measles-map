// Core entity definitions: agents, mixing groups, and the enums that tie them
// together.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace measles {

// Measles natural history. The infectious window runs from roughly four days
// before rash onset (prodrome: fever, cough, coryza, conjunctivitis) to four
// days after, which is why `Prodromal` is a distinct - and epidemiologically
// dominant - state: nobody knows it is measles yet.
enum class HealthState : std::uint8_t {
  Susceptible = 0,
  Exposed,     // infected, not yet infectious
  Prodromal,   // infectious, pre-rash, usually undiagnosed
  Rash,        // infectious, clinically recognisable
  Recovered,   // lifelong immunity
  Dead,
  Count
};

// How an agent came by whatever immunity they have.
enum class ImmunityStatus : std::uint8_t {
  None = 0,
  Maternal,   // infant antibody, wanes over the first year
  OneDose,    // MMR x1
  TwoDose,    // MMR x2
  Natural,    // prior infection (incl. pre-vaccine-era birth cohorts)
  Count
};

// The daily activity role that determines where an agent spends its day.
enum class Role : std::uint8_t {
  Infant = 0,      // under school age and cared for at home
  DaycareChild,
  Student,
  Teacher,
  Worker,
  HealthcareWorker,  // hospital or urgent care staff
  LtcfStaff,
  LtcfResident,
  HomeAdult,       // not in the labour force
  Retired,
  Count
};

// Every venue where transmission can happen is one of these mixing groups.
// Measles is airborne, so the same machinery (shared air for a block of hours)
// covers a living room, a classroom and an emergency department waiting room -
// only the intensity and the group size differ.
enum class GroupType : std::uint8_t {
  Household = 0,
  DaycareRoom,
  Classroom,
  SchoolCommon,      // hallways, cafeteria, assembly: whole-school mixing
  WorkTeam,
  WorkplaceCommon,
  HospitalWaiting,   // emergency department / outpatient waiting room
  HospitalWard,      // inpatients plus the staff caring for them
  UrgentCareWaiting,
  LtcfUnit,          // nursing home wing: residents plus the staff on shift
  LtcfCommon,        // dining room / activity room
  CommunityHub,      // grocery store, place of worship, retail
  Count
};

constexpr int kGroupTypeCount = static_cast<int>(GroupType::Count);

const char* group_type_name(GroupType t);
const char* health_state_name(HealthState s);
const char* immunity_name(ImmunityStatus s);
const char* role_name(Role r);

// A day is split into four blocks so that agents can move between venues and
// so that exposure duration enters the hazard explicitly.
enum class Block : std::uint8_t {
  EarlyDay = 0,  // waking hours at home
  Daytime,       // school / work / long-term care shift
  Evening,       // errands and community venues
  Night,         // at home asleep
  Count
};

constexpr int kBlockCount = static_cast<int>(Block::Count);

// Hours spent in each block; the hazard scales with contact time.
constexpr double kBlockHours[kBlockCount] = {2.0, 8.0, 3.0, 11.0};

struct Agent {
  int id = -1;
  int age = 0;               // years
  int age_months = 0;        // only meaningful under age 1
  Role role = Role::HomeAdult;

  HealthState state = HealthState::Susceptible;
  ImmunityStatus immunity = ImmunityStatus::None;
  bool protected_by_immunity = false;  // vaccine "take" / prior infection
  double susceptibility = 1.0;         // residual risk multiplier

  // Static group memberships (-1 = not a member).
  int household = -1;
  int daytime_group = -1;   // classroom, work team, LTCF unit, ward, ...
  int secondary_group = -1; // school/workplace common area, LTCF common room
  int community_hub = -1;
  int site_id = -1;         // school / workplace / facility identifier
  int home_hospital = -1;   // hospital an agent would present to
  int home_urgent_care = -1;

  // Infection timeline (day indices; -1 when not applicable).
  int day_exposed = -1;
  int day_prodromal = -1;
  int day_rash = -1;
  int day_recover = -1;
  int infector = -1;
  GroupType acquired_in = GroupType::Household;
  int secondary_infections = 0;
  int generation = 0;

  // Clinical course.
  bool severe = false;         // complication requiring admission
  bool will_die = false;
  bool hospitalized = false;
  int day_admitted = -1;
  int day_discharge = -1;
  int assigned_hospital = -1;
  int pre_admission_group = -1;  // daytime group to return to after discharge

  // Detection and control.
  bool detected = false;
  int day_detected = -1;
  bool isolated = false;
  int isolation_until = -1;
  int quarantine_until = -1;   // excluded from school/work, susceptible contact
  bool vaccinated_during_response = false;
  int vaccine_effective_day = -1;  // seroconversion date for an outbreak dose
  bool hesitant_household = false;

  // Health-care seeking scheduled for today (-1 = none).
  int visit_group_today = -1;
  Block visit_block_today = Block::Daytime;

  bool alive() const { return state != HealthState::Dead; }
  bool infectious() const {
    return state == HealthState::Prodromal || state == HealthState::Rash;
  }
};

// A mixing group: a set of people who share air for one or more blocks a day.
struct ContactGroup {
  GroupType type = GroupType::Household;
  int site_id = -1;              // school, workplace, hospital, facility id
  std::vector<int> members;      // static roster (waiting rooms stay empty)
  double environment = 0.0;      // lingering airborne virus (measles hallmark)
  int infections_here = 0;
};

}  // namespace measles
