#include "types.hpp"

namespace measles {

const char* group_type_name(GroupType t) {
  switch (t) {
    case GroupType::Household: return "household";
    case GroupType::DaycareRoom: return "daycare";
    case GroupType::Classroom: return "classroom";
    case GroupType::SchoolCommon: return "school_common";
    case GroupType::WorkTeam: return "work_team";
    case GroupType::WorkplaceCommon: return "workplace_common";
    case GroupType::HospitalWaiting: return "hospital_waiting";
    case GroupType::HospitalWard: return "hospital_ward";
    case GroupType::UrgentCareWaiting: return "urgent_care";
    case GroupType::LtcfUnit: return "ltcf_unit";
    case GroupType::LtcfCommon: return "ltcf_common";
    case GroupType::CommunityHub: return "community";
    default: return "unknown";
  }
}

const char* health_state_name(HealthState s) {
  switch (s) {
    case HealthState::Susceptible: return "susceptible";
    case HealthState::Exposed: return "exposed";
    case HealthState::Prodromal: return "prodromal";
    case HealthState::Rash: return "rash";
    case HealthState::Recovered: return "recovered";
    case HealthState::Dead: return "dead";
    default: return "unknown";
  }
}

const char* immunity_name(ImmunityStatus s) {
  switch (s) {
    case ImmunityStatus::None: return "none";
    case ImmunityStatus::Maternal: return "maternal";
    case ImmunityStatus::OneDose: return "one_dose";
    case ImmunityStatus::TwoDose: return "two_dose";
    case ImmunityStatus::Natural: return "natural";
    default: return "unknown";
  }
}

const char* role_name(Role r) {
  switch (r) {
    case Role::Infant: return "infant";
    case Role::DaycareChild: return "daycare_child";
    case Role::Student: return "student";
    case Role::Teacher: return "teacher";
    case Role::Worker: return "worker";
    case Role::HealthcareWorker: return "healthcare_worker";
    case Role::LtcfStaff: return "ltcf_staff";
    case Role::LtcfResident: return "ltcf_resident";
    case Role::HomeAdult: return "home_adult";
    case Role::Retired: return "retired";
    default: return "unknown";
  }
}

}  // namespace measles
