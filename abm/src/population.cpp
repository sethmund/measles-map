#include "population.hpp"

#include <algorithm>
#include <cmath>
#include <sstream>

namespace measles {
namespace {

constexpr int kReferenceYear = 2026;

int school_level_for_age(int age) {
  if (age <= 10) return 0;   // elementary
  if (age <= 13) return 1;   // middle
  return 2;                  // high
}

// Draws the ages of one household. The mixture of household types below is
// tuned to land near a typical North American age pyramid (~24% under 18,
// ~16% aged 65+) with a mean household size of about 2.4.
std::vector<int> draw_household(Rng& rng) {
  std::vector<int> ages;
  const double u = rng.uniform();
  if (u < 0.28) {
    // Single-person household.
    ages.push_back(rng.bernoulli(0.25) ? rng.uniform_int(65, 92) : rng.uniform_int(20, 64));
  } else if (u < 0.61) {
    // Couple without children at home.
    if (rng.bernoulli(0.35)) {
      const int a = rng.uniform_int(65, 90);
      ages.push_back(a);
      ages.push_back(std::min(95, std::max(62, a + rng.uniform_int(-4, 4))));
    } else {
      const int a = rng.uniform_int(24, 64);
      ages.push_back(a);
      ages.push_back(std::min(70, std::max(20, a + rng.uniform_int(-5, 5))));
    }
  } else if (u < 0.87) {
    // One or two parents with children.
    const int parents = rng.bernoulli(0.72) ? 2 : 1;
    const int parent_age = rng.uniform_int(26, 52);
    for (int i = 0; i < parents; ++i) {
      ages.push_back(std::max(20, parent_age + rng.uniform_int(-4, 4)));
    }
    const int kids = 1 + rng.weighted_choice({0.42, 0.36, 0.15, 0.07});  // 1-4 children
    for (int i = 0; i < kids; ++i) {
      ages.push_back(rng.uniform_int(0, 17));
    }
  } else if (u < 0.94) {
    // Multigenerational: grandparent, parents, children.
    const int parent_age = rng.uniform_int(30, 55);
    ages.push_back(parent_age);
    ages.push_back(std::max(20, parent_age + rng.uniform_int(-4, 4)));
    const int kids = 1 + rng.weighted_choice({0.4, 0.4, 0.2});
    for (int i = 0; i < kids; ++i) ages.push_back(rng.uniform_int(0, 17));
    ages.push_back(rng.uniform_int(66, 92));
  } else {
    // Shared housing among young adults.
    const int n = rng.uniform_int(2, 4);
    for (int i = 0; i < n; ++i) ages.push_back(rng.uniform_int(18, 34));
  }
  return ages;
}

// Assigns `members` to fixed-size sub-groups of `site_groups`, creating the
// groups on demand so every sub-group is close to `target_size`.
void fill_subgroups(World& w, const std::vector<int>& members, std::vector<int>& site_groups,
                    GroupType type, int site_id, int target_size,
                    std::vector<int>& out_group_of_member) {
  out_group_of_member.assign(members.size(), -1);
  const int needed = std::max(1, static_cast<int>((members.size() + target_size - 1) / target_size));
  while (static_cast<int>(site_groups.size()) < needed) {
    site_groups.push_back(w.new_group(type, site_id));
  }
  for (std::size_t i = 0; i < members.size(); ++i) {
    const int g = site_groups[i % site_groups.size()];
    w.join(g, members[i]);
    out_group_of_member[i] = g;
  }
}

}  // namespace

World build_world(const Params& p, Rng& rng) {
  World w;
  w.agents.reserve(static_cast<std::size_t>(p.population));

  // ---- Households ---------------------------------------------------------
  while (static_cast<int>(w.agents.size()) < p.population) {
    const std::vector<int> ages = draw_household(rng);
    std::vector<int> roster;
    for (int age : ages) {
      if (static_cast<int>(w.agents.size()) >= p.population) break;
      Agent a;
      a.id = static_cast<int>(w.agents.size());
      a.age = age;
      a.age_months = (age == 0) ? rng.uniform_int(0, 11) : 12 * age;
      a.household = static_cast<int>(w.household_rosters.size());
      roster.push_back(a.id);
      w.agents.push_back(a);
    }
    if (!roster.empty()) w.household_rosters.push_back(roster);
  }
  const int n_people = static_cast<int>(w.agents.size());

  // ---- Long-term care facilities -----------------------------------------
  // Residents are drawn out of private households: they live, eat and sleep in
  // their facility unit, which is why an introduction there spreads so fast.
  const int n_ltcf = std::max(1, static_cast<int>(std::lround(
                                   static_cast<double>(n_people) / p.people_per_ltcf)));
  w.ltcfs.resize(static_cast<std::size_t>(n_ltcf));
  for (int i = 0; i < n_ltcf; ++i) {
    w.ltcfs[i].name = "ltcf_" + std::to_string(i);
    w.ltcfs[i].common_group = w.new_group(GroupType::LtcfCommon, i);
  }
  std::vector<int> ltcf_residents;
  for (Agent& a : w.agents) {
    if (a.age < 65) continue;
    const double rate = (a.age >= 80) ? p.ltcf_residency_rate_80plus : p.ltcf_residency_rate_65to79;
    if (rng.bernoulli(rate)) ltcf_residents.push_back(a.id);
  }
  for (std::size_t i = 0; i < ltcf_residents.size(); ++i) {
    const int facility = static_cast<int>(i % w.ltcfs.size());
    w.ltcfs[static_cast<std::size_t>(facility)].residents.push_back(ltcf_residents[i]);
  }
  for (int f = 0; f < n_ltcf; ++f) {
    Ltcf& fac = w.ltcfs[static_cast<std::size_t>(f)];
    std::vector<int> group_of;
    fill_subgroups(w, fac.residents, fac.units, GroupType::LtcfUnit, f, p.ltcf_unit_size, group_of);
    for (std::size_t i = 0; i < fac.residents.size(); ++i) {
      Agent& a = w.agents[static_cast<std::size_t>(fac.residents[i])];
      a.role = Role::LtcfResident;
      a.site_id = f;
      a.daytime_group = group_of[i];
      a.secondary_group = fac.common_group;
      a.household = -1;  // no longer part of a private household
    }
  }

  // ---- Household contact groups ------------------------------------------
  w.household_group_of.assign(w.household_rosters.size(), -1);
  for (std::size_t h = 0; h < w.household_rosters.size(); ++h) {
    std::vector<int> remaining;
    for (int id : w.household_rosters[h]) {
      if (w.agents[static_cast<std::size_t>(id)].household >= 0) remaining.push_back(id);
    }
    if (remaining.empty()) continue;
    const int g = w.new_group(GroupType::Household, static_cast<int>(h));
    w.household_group_of[h] = g;
    for (int id : remaining) w.join(g, id);
  }

  // ---- Children: daycare and school --------------------------------------
  std::vector<int> daycare_kids;
  std::vector<std::vector<int>> students_by_level(3);
  for (Agent& a : w.agents) {
    if (a.role == Role::LtcfResident) continue;
    if (a.age < 1) {
      a.role = Role::Infant;
    } else if (a.age < 5) {
      if (rng.bernoulli(p.daycare_attendance)) {
        a.role = Role::DaycareChild;
        daycare_kids.push_back(a.id);
      } else {
        a.role = Role::Infant;  // cared for at home
      }
    } else if (a.age < 18) {
      a.role = Role::Student;
      students_by_level[static_cast<std::size_t>(school_level_for_age(a.age))].push_back(a.id);
    }
  }

  const int level_size[3] = {p.school_size_elementary, p.school_size_middle, p.school_size_high};
  for (int level = 0; level < 3; ++level) {
    std::vector<int>& kids = students_by_level[static_cast<std::size_t>(level)];
    if (kids.empty()) continue;
    rng.shuffle(kids);
    const int n_schools = std::max(1, static_cast<int>((kids.size() + level_size[level] - 1) /
                                                       level_size[level]));
    const int first = static_cast<int>(w.schools.size());
    for (int s = 0; s < n_schools; ++s) {
      School sch;
      sch.level = level;
      sch.name = std::string(level == 0 ? "elementary_" : level == 1 ? "middle_" : "high_") +
                 std::to_string(s);
      sch.common_group = w.new_group(GroupType::SchoolCommon, first + s);
      w.schools.push_back(sch);
    }
    for (std::size_t i = 0; i < kids.size(); ++i) {
      const int school_index = first + static_cast<int>(i % static_cast<std::size_t>(n_schools));
      Agent& a = w.agents[static_cast<std::size_t>(kids[i])];
      a.site_id = school_index;
      a.secondary_group = w.schools[static_cast<std::size_t>(school_index)].common_group;
    }
    // Classrooms are formed within each school and kept age-homogeneous by
    // sorting on age before slicing.
    for (int s = 0; s < n_schools; ++s) {
      const int school_index = first + s;
      std::vector<int> roster;
      for (int id : kids) {
        if (w.agents[static_cast<std::size_t>(id)].site_id == school_index) roster.push_back(id);
      }
      std::sort(roster.begin(), roster.end(), [&w](int x, int y) {
        return w.agents[static_cast<std::size_t>(x)].age <
               w.agents[static_cast<std::size_t>(y)].age;
      });
      School& sch = w.schools[static_cast<std::size_t>(school_index)];
      const int n_rooms = std::max(1, static_cast<int>((roster.size() + p.classroom_size - 1) /
                                                       p.classroom_size));
      for (int r = 0; r < n_rooms; ++r) sch.classrooms.push_back(w.new_group(GroupType::Classroom,
                                                                            school_index));
      for (std::size_t i = 0; i < roster.size(); ++i) {
        const int g = sch.classrooms[std::min(sch.classrooms.size() - 1,
                                              i / static_cast<std::size_t>(p.classroom_size))];
        w.join(g, roster[i]);
        w.agents[static_cast<std::size_t>(roster[i])].daytime_group = g;
      }
    }
  }

  if (!daycare_kids.empty()) {
    rng.shuffle(daycare_kids);
    const int n_centers = std::max(1, static_cast<int>((daycare_kids.size() + p.daycare_center_size - 1) /
                                                       p.daycare_center_size));
    w.daycares.resize(static_cast<std::size_t>(n_centers));
    for (int i = 0; i < n_centers; ++i) w.daycares[static_cast<std::size_t>(i)].name =
        "daycare_" + std::to_string(i);
    for (std::size_t i = 0; i < daycare_kids.size(); ++i) {
      const int center = static_cast<int>(i % static_cast<std::size_t>(n_centers));
      Daycare& d = w.daycares[static_cast<std::size_t>(center)];
      const std::size_t slot = i / static_cast<std::size_t>(n_centers);
      const std::size_t room_index = slot / static_cast<std::size_t>(p.daycare_room_size);
      while (d.rooms.size() <= room_index) d.rooms.push_back(w.new_group(GroupType::DaycareRoom, center));
      const int g = d.rooms[room_index];
      w.join(g, daycare_kids[i]);
      Agent& a = w.agents[static_cast<std::size_t>(daycare_kids[i])];
      a.site_id = center;
      a.daytime_group = g;
    }
  }

  // ---- Adults: who works, and where --------------------------------------
  std::vector<int> labour_pool;
  for (Agent& a : w.agents) {
    if (a.role != Role::HomeAdult && a.role != Role::Retired) continue;
    if (a.age < 18) continue;
    bool employed = false;
    if (a.age < 65) {
      employed = rng.bernoulli(p.employment_rate_18_64);
    } else if (a.age < 75) {
      employed = rng.bernoulli(p.employment_rate_65_74);
    }
    if (employed) {
      a.role = Role::Worker;
      labour_pool.push_back(a.id);
    } else {
      a.role = (a.age >= 65) ? Role::Retired : Role::HomeAdult;
    }
  }
  // Adults aged 18-64 that were never touched above default to HomeAdult.
  for (Agent& a : w.agents) {
    if (a.age >= 18 && a.role == Role::HomeAdult && a.age >= 65) a.role = Role::Retired;
  }
  rng.shuffle(labour_pool);
  std::size_t next_worker = 0;
  auto take_worker = [&]() -> int {
    if (next_worker >= labour_pool.size()) return -1;
    return labour_pool[next_worker++];
  };

  // Hospitals: wards plus one emergency/outpatient waiting area each. The
  // waiting room is the classic measles amplifier - an undiagnosed prodromal
  // patient shares air with newborns, oncology patients and the frail elderly.
  const int n_hospitals = std::max(1, static_cast<int>(std::lround(
                                        static_cast<double>(n_people) / p.people_per_hospital)));
  const int total_beds = std::max(n_hospitals * 4,
                                  static_cast<int>(std::lround(n_people * p.hospital_beds_per_1000 / 1000.0)));
  w.hospitals.resize(static_cast<std::size_t>(n_hospitals));
  for (int i = 0; i < n_hospitals; ++i) {
    Hospital& h = w.hospitals[static_cast<std::size_t>(i)];
    h.name = "hospital_" + std::to_string(i);
    h.beds = std::max(4, total_beds / n_hospitals);
    h.waiting_group = w.new_group(GroupType::HospitalWaiting, i);
    const int n_wards = std::max(1, h.beds / 25);
    for (int k = 0; k < n_wards; ++k) h.wards.push_back(w.new_group(GroupType::HospitalWard, i));
  }
  for (int i = 0; i < n_hospitals; ++i) {
    Hospital& h = w.hospitals[static_cast<std::size_t>(i)];
    const int staff = std::max(4, static_cast<int>(std::lround(h.beds * p.hospital_staff_per_bed)));
    for (int s = 0; s < staff; ++s) {
      const int id = take_worker();
      if (id < 0) break;
      Agent& a = w.agents[static_cast<std::size_t>(id)];
      a.role = Role::HealthcareWorker;
      a.site_id = i;
      // Roughly a third of clinical staff work the front door.
      if (s % 3 == 0) {
        a.daytime_group = h.waiting_group;
      } else {
        a.daytime_group = h.wards[static_cast<std::size_t>(s) % h.wards.size()];
      }
      w.join(a.daytime_group, id);
    }
  }

  const int n_urgent = std::max(1, static_cast<int>(std::lround(
                                     static_cast<double>(n_people) / p.people_per_urgent_care)));
  w.urgent_cares.resize(static_cast<std::size_t>(n_urgent));
  for (int i = 0; i < n_urgent; ++i) {
    UrgentCare& u = w.urgent_cares[static_cast<std::size_t>(i)];
    u.name = "urgent_care_" + std::to_string(i);
    u.waiting_group = w.new_group(GroupType::UrgentCareWaiting, i);
    const int staff = std::max(2, static_cast<int>(std::lround(p.urgent_care_staff)));
    for (int s = 0; s < staff; ++s) {
      const int id = take_worker();
      if (id < 0) break;
      Agent& a = w.agents[static_cast<std::size_t>(id)];
      a.role = Role::HealthcareWorker;
      a.site_id = i;
      a.daytime_group = u.waiting_group;
      w.join(u.waiting_group, id);
    }
  }

  // Long-term care staff rotate through resident units and are the usual route
  // by which an outbreak reaches a facility.
  for (int f = 0; f < n_ltcf; ++f) {
    Ltcf& fac = w.ltcfs[static_cast<std::size_t>(f)];
    if (fac.residents.empty() || fac.units.empty()) continue;
    const int staff = std::max(2, static_cast<int>(std::lround(
                                     fac.residents.size() * p.ltcf_staff_per_resident)));
    for (int s = 0; s < staff; ++s) {
      const int id = take_worker();
      if (id < 0) break;
      Agent& a = w.agents[static_cast<std::size_t>(id)];
      a.role = Role::LtcfStaff;
      a.site_id = f;
      a.daytime_group = fac.units[static_cast<std::size_t>(s) % fac.units.size()];
      a.secondary_group = fac.common_group;
      w.join(a.daytime_group, id);
    }
  }

  // Teachers and daycare staff: one adult per classroom or room.
  for (std::size_t s = 0; s < w.schools.size(); ++s) {
    for (int room : w.schools[s].classrooms) {
      const int id = take_worker();
      if (id < 0) break;
      Agent& a = w.agents[static_cast<std::size_t>(id)];
      a.role = Role::Teacher;
      a.site_id = static_cast<int>(s);
      a.daytime_group = room;
      a.secondary_group = w.schools[s].common_group;
      w.join(room, id);
    }
  }
  for (std::size_t d = 0; d < w.daycares.size(); ++d) {
    for (int room : w.daycares[d].rooms) {
      for (int k = 0; k < 2; ++k) {  // two carers per room
        const int id = take_worker();
        if (id < 0) break;
        Agent& a = w.agents[static_cast<std::size_t>(id)];
        a.role = Role::Teacher;
        a.site_id = static_cast<int>(d);
        a.daytime_group = room;
        w.join(room, id);
      }
    }
  }

  // Everyone still in the labour pool goes to an ordinary workplace.
  std::vector<int> remaining_workers(labour_pool.begin() +
                                         static_cast<std::ptrdiff_t>(std::min(next_worker,
                                                                              labour_pool.size())),
                                     labour_pool.end());
  std::size_t placed = 0;
  while (placed < remaining_workers.size()) {
    const double mu = std::log(std::max(2.0, p.workplace_mean_size));
    int size = static_cast<int>(std::lround(rng.lognormal(mu, p.workplace_size_sigma)));
    size = std::max(2, std::min(size, 600));
    size = std::min<int>(size, static_cast<int>(remaining_workers.size() - placed));
    const int site = static_cast<int>(w.workplaces.size());
    Workplace wp;
    wp.name = "workplace_" + std::to_string(site);
    wp.common_group = w.new_group(GroupType::WorkplaceCommon, site);
    w.workplaces.push_back(wp);
    std::vector<int> staff(remaining_workers.begin() + static_cast<std::ptrdiff_t>(placed),
                           remaining_workers.begin() + static_cast<std::ptrdiff_t>(placed + size));
    std::vector<int> group_of;
    fill_subgroups(w, staff, w.workplaces.back().teams, GroupType::WorkTeam, site,
                   p.work_team_size, group_of);
    for (std::size_t i = 0; i < staff.size(); ++i) {
      Agent& a = w.agents[static_cast<std::size_t>(staff[i])];
      a.site_id = site;
      a.daytime_group = group_of[i];
      a.secondary_group = w.workplaces.back().common_group;
      w.join(w.workplaces.back().common_group, staff[i]);
    }
    placed += static_cast<std::size_t>(size);
  }

  // ---- Community venues, and where each family shops ----------------------
  const int n_venues = std::max(1, n_people / p.people_per_community_hub);
  w.venues.resize(static_cast<std::size_t>(n_venues));
  for (int i = 0; i < n_venues; ++i) {
    CommunityVenue& v = w.venues[static_cast<std::size_t>(i)];
    v.name = "venue_" + std::to_string(i);
    v.group = w.new_group(GroupType::CommunityHub, i);
    // Vaccine refusal clusters socially and geographically, so each venue's
    // catchment gets its own refusal propensity rather than the population mean.
    const double m = std::min(0.95, std::max(0.001, p.hesitancy_mean));
    const double k = std::max(0.5, p.hesitancy_clustering);
    v.hesitancy = rng.beta_dist(m * k, (1.0 - m) * k);
  }
  for (std::size_t h = 0; h < w.household_rosters.size(); ++h) {
    const int venue = rng.uniform_int(0, n_venues - 1);
    const bool hesitant = rng.bernoulli(w.venues[static_cast<std::size_t>(venue)].hesitancy);
    for (int id : w.household_rosters[h]) {
      Agent& a = w.agents[static_cast<std::size_t>(id)];
      a.community_hub = w.venues[static_cast<std::size_t>(venue)].group;
      a.hesitant_household = hesitant;
      w.join(a.community_hub, id);
    }
  }
  for (Agent& a : w.agents) {
    if (a.community_hub < 0) {
      const int venue = rng.uniform_int(0, n_venues - 1);
      a.community_hub = w.venues[static_cast<std::size_t>(venue)].group;
      w.join(a.community_hub, a.id);
    }
    a.home_hospital = rng.uniform_int(0, n_hospitals - 1);
    a.home_urgent_care = rng.uniform_int(0, n_urgent - 1);
  }

  // ---- Immunity profile ---------------------------------------------------
  auto grant = [&](Agent& a, ImmunityStatus status, double efficacy) {
    a.immunity = status;
    if (rng.bernoulli(efficacy)) {
      a.protected_by_immunity = true;
      a.state = HealthState::Recovered;  // not at risk for the whole run
    }
  };

  for (Agent& a : w.agents) {
    const int birth_year = kReferenceYear - a.age;
    if (a.age < 1) {
      // Maternal antibody protects most newborns, then wanes before the first
      // dose is due at 12 months - the classic vulnerable window.
      const double prot = (a.age_months < 6) ? p.maternal_protection_under6mo
                                             : p.maternal_protection_6to11mo;
      a.immunity = ImmunityStatus::Maternal;
      if (rng.bernoulli(prot)) {
        a.protected_by_immunity = true;
        a.state = HealthState::Recovered;
      }
      continue;
    }
    if (a.age < 18) {
      const double d1 = a.hesitant_household ? p.coverage_hesitant_dose1 : p.coverage_dose1;
      if (rng.bernoulli(d1)) {
        const bool dose2_due = a.age >= 5 || (a.age == 4 && rng.bernoulli(0.5));
        if (dose2_due && rng.bernoulli(p.coverage_dose2_given_dose1)) {
          grant(a, ImmunityStatus::TwoDose, p.ve_two_dose);
        } else {
          grant(a, ImmunityStatus::OneDose, p.ve_one_dose);
        }
      }
      continue;
    }
    if (birth_year <= 1957) {
      // Essentially everyone in these cohorts met measles as a child.
      grant(a, ImmunityStatus::Natural, p.pre1957_natural_immunity);
    } else if (birth_year <= 1989) {
      const double u = rng.uniform();
      if (u < p.adult_1957_1989_two_dose) {
        grant(a, ImmunityStatus::TwoDose, p.ve_two_dose);
      } else if (u < p.adult_1957_1989_two_dose + p.adult_1957_1989_one_dose) {
        grant(a, ImmunityStatus::OneDose, p.ve_one_dose);
      }
    } else {
      if (rng.bernoulli(p.adult_post1989_two_dose)) {
        grant(a, ImmunityStatus::TwoDose, p.ve_two_dose);
      } else if (rng.bernoulli(0.4)) {
        grant(a, ImmunityStatus::OneDose, p.ve_one_dose);
      }
    }
  }
  // Occupational requirements top up health-care and long-term care staff.
  for (Agent& a : w.agents) {
    if (a.protected_by_immunity) continue;
    const double target = (a.role == Role::HealthcareWorker) ? p.healthcare_worker_two_dose
                        : (a.role == Role::LtcfStaff)        ? p.ltcf_staff_two_dose
                                                             : -1.0;
    if (target < 0.0) continue;
    if (rng.bernoulli(target)) grant(a, ImmunityStatus::TwoDose, p.ve_two_dose);
  }

  return w;
}

std::string describe_world(const World& w, const Params& p) {
  (void)p;
  long long susceptible = 0, immune = 0, children = 0, elderly = 0, ltcf_res = 0, hcw = 0;
  long long students = 0, workers = 0, daycare = 0;
  for (const Agent& a : w.agents) {
    if (a.state == HealthState::Susceptible) ++susceptible; else ++immune;
    if (a.age < 18) ++children;
    if (a.age >= 65) ++elderly;
    switch (a.role) {
      case Role::LtcfResident: ++ltcf_res; break;
      case Role::HealthcareWorker: ++hcw; break;
      case Role::Student: ++students; break;
      case Role::DaycareChild: ++daycare; break;
      case Role::Worker: ++workers; break;
      default: break;
    }
  }
  const double n = static_cast<double>(w.agents.size());
  std::ostringstream os;
  os.setf(std::ios::fixed);
  os.precision(1);
  os << "Community: " << w.agents.size() << " people in " << w.household_rosters.size()
     << " households (mean size " << (n / std::max<std::size_t>(1, w.household_rosters.size()))
     << ")\n";
  os << "  age structure   : " << (100.0 * children / n) << "% under 18, "
     << (100.0 * elderly / n) << "% 65+\n";
  os << "  immunity        : " << (100.0 * immune / n) << "% immune at baseline, "
     << susceptible << " susceptible\n";
  os << "  schools         : " << w.schools.size() << " (" << students << " students), "
     << w.daycares.size() << " daycare centres (" << daycare << " children)\n";
  os << "  workplaces      : " << w.workplaces.size() << " (" << workers << " workers)\n";
  int beds = 0;
  for (const Hospital& h : w.hospitals) beds += h.beds;
  os << "  health care     : " << w.hospitals.size() << (w.hospitals.size() == 1 ? " hospital (" : " hospitals (")
     << beds << " beds), " << w.urgent_cares.size()
     << (w.urgent_cares.size() == 1 ? " urgent care, " : " urgent cares, ") << hcw << " staff\n";
  os << "  long-term care  : " << w.ltcfs.size()
     << (w.ltcfs.size() == 1 ? " facility, " : " facilities, ") << ltcf_res << " residents\n";
  os << "  community venues: " << w.venues.size() << ", contact groups total " << w.groups.size()
     << "\n";
  return os.str();
}

}  // namespace measles
