// Synthetic population and facility generation: households, schools, daycares,
// workplaces, hospitals, urgent cares, long-term care facilities and community
// venues, plus the immunity profile of every agent.
#pragma once

#include <string>
#include <vector>

#include "params.hpp"
#include "rng.hpp"
#include "types.hpp"

namespace measles {

struct School {
  std::string name;
  int level = 0;  // 0 elementary, 1 middle, 2 high
  int common_group = -1;
  std::vector<int> classrooms;
};

struct Daycare {
  std::string name;
  std::vector<int> rooms;
};

struct Workplace {
  std::string name;
  int common_group = -1;
  std::vector<int> teams;
};

struct Hospital {
  std::string name;
  int waiting_group = -1;   // emergency department / outpatient waiting area
  std::vector<int> wards;
  int beds = 0;
  int occupancy = 0;
};

struct UrgentCare {
  std::string name;
  int waiting_group = -1;
};

struct Ltcf {
  std::string name;
  int common_group = -1;
  std::vector<int> units;
  std::vector<int> residents;
};

struct CommunityVenue {
  std::string name;
  int group = -1;
  double hesitancy = 0.0;  // local vaccine refusal propensity
};

// The whole simulated community.
struct World {
  std::vector<Agent> agents;
  std::vector<ContactGroup> groups;

  std::vector<School> schools;
  std::vector<Daycare> daycares;
  std::vector<Workplace> workplaces;
  std::vector<Hospital> hospitals;
  std::vector<UrgentCare> urgent_cares;
  std::vector<Ltcf> ltcfs;
  std::vector<CommunityVenue> venues;

  std::vector<std::vector<int>> household_rosters;
  std::vector<int> household_group_of;  // roster index -> contact group id

  int new_group(GroupType type, int site_id) {
    ContactGroup g;
    g.type = type;
    g.site_id = site_id;
    groups.push_back(g);
    return static_cast<int>(groups.size()) - 1;
  }

  void join(int group_id, int agent_id) {
    if (group_id < 0) return;
    groups[static_cast<std::size_t>(group_id)].members.push_back(agent_id);
  }
};

// Builds a complete synthetic community from `p`, consuming randomness from
// `rng`. Deterministic for a fixed seed.
World build_world(const Params& p, Rng& rng);

// One-line-per-category description of the generated community.
std::string describe_world(const World& w, const Params& p);

}  // namespace measles
