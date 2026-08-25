# 🦠 Measles Community ABM (C++)

An agent-based model of measles transmission through a synthetic community:
households, daycares, schools, workplaces, urgent cares, hospitals, long-term
care facilities and community venues, with a vaccination profile by birth cohort
and a configurable public health response.

It is a standalone C++17 program with no third-party dependencies — a companion
to the continental surveillance map in the repository root. The map shows where
cases *are*; this model explores how an introduction into one community *plays
out*, and what changes it.

```
cd abm
make                 # or: cmake -B build && cmake --build build
./measles_abm        # baseline scenario, writes ./results
```

---

## What is modelled

**Agents.** Every person has an age, a household, an immunity status, a daily
activity role and a set of venues they belong to. Ages and household
compositions are drawn from a mixture of household types (single, couple,
family, multigenerational, shared) that reproduces a plausible age pyramid
(~25% under 18, ~16% aged 65+, mean household size 2.4).

**Places.** Transmission happens in *contact groups* — sets of people sharing
air for part of a day:

| Venue | Who is there | Why it matters for measles |
|---|---|---|
| Household | everyone at home | ~90% secondary attack rate among susceptibles |
| Daycare room / classroom | children plus their carers and teachers | the main engine of school-age outbreaks |
| School common areas | the whole school | carries an outbreak between classrooms |
| Work team / workplace common | employed adults | how it reaches parents |
| **Urgent care waiting room** | walk-ins and staff | a prodromal case looks like any winter virus, so it is *not* isolated |
| **Hospital ED/outpatient waiting** | patients seeking care, staff | the classic nosocomial amplifier |
| **Hospital ward** | admitted cases and the staff caring for them | ward exposure until measles is recognised |
| **Long-term care unit / common room** | residents plus the staff on shift | staff-borne introduction into the frailest group |
| Community venue | shops, worship, retail — a local catchment | ties otherwise separate households together |

**The day** is split into four blocks — early morning at home, daytime
(school/work/shift), evening (errands and community venues), night — with 2, 8,
3 and 11 hours respectively. Exposure time enters the hazard directly, so an
eight-hour school day counts for more than a one-hour shop. Weekends empty the
schools and most workplaces; health care and long-term care keep running.

**Airborne persistence.** Measles aerosol stays infectious in a room for up to
two hours after the case has left. Each venue carries a decaying environmental
load, which is what lets a waiting room infect the next patient through the
door even when the index case has already gone home.

**Natural history.** Exposure → rash averages 14 days. A case is infectious for
four days *before* the rash (the prodrome — fever, cough, coryza, conjunctivitis,
indistinguishable from any other viral illness) and four days after. The
prodromal window is where most transmission happens and why detection is always
late.

**Immunity** is assigned by birth cohort, not by a single coverage number:

- born before 1958 — near-universal natural immunity;
- 1958–1989 — the one-dose era, a mix of one and two doses;
- 1990 onward — two-dose schedule;
- children — dose 1 at 12 months, dose 2 at 4–6 years, subject to coverage;
- infants under 1 — waning maternal antibody, mostly unprotected after 6 months;
- health-care and long-term care staff — occupational two-dose requirements.

Vaccine efficacy is all-or-nothing (93% for one dose, 97% for two): a "take"
makes an agent immune for the run, a failure leaves them fully susceptible.
Refusal is **clustered**, not spread evenly: each community venue's catchment
draws its own refusal propensity, and households inherit it, so susceptibility
pools the way it does in reality.

**Care seeking.** Prodromal cases visit urgent care at a configurable rate;
rash cases seek care at an emergency department or urgent care. A background
stream of routine visits (about three per person per year, skewed towards the
very young and the old) populates the waiting rooms they walk into.

**Severity** is age-dependent: hospitalisation risk peaks in infants and the
elderly, case fatality likewise. Admitted cases occupy hospital beds and mix
with ward staff until measles is recognised.

**Response.** Cases are reported with a delay, after which the case is isolated,
household contacts are quarantined and offered post-exposure prophylaxis, and
susceptible classmates are excluded for 21 days. Once a threshold of reported
cases is crossed, a community-wide response ramps up: airborne precautions in
health care, restricted communal activity in long-term care, and catch-up
vaccination of the unvaccinated (reaching hesitant households far less often).

---

## Usage

```
./measles_abm [options]

  --config PATH        read parameters from a key=value file
  --set KEY=VALUE      override one parameter (repeatable)
  --seed N             random seed
  --days N             days to simulate
  --pop N              community size
  --out DIR            output directory (default: results)
  --mode MODE          run (default) | r0 | sweep
  --replicates N       replicates for r0 / sweep modes
  --sweep KEY=V1,V2,.. parameter values to sweep (mode sweep)
  --no-linelist        skip writing the per-case line list
  --quiet              only print the final summary
```

Scenarios shipped in `config/`:

```bash
./measles_abm --config config/baseline.cfg                       # typical coverage
./measles_abm --config config/low_coverage.cfg                   # clustered refusal, slow response
./measles_abm --config config/no_response.cfg                    # no public health action at all
```

Any parameter can be overridden without editing a file:

```bash
./measles_abm --set coverage_dose1=0.80 --set school_exclusion=false --seed 7
```

Two analysis modes come built in:

```bash
./measles_abm --mode r0 --replicates 40
./measles_abm --mode sweep --sweep coverage_dose1=0.98,0.95,0.92,0.88,0.80 --replicates 5
```

`--mode r0` seeds single index cases into an otherwise fully susceptible copy of
the community and counts their direct offspring. `--mode sweep` re-runs the
whole model across values of any parameter and writes `sweep.csv`.

---

## Output

Written to `--out` (default `results/`):

| File | Contents |
|---|---|
| `timeseries.csv` | one row per day: state counts, incidence, admissions, deaths, doses, isolation and quarantine counts, and new infections broken down by venue |
| `linelist.csv` | one row per infection: age, role, immunity status, day infected, day of rash, infector, generation, venue, outcome |
| `infections_by_setting.csv` | total infections and share by venue |
| `infections_by_age.csv` | infections and incidence per 1000 by age band |
| `summary.txt` | the terminal report |

In `timeseries.csv`, `susceptible` counts people genuinely at risk and `immune`
counts everyone protected — vaccine-immune, naturally immune and recovered
alike — so `immune` starts high rather than at zero.

---

## Calibration and validation

The household hazard is anchored to the ~90% secondary attack rate reported
among susceptible household contacts. The remaining venue hazards were
calibrated so the assembled model reproduces a basic reproduction number near
the 12–18 usually quoted for measles:

```
$ ./measles_abm --mode r0 --replicates 40
  R0 = 17.10 (sd 10.52, n = 40)
  Secondary infections per index case, by setting:
    household            1.98
    classroom            3.75
    community            3.33
    work_team            2.08
    urgent_care          1.50
    daycare              1.05
    hospital_ward        0.97
    ltcf_unit            0.78
    hospital_waiting     0.60
    ...
```

The large spread across index cases is the point, not noise: a case whose child
attends school infects an order of magnitude more people than a retiree who
stays home. Estimates vary by a few units between seeds and community sizes.

`make test` (or `ctest`) runs the checks in `tests/test_abm.cpp`: config
parsing and validation, synthetic-population invariants, run-to-run determinism
under a fixed seed, zero transmission in a fully immune community, a
near-complete epidemic in an unprotected one, the shape of the natural history
timeline, that the response reduces the final size, and that R0 lands in a
plausible range.

Behaviour worth sanity-checking against the literature, from the shipped
scenarios: roughly one in five reported cases admitted, case fatality of the
order of one per thousand, most infections among school-age children, and
long-term care residents largely spared because their birth cohorts carry
natural immunity — while the staff who care for them do not automatically.

---

## Layout

```
abm/
  src/
    types.hpp/.cpp        agents, contact groups, enums
    rng.hpp               seeded random draws
    config.hpp/.cpp       key=value configuration with unknown-key warnings
    params.hpp/.cpp       every parameter and its default, plus validation
    population.hpp/.cpp   synthetic households, facilities and immunity
    model.hpp/.cpp        the daily loop: progression, movement, transmission, response
    reporter.hpp/.cpp     CSV and terminal reporting
    main.cpp              command line
  tests/test_abm.cpp      self-checks
  config/                 scenario files
```

A 60,000-person community for a year runs in about three seconds on one core.

---

## Limitations

Worth stating plainly, since the outputs look precise:

- **Not fitted to any real place.** Household mixtures, facility ratios and
  contact intensities are plausible defaults, not estimates for a specific
  county. Use the configuration to fit a community you actually have data for.
- **All-or-nothing vaccine efficacy.** There is no partial protection and no
  modified (attenuated) measles in vaccinated agents beyond a reduced chance of
  admission.
- **Closed population.** No births, ageing, deaths from other causes, or
  migration; the only inflow is the importation rate.
- **Static schedules.** Agents do not change jobs, schools or households, and
  behaviour does not change in response to the outbreak beyond the modelled
  interventions.
- **Hospital beds are not contested.** Occupancy is reported against bed counts,
  but non-measles demand is not simulated, so "over capacity" understates strain.
- **Post-exposure prophylaxis protects contacts who are not yet infected.** It
  does not abort an infection already incubating.
- Results are stochastic and heavy-tailed. A single run is an anecdote — use
  `--mode sweep` or several seeds before drawing a conclusion.
