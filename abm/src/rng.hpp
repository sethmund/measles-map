// Deterministic random number utilities for the measles ABM.
#pragma once

#include <cmath>
#include <cstdint>
#include <random>
#include <vector>

namespace measles {

// Thin wrapper over mt19937_64 so every draw in the model goes through one
// reproducible stream. Given the same seed the model is bit-for-bit repeatable.
class Rng {
 public:
  explicit Rng(std::uint64_t seed) : engine_(seed) {}

  std::uint64_t raw() { return engine_(); }

  // Uniform on [0, 1).
  double uniform() {
    return std::generate_canonical<double, 53>(engine_);
  }

  double uniform(double lo, double hi) { return lo + (hi - lo) * uniform(); }

  bool bernoulli(double p) {
    if (p <= 0.0) return false;
    if (p >= 1.0) return true;
    return uniform() < p;
  }

  // Uniform integer on [lo, hi] inclusive.
  int uniform_int(int lo, int hi) {
    if (hi <= lo) return lo;
    std::uniform_int_distribution<int> d(lo, hi);
    return d(engine_);
  }

  double normal(double mean, double sd) {
    std::normal_distribution<double> d(mean, sd);
    return d(engine_);
  }

  // Normal truncated to [lo, hi], used for incubation/duration draws where a
  // negative or absurdly long value would be nonsense.
  double normal_trunc(double mean, double sd, double lo, double hi) {
    for (int i = 0; i < 32; ++i) {
      const double x = normal(mean, sd);
      if (x >= lo && x <= hi) return x;
    }
    return std::min(std::max(mean, lo), hi);
  }

  double lognormal(double mu, double sigma) {
    std::lognormal_distribution<double> d(mu, sigma);
    return d(engine_);
  }

  int poisson(double lambda) {
    if (lambda <= 0.0) return 0;
    std::poisson_distribution<int> d(lambda);
    return d(engine_);
  }

  double gamma(double shape, double scale) {
    std::gamma_distribution<double> d(shape, scale);
    return d(engine_);
  }

  // Beta(a, b) via the standard pair-of-gammas construction.
  double beta_dist(double a, double b) {
    const double x = gamma(a, 1.0);
    const double y = gamma(b, 1.0);
    const double s = x + y;
    if (s <= 0.0) return 0.5;
    return x / s;
  }

  int binomial(int n, double p) {
    if (n <= 0 || p <= 0.0) return 0;
    if (p >= 1.0) return n;
    std::binomial_distribution<int> d(n, p);
    return d(engine_);
  }

  // Index sampled proportional to the given (non-negative) weights.
  std::size_t weighted_choice(const std::vector<double>& weights) {
    double total = 0.0;
    for (double w : weights) total += w;
    if (total <= 0.0) return 0;
    double x = uniform() * total;
    for (std::size_t i = 0; i < weights.size(); ++i) {
      x -= weights[i];
      if (x <= 0.0) return i;
    }
    return weights.size() - 1;
  }

  template <typename T>
  void shuffle(std::vector<T>& v) {
    for (std::size_t i = v.size(); i > 1; --i) {
      const std::size_t j = static_cast<std::size_t>(uniform_int(0, static_cast<int>(i) - 1));
      std::swap(v[i - 1], v[j]);
    }
  }

 private:
  std::mt19937_64 engine_;
};

}  // namespace measles
