// Key/value configuration file loading with typed accessors.
#pragma once

#include <map>
#include <set>
#include <string>
#include <vector>

namespace measles {

// A flat `key = value` store populated from a config file and/or `--set k=v`
// command line overrides. Every lookup is recorded so unused (typically
// misspelled) keys can be reported back to the user instead of silently
// ignored.
class Config {
 public:
  // Parses `key = value` lines; `#` and `;` start a comment. Returns false and
  // fills `error` if the file cannot be read or a line is malformed.
  bool load_file(const std::string& path, std::string& error);

  // Parses a single "key=value" override string.
  bool set_from_assignment(const std::string& assignment, std::string& error);

  void set(const std::string& key, const std::string& value) { values_[key] = value; }

  bool has(const std::string& key) const { return values_.count(key) > 0; }

  double get_double(const std::string& key, double fallback) const;
  int get_int(const std::string& key, int fallback) const;
  unsigned long long get_uint64(const std::string& key, unsigned long long fallback) const;
  bool get_bool(const std::string& key, bool fallback) const;
  std::string get_string(const std::string& key, const std::string& fallback) const;

  // Keys present in the config that were never read back. Anything listed here
  // is almost certainly a typo.
  std::vector<std::string> unused_keys() const;

  const std::map<std::string, std::string>& values() const { return values_; }

 private:
  std::map<std::string, std::string> values_;
  mutable std::set<std::string> read_keys_;
};

}  // namespace measles
