#include "config.hpp"

#include <cstdlib>
#include <fstream>
#include <sstream>

namespace measles {
namespace {

std::string trim(const std::string& s) {
  const char* ws = " \t\r\n";
  const std::size_t b = s.find_first_not_of(ws);
  if (b == std::string::npos) return "";
  const std::size_t e = s.find_last_not_of(ws);
  return s.substr(b, e - b + 1);
}

std::string strip_comment(const std::string& s) {
  const std::size_t p = s.find_first_of("#;");
  return p == std::string::npos ? s : s.substr(0, p);
}

}  // namespace

bool Config::set_from_assignment(const std::string& assignment, std::string& error) {
  const std::size_t eq = assignment.find('=');
  if (eq == std::string::npos) {
    error = "expected key=value, got '" + assignment + "'";
    return false;
  }
  const std::string key = trim(assignment.substr(0, eq));
  const std::string value = trim(assignment.substr(eq + 1));
  if (key.empty()) {
    error = "empty key in '" + assignment + "'";
    return false;
  }
  values_[key] = value;
  return true;
}

bool Config::load_file(const std::string& path, std::string& error) {
  std::ifstream in(path);
  if (!in) {
    error = "cannot open config file: " + path;
    return false;
  }
  std::string line;
  int lineno = 0;
  while (std::getline(in, line)) {
    ++lineno;
    const std::string body = trim(strip_comment(line));
    if (body.empty()) continue;
    std::string err;
    if (!set_from_assignment(body, err)) {
      error = path + ":" + std::to_string(lineno) + ": " + err;
      return false;
    }
  }
  return true;
}

double Config::get_double(const std::string& key, double fallback) const {
  read_keys_.insert(key);
  const auto it = values_.find(key);
  if (it == values_.end()) return fallback;
  return std::strtod(it->second.c_str(), nullptr);
}

int Config::get_int(const std::string& key, int fallback) const {
  read_keys_.insert(key);
  const auto it = values_.find(key);
  if (it == values_.end()) return fallback;
  return static_cast<int>(std::strtol(it->second.c_str(), nullptr, 10));
}

unsigned long long Config::get_uint64(const std::string& key, unsigned long long fallback) const {
  read_keys_.insert(key);
  const auto it = values_.find(key);
  if (it == values_.end()) return fallback;
  return std::strtoull(it->second.c_str(), nullptr, 10);
}

bool Config::get_bool(const std::string& key, bool fallback) const {
  read_keys_.insert(key);
  const auto it = values_.find(key);
  if (it == values_.end()) return fallback;
  const std::string& v = it->second;
  return v == "1" || v == "true" || v == "yes" || v == "on";
}

std::string Config::get_string(const std::string& key, const std::string& fallback) const {
  read_keys_.insert(key);
  const auto it = values_.find(key);
  if (it == values_.end()) return fallback;
  return it->second;
}

std::vector<std::string> Config::unused_keys() const {
  std::vector<std::string> unused;
  for (const auto& kv : values_) {
    if (read_keys_.count(kv.first) == 0) unused.push_back(kv.first);
  }
  return unused;
}

}  // namespace measles
