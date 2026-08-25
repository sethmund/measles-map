// CSV and human-readable reporting of simulation output.
#pragma once

#include <string>

#include "model.hpp"
#include "params.hpp"

namespace measles {

// Creates `dir` (and parents) if needed; returns false on failure.
bool ensure_directory(const std::string& dir, std::string& error);

bool write_timeseries(const std::string& path, const std::vector<DayRecord>& history,
                      std::string& error);
bool write_linelist(const std::string& path, const Model& model, std::string& error);
bool write_setting_table(const std::string& path, const Summary& s, std::string& error);
bool write_age_table(const std::string& path, const Summary& s, std::string& error);
bool write_text(const std::string& path, const std::string& body, std::string& error);

// Formats the end-of-run report printed to the terminal.
std::string format_summary(const Summary& s, const Params& p);

}  // namespace measles
