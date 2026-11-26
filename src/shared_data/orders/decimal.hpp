#pragma once

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

namespace marketlab {
// All v3 wire prices/quantities use this scale, independently of product ticks.
inline constexpr std::int64_t decimal_scale = 100000000;

inline std::int64_t parse_decimal(std::string_view text) {
  if (text.empty()) throw std::invalid_argument("Empty decimal");
  std::uint64_t whole = 0, fraction = 0, places = 0;
  bool point = false, digit = false;
  constexpr auto limit = std::uint64_t(std::numeric_limits<std::int64_t>::max());
  for (const char c : text) {
    if (c == '.' && !point && digit) { point = true; continue; }
    if (c < '0' || c > '9') throw std::invalid_argument("Expected a nonnegative decimal");
    digit = true;
    const unsigned d = c - '0';
    if (!point) {
      if (whole > (limit / decimal_scale - d) / 10)
        throw std::out_of_range("Decimal overflow");
      whole = whole * 10 + d;
    } else if (places < 8) {
      fraction = fraction * 10 + d;
      ++places;
    } else if (d) {
      throw std::invalid_argument("Decimal exceeds eight fractional digits");
    }
  }
  if (!digit || text.back() == '.') throw std::invalid_argument("Incomplete decimal");
  while (places++ < 8) fraction *= 10;
  if (whole > (limit - fraction) / decimal_scale) throw std::out_of_range("Decimal overflow");
  return static_cast<std::int64_t>(whole * decimal_scale + fraction);
}

inline std::string format_decimal(std::uint64_t value) {
  auto result = std::to_string(value / decimal_scale);
  if (const auto remainder = value % decimal_scale) {
    auto fraction = std::to_string(decimal_scale + remainder).substr(1);
    while (fraction.back() == '0') fraction.pop_back();
    result += '.' + fraction;
  }
  return result;
}
}  // namespace marketlab
