#pragma once
#include <charconv>
#include <cctype>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace midas::chronoflow {
// Manifest reader: numbers remain decimal text, so 64-bit IDs never pass through
// double. Reject duplicate keys, excessive nesting, and trailing input.
struct Json {
  enum class Type { Object, Array, String, Number, Literal } type = Type::Literal;
  std::string scalar;
  std::map<std::string, Json> object;
  std::vector<Json> array;
  const Json& at(const std::string& key) const {
    if (type != Type::Object || !object.contains(key))
      throw std::runtime_error("Missing manifest field: " + key);
    return object.at(key);
  }
  std::string text() const {
    if (type != Type::String)
      throw std::runtime_error("Manifest string expected");
    return scalar;
  }
  std::uint64_t number() const {
    std::uint64_t n{};
    const auto r = std::from_chars(scalar.data(), scalar.data() + scalar.size(), n);
    if ((type != Type::String && type != Type::Number) || scalar.empty() || r.ec != std::errc{} ||
        r.ptr != scalar.data() + scalar.size())
      throw std::runtime_error("Manifest unsigned integer expected");
    return n;
  }
};
class JsonReader {
 public:
  explicit JsonReader(std::string_view input) : in_(input) {
  }
  Json parse() {
    auto out = value(0);
    space();
    if (at_ != in_.size())
      fail();
    return out;
  }

 private:
  std::string_view in_;
  std::size_t at_ = 0;
  [[noreturn]] void fail() const {
    throw std::runtime_error("Invalid manifest JSON at byte " + std::to_string(at_));
  }
  void space() {
    while (at_ < in_.size() && std::isspace(static_cast<unsigned char>(in_[at_])))
      ++at_;
  }
  bool take(char c) {
    space();
    if (at_ < in_.size() && in_[at_] == c) {
      ++at_;
      return true;
    }
    return false;
  }
  unsigned hex() {
    if (at_ == in_.size())
      fail();
    const auto c = in_[at_++];
    if (c >= '0' && c <= '9')
      return c - '0';
    if (c >= 'a' && c <= 'f')
      return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
      return c - 'A' + 10;
    fail();
  }
  std::string string() {
    if (!take('"'))
      fail();
    std::string out;
    while (at_ < in_.size()) {
      auto c = in_[at_++];
      if (c == '"')
        return out;
      if (static_cast<unsigned char>(c) < 32)
        fail();
      if (c != '\\') {
        out += c;
        continue;
      }
      if (at_ == in_.size())
        fail();
      c = in_[at_++];
      switch (c) {
        case '"':
        case '\\':
        case '/':
          out += c;
          break;
        case 'b':
          out += '\b';
          break;
        case 'f':
          out += '\f';
          break;
        case 'n':
          out += '\n';
          break;
        case 'r':
          out += '\r';
          break;
        case 't':
          out += '\t';
          break;
        case 'u': {
          unsigned cp = 0;
          for (int i = 0; i < 4; ++i)
            cp = (cp << 4) | hex();
          if (cp >= 0xd800 && cp <= 0xdfff)
            fail();
          if (cp < 128)
            out += static_cast<char>(cp);
          else if (cp < 2048) {
            out += static_cast<char>(0xc0 | (cp >> 6));
            out += static_cast<char>(0x80 | (cp & 63));
          } else {
            out += static_cast<char>(0xe0 | (cp >> 12));
            out += static_cast<char>(0x80 | ((cp >> 6) & 63));
            out += static_cast<char>(0x80 | (cp & 63));
          }
          break;
        }
        default:
          fail();
      }
    }
    fail();
  }
  Json value(unsigned depth) {
    if (depth > 64)
      fail();
    space();
    if (at_ == in_.size())
      fail();
    Json out;
    if (take('{')) {
      out.type = Json::Type::Object;
      if (take('}'))
        return out;
      do {
        auto key = string();
        if (!take(':'))
          fail();
        auto child = value(depth + 1);
        if (!out.object.emplace(std::move(key), std::move(child)).second)
          fail();
        if (take('}'))
          return out;
      } while (take(','));
      fail();
    }
    if (take('[')) {
      out.type = Json::Type::Array;
      if (take(']'))
        return out;
      do {
        out.array.push_back(value(depth + 1));
        if (take(']'))
          return out;
      } while (take(','));
      fail();
    }
    if (in_[at_] == '"') {
      out.type = Json::Type::String;
      out.scalar = string();
      return out;
    }
    const auto begin = at_;
    while (at_ < in_.size() && in_[at_] != ',' && in_[at_] != ']' && in_[at_] != '}' &&
           !std::isspace(static_cast<unsigned char>(in_[at_])))
      ++at_;
    out.scalar = std::string(in_.substr(begin, at_ - begin));
    if (out.scalar == "true" || out.scalar == "false" || out.scalar == "null")
      return out;
    if (out.scalar.empty())
      fail();
    // These manifests contain integers only; never accept nan/inf or exponents.
    out.type = Json::Type::Number;
    auto digits = std::string_view(out.scalar);
    if (digits.front() == '-')
      digits.remove_prefix(1);
    if (digits.empty() || (digits.size() > 1 && digits.front() == '0'))
      fail();
    for (char c : digits)
      if (c < '0' || c > '9')
        fail();
    return out;
  }
};
}  // namespace midas::chronoflow
