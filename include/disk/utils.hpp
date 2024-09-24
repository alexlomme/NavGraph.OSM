#include <sstream>
#include <string>

std::string numFilename(const std::string& filename, uint64_t num,
                        std::string ext) {
  std::ostringstream stream;
  stream << filename << std::to_string(num) << "." << ext;
  return stream.str();
}