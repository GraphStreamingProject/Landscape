#include <streambuf>
#include <istream>

/*
 * An implementation of std::istream that does not copy memory buffers
 * or perform memory allocation
 */

struct membuf : std::streambuf {
  membuf(char *buf, size_t size) {
    this->setg(buf, buf, buf+size);
  }
};

struct imemstream : virtual membuf, std::istream {
  imemstream(char *buf, size_t size) : membuf(buf, size), 
    std::istream(static_cast<std::streambuf*>(this)) {}
};
