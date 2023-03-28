#pragma once

#include <iostream>
#include <streambuf>
#include <cassert>

/*
 * Inspiration for these classes from
 * https://blog.csdn.net/tangyin025/article/details/50487544
 */

/*
 * An implementation of std::istream that does not copy memory buffers
 * or perform memory allocation
 */
class imembuf : public std::streambuf {
 public:
  imembuf(char* buf, size_t size) : begin(buf), end(buf + size), current(buf) {}
  imembuf(const imembuf&) = delete;
  imembuf& operator=(const imembuf&) = delete;

  int_type underflow() {
    if (current == end) return traits_type::eof();
    return traits_type::to_int_type(*current);
  }
  int_type uflow() {
    if (current == end) return traits_type::eof();
    return traits_type::to_int_type(*current++);
  }
  int_type pbackfail(int_type ch) {
    if (current == begin || (ch != traits_type::eof() && ch != current[-1]))
      return traits_type::eof();
    return traits_type::to_int_type(*--current);
  }
  std::streamsize showmanyc() {
    if (current <= end) return end - current;
    return 0;
  }

  const char* const begin;
  const char* const end;
  const char* current;
};

class imemstream : public std::istream {
 private:
  imembuf input_buf;

 public:
  imemstream(char* buf, size_t size) : std::istream(&input_buf), input_buf(buf, size) {}

  std::streampos tellg() { return input_buf.current - input_buf.begin; }
};

/*
 * An implementation of std::ostream that does not copy memory buffers
 * or perform memory allocation
 */
class omembuf : public std::streambuf {
 public:
  omembuf(char* buf, size_t size) : buf(buf), size(size) { reset(); }
  omembuf(omembuf &&other) : buf(std::move(other.buf)), size(std::move(other.size)) {
    // the pointers should be to the same spot!
    this->setg(buf, buf + other.tellp(), buf + size);
    this->setp(buf + other.tellp(), buf + size);
  };

  void reset() {
    this->setg(buf, buf, buf + size);
    this->setp(buf, buf + size);
  }

  std::streampos tellp() { return pptr() - pbase(); }
  size_t capacity() { return size; }

 private:
  // the local buffer is the data itself so it should not fill up
  int_type overflow(int_type ch) {
    if (pptr() < epptr()) {
      *pptr() = ch;
      return ch;
    }
    assert(false);
    return traits_type::eof();
  }
  int sync() { return 0; }  // data is already sync'd

  char* buf;
  size_t size;
};

class omemstream : public std::ostream {
 private:
  omembuf out_buf;

 public:
  omemstream(omemstream &&o) : std::ostream(&out_buf), out_buf(std::move(o.out_buf)) {}
  omemstream(char* buf, size_t size) : std::ostream(&out_buf), out_buf(buf, size) {}
  void reset() { out_buf.reset(); }

  std::streampos tellp() { return out_buf.tellp(); }
};
