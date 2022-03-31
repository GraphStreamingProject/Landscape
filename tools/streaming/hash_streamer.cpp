#include <iostream>
#include <cstdlib>
#include <cmath>

#include "hash_streamer.h"

constexpr GraphUpdate NO_UPDATE = {{1,1}, DELETE};

using ull = uint64_t;
std::pair<ull, ull> inv_pairing_fn(ull idx) {
  ull eidx = 8ull*idx + 1ull;
  eidx = sqrt(eidx)+1ull;
  eidx/=2ull;
  ull i,j = (ull) eidx;
  if ((j & 1ull) == 0ull) i = idx-(j>>1ull)*(j-1ull);
  else i = idx-j*((j-1ull)>>1ull);
  return {i, j};
}

HashStreamer::HashStreamer(node_id_t num_nodes, edge_id_t num_updates,
                           edge_id_t prime, double er_prob,
                           int rounds, long seed1, long seed2) : num_nodes(num_nodes),
                                                                 num_updates(num_updates), prime(prime), p(er_prob),
                                                                 rounds(rounds), seed1(seed1), seed2(seed2) {
  num_edges = ((edge_id_t) num_nodes)*((edge_id_t) num_nodes - 1)/2ull;
  cutoff = (vec_hash_t) std::round(0x1000000*p); // surprisingly, this works
  // init generator values
  _step = prime / 13ull;
  _curr = 0;
  _curr_round = 0;
}

HashStreamer::HashStreamer(node_id_t num_nodes, edge_id_t prime, double er_prob,
                           int rounds, long seed1, long seed2)
      : HashStreamer(num_nodes, 0, prime,
                     er_prob, rounds, seed1, seed2) {
}


node_id_t HashStreamer::nodes() const { return num_nodes; }

edge_id_t HashStreamer::stream_length() const {
  if (num_updates) return num_updates;
  std::cout << "This is an experimental stream! Cannot get stream length as "
               "it is still undetermined." << std::endl;
  exit(EXIT_FAILURE);
}

// based on the round, either insert or delete
GraphUpdate HashStreamer::get_edge() {
  vec_hash_t hashed = vec_hash(&_curr, sizeof(edge_id_t), seed1);
  if (_curr_round == 0) {
    auto ins = hashed & 1;
    if (ins) {
      auto pa = inv_pairing_fn(_curr);
      ++wi;
      return {pa, INSERT};
    }
  } else {
    auto curr = (hashed & (1 << _curr_round)) >> _curr_round;
    auto prev = (hashed & (1 << (_curr_round - 1))) >> (_curr_round - 1);
    if (curr ^ prev) {
      auto pa = inv_pairing_fn(_curr);
      if (curr) ++wi;
      else ++wd;
      return {pa, (curr) ? INSERT : DELETE};
    }
  }

  return NO_UPDATE; // default to represent "nothing"
}

GraphUpdate HashStreamer::correct_edge() {
  vec_hash_t hashed = vec_hash(&_curr, sizeof(edge_id_t), seed1);
  vec_hash_t filter = vec_hash(&_curr, sizeof(edge_id_t), seed2);
  filter &= 0xffffff; // take lower 16 bits
  hashed &= 1 << (rounds - 2);
  if ((hashed == 0) ^ (filter > cutoff)) { // not inserted xor should not be inserted
    // edge is not in proper state
    auto pa = inv_pairing_fn(_curr);
    if (hashed) ++cd;
    else ++ci;
    return {pa, (hashed) ? DELETE : INSERT}; // del if present, ins if not
  }

  return NO_UPDATE;
}


// puts the next step and round values in _curr and _curr_round, resp
// _curr_round will be set to -1 if there are no more values to generate
void HashStreamer::advance_state_to_next_value() {
  while (true) {
    do {
      _curr += _step;
      _curr %= prime;
    } while (_curr > num_edges);
    if (_curr != 0) return;
    ++_curr_round;
    if (_curr_round == rounds) {
      _curr_round = -1;
      return;
    }
  }
}

GraphUpdate HashStreamer::next() {
  while (true) {
    advance_state_to_next_value();
    if (_curr_round == -1) return NO_UPDATE;
    if (_curr_round < rounds - 1) {
      // write edge
      auto upd = get_edge();
      if (upd != NO_UPDATE) return upd;
    } else {
      // correct edge
      auto upd = correct_edge();
      if (upd != NO_UPDATE) return upd;
    }
  }
}

void HashStreamer::dump_edges() {
  auto write_edge = [this]() {
      auto upd = get_edge();
      if ((upd.first.first ^ upd.first.second) == 0) return; // special return
      // code if the edge does not need to be modified
      std::cout << upd.second << "\t"
                << upd.first.first << "\t" << upd.first.second << "\n";
      ++num_updates;
  };
  auto write_correct_edge = [this]() {
      auto upd = correct_edge();
      if ((upd.first.first ^ upd.first.second) == 0) return; // special return
      // code if the edge does not need to be modified
      std::cout << upd.second << "\t"
                << upd.first.first << "\t" << upd.first.second << "\n";
      ++num_updates;
  };

  for (; _curr_round < rounds - 1; ++_curr_round) {
    _curr = _step;
    while (_curr != 0) {
      if (_curr <= num_edges) {
        write_edge();
      }
      _curr += _step;
      _curr %= prime;
    }
  }

  // final round to get everything lined up properly
  _curr = _step;
  while (_curr != 0) {
    if (_curr <= num_edges) {
      write_correct_edge();
    }
    _curr += _step;
    _curr %= prime;
  }

  std::cout << "Num updates: " << num_updates << std::endl;
  std::cout << wi << " " << wd << " " << ci << " " << cd << std::endl;
  std::cout << (wi - wd + ci - cd) / (double) num_edges << "\t" << seed1 << " "
  << seed2 << "\n";
}
