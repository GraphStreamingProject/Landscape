#include "gz_nonsequential_streamer.h"

#include <iostream>
#include <cstdlib>
#include <cmath>

constexpr GraphUpdate NO_UPDATE = {{1,1}, DELETE};

GZNonsequentialStreamer::GZNonsequentialStreamer(node_id_t num_nodes, edge_id_t num_updates,
                           edge_id_t prime, double er_prob,
                           int rounds, long seed1, long seed2) : num_nodes(num_nodes),
                                                                 num_updates(num_updates), prime(prime), p(er_prob),
                                                                 rounds(rounds), seed1(seed1), seed2(seed2) {
  cutoff = (vec_hash_t) std::round(0x1000000*p); // surprisingly, this works
  // init generator values
  _step = prime / 13ull;
  _curr_i = 0;
  _j_step = 1;
  _curr_j = 0;
  _curr_round = 0;
}

GZNonsequentialStreamer::GZNonsequentialStreamer(node_id_t num_nodes, edge_id_t prime, double er_prob,
                           int rounds, long seed1, long seed2)
      : GZNonsequentialStreamer(num_nodes, 0, prime,
                     er_prob, rounds, seed1, seed2) {
}


node_id_t GZNonsequentialStreamer::nodes() const { return num_nodes; }

edge_id_t GZNonsequentialStreamer::stream_length() const {
  if (num_updates) return num_updates;
  std::cout << "This is an experimental stream! Cannot get stream length as "
               "it is still undetermined." << std::endl;
  exit(EXIT_FAILURE);
}

// based on the round, either insert or delete
GraphUpdate GZNonsequentialStreamer::get_edge() {
  if (_curr_round == 0) {
    const auto val = nondirectional_non_self_edge_pairing_fn(_curr_i, _curr_j);
    col_hash_t hashed = col_hash(&val, sizeof(val), seed1);
    auto ins = hashed & 1;
    if (ins) {
      return {{_curr_i,_curr_j}, INSERT};
    } else return NO_UPDATE;
  } else {
      return {{_curr_i,_curr_j}, INSERT}; // it's whatever, cube sketch will handle it
  }
}

GraphUpdate GZNonsequentialStreamer::correct_edge() {
  const auto val = nondirectional_non_self_edge_pairing_fn(_curr_i, _curr_j);
  col_hash_t hashed = col_hash(&val, sizeof(val), seed1);
  col_hash_t filter = col_hash(&val, sizeof(val), seed2);
  filter &= 0xffffff; // take lower 24 bits
  hashed &= 1;
  if ((hashed == 0) ^ (filter > cutoff)) { // not inserted xor should not be inserted
    // edge is not in proper state
    return {{_curr_i, _curr_j}, (hashed) ? DELETE : INSERT}; // del if present, ins if not
  }

  return NO_UPDATE;
}


// puts the next step and round values in _curr and _curr_round, resp
// _curr_round will be set to -1 if there are no more values to generate
/*
 * j_step <- 1
 * i_step <- some number
 * while (j_step < i_step):
 *   output { i_step * x, j_step * x } % p for all x in (0,p)
 *   increment j_step
 * end while
 */
void GZNonsequentialStreamer::advance_state_to_next_value() {
  while (true) {
    do {
      _curr_i += _step;
      _curr_i %= prime;
      _curr_j += _j_step;
      _curr_j %= prime;
    } while (_curr_i >= num_nodes || _curr_j >= num_nodes);
    if (_curr_i != 0) return;
    ++_j_step;
    if (_j_step != _step) continue;
    _j_step = 1;
    ++_curr_round;
    if (_curr_round == rounds) {
      _curr_round = -1;
      return;
    }
  }
}

GraphUpdate GZNonsequentialStreamer::next() {
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

void GZNonsequentialStreamer::dump_edges() {
  auto write_edge = [this]() {
      auto upd = get_edge();
      if ((upd.first.first ^ upd.first.second) == 0) return; // special return
      // code if the edge does not need to be modified
//      std::cout << upd.second << "\t"
//                << upd.first.first << "\t" << upd.first.second << "\n";
      ++num_updates;
  };
  auto write_correct_edge = [this]() {
      auto upd = correct_edge();
      if ((upd.first.first ^ upd.first.second) == 0) return; // special return
      // code if the edge does not need to be modified
//      std::cout << upd.second << "\t"
//                << upd.first.first << "\t" << upd.first.second << "\n";
      ++num_updates;
  };

  for (; _curr_round < rounds - 1; ++_curr_round) {
    _j_step = 1;
    for (; _j_step < _step; ++_j_step) {
      _curr_i = _step;
      _curr_j = _j_step;
      while (_curr_i != 0) {
        if (_curr_i < num_nodes && _curr_j < num_nodes) {
          write_edge();
        }
        _curr_i += _step;
        _curr_i %= prime;
        _curr_j += _j_step;
        _curr_j %= prime;
      }
    }
  }

  // final round to get everything lined up properly
  _j_step = 1;
  for (; _j_step < _step; ++_j_step) {
    _curr_i = _step;
    _curr_j = _j_step;
    while (_curr_i != 0) {
      if (_curr_i < num_nodes && _curr_j < num_nodes) {
        write_correct_edge();
      }
      _curr_i += _step;
      _curr_i %= prime;
      _curr_j += _j_step;
      _curr_j %= prime;
    }
  }

  std::cout << "Num updates: " << num_updates << std::endl;
  std::cout << seed1 << " " << seed2 << "\n";
}
