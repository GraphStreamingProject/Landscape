#include "gz_sequential_streamer.h"

#include <iostream>
#include <cstdlib>
#include <cmath>

constexpr GraphUpdate NO_UPDATE = {{1,1}, DELETE};

bool has_update(GraphUpdate upd) {
  return upd.type != NO_UPDATE.type || upd.edge.src != NO_UPDATE.edge.src ||
         upd.edge.dst != NO_UPDATE.edge.dst;
}

GZSequentialStreamer::GZSequentialStreamer(node_id_t num_nodes, edge_id_t num_updates,
                                           double er_prob, int rounds, long seed2) :
                           num_nodes(num_nodes),
                           num_updates(num_updates), p(er_prob),
                           rounds(rounds), seed2(seed2) {
  cutoff = (vec_hash_t) std::round(0x1000000*p); // surprisingly, this works
  // init generator values
  _curr_i = 0;
  _curr_j = 0;
  _curr_round = 0;
}

GZSequentialStreamer::GZSequentialStreamer(node_id_t num_nodes, double er_prob, int rounds,
                                           long seed2)
      : GZSequentialStreamer(num_nodes, 0, er_prob, rounds, seed2) {
}


node_id_t GZSequentialStreamer::nodes() const { return num_nodes; }

edge_id_t GZSequentialStreamer::stream_length() const {
  if (num_updates) return num_updates;
  std::cout << "This is an experimental stream! Cannot get stream length as "
               "it is still undetermined." << std::endl;
  exit(EXIT_FAILURE);
}

// based on the round, either insert or delete
GraphUpdate GZSequentialStreamer::get_edge() {
  if (_curr_round == 0) {
    if ((_curr_i + _curr_j) & 1) {
      return {{_curr_i,_curr_j}, INSERT};
    } else return NO_UPDATE;
  } else {
    return {{_curr_i,_curr_j}, INSERT}; // it's whatever, cube sketch will handle it
  }
}

GraphUpdate GZSequentialStreamer::correct_edge() {
  const auto val = nondirectional_non_self_edge_pairing_fn(_curr_i, _curr_j);
  col_hash_t hashed = (_curr_i + _curr_j) & 1;
  col_hash_t filter = col_hash(&val, sizeof(val), seed2);
  filter &= 0xffffff; // take lower 24 bits
  if ((hashed == 0) ^ (filter > cutoff)) { // not inserted xor should not be inserted
    // edge is not in proper state
    return {{_curr_i, _curr_j}, INSERT};
  }

  return NO_UPDATE;
}


// goes in row then col-major order
inline void GZSequentialStreamer::advance_state_to_next_value() {
  while (true) {
    ++_curr_j;
    if (_curr_j < num_nodes) return;

    ++_curr_i;
    _curr_j = _curr_i;
    if (_curr_i != num_nodes) continue;

    ++_curr_round;
    _curr_i = 0;
    _curr_j = 0;
  }
}

GraphUpdate GZSequentialStreamer::next() {
  while (true) {
    advance_state_to_next_value();
    if (_curr_round < rounds - 1) {
      // write edge
      auto upd = get_edge();
      if (not_empty(upd)) return upd;
    } else {
      // correct edge
      auto upd = correct_edge();
      if (not_empty(upd)) return upd;
    }
  }
}

void GZSequentialStreamer::dump_edges() {
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
    for (_curr_i = 0; _curr_i < num_nodes; ++_curr_i) {
      for (_curr_j = _curr_i + 1; _curr_j < num_nodes; ++_curr_j) {
        write_edge();
      }
    }
  }

  // final round to get everything lined up properly
  for (_curr_i = 0; _curr_i < num_nodes; ++_curr_i) {
    for (_curr_j = _curr_i + 1; _curr_j < num_nodes; ++_curr_j) {
      write_correct_edge();
    }
  }

  std::cout << "Num updates: " << num_updates << std::endl;
  std::cout << "Seed: " << seed2 << "\n";
}
