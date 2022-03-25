#pragma once

#include "graph.h"

class FastStreamer {
private:
  node_id_t num_nodes;
  edge_id_t num_updates;
  edge_id_t prime;
  vec_hash_t cutoff;
  double p;
  int rounds;

  // generator values
  edge_id_t _step;
  edge_id_t _curr_i;
  edge_id_t _j_step;
  edge_id_t _curr_j;
  int _curr_round;

  unsigned seed1;
  unsigned seed2;

  void advance_state_to_next_value();
  GraphUpdate get_edge();
  GraphUpdate correct_edge();

public:
  FastStreamer(node_id_t num_nodes, edge_id_t num_updates, edge_id_t prime,
               double er_prob, int rounds, long seed1, long seed2);

  FastStreamer(node_id_t num_nodes, edge_id_t prime, double er_prob, int
  rounds, long seed1, long seed2);

  node_id_t nodes() const;
  edge_id_t stream_length() const;

  GraphUpdate next();

  void dump_edges();

};