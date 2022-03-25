#pragma once

#include "graph.h"

class WrongAndFast {
private:
  node_id_t num_nodes;
  edge_id_t num_updates;
  vec_hash_t cutoff;
  double p;
  int rounds;

  // generator values
  edge_id_t _curr_i;
  edge_id_t _curr_j;
  int _curr_round;

  unsigned seed2;

  void advance_state_to_next_value();
  GraphUpdate get_edge();
  GraphUpdate correct_edge();

public:
  WrongAndFast(node_id_t num_nodes, edge_id_t num_updates,
               double er_prob, int rounds, long seed2);

  WrongAndFast(node_id_t num_nodes, double er_prob, int
  rounds, long seed2);

  node_id_t nodes() const;
  edge_id_t stream_length() const;

  GraphUpdate next();

  void dump_edges();
};