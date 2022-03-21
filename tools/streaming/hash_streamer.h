#pragma once

#include "graph.h"

class HashStreamer {
private:
  node_id_t num_nodes;
  edge_id_t num_edges;
  edge_id_t num_updates;
  edge_id_t prime;
  vec_hash_t cutoff;
  double p;
  int rounds;

  // generator values
  edge_id_t _step;
  edge_id_t _curr;
  int _curr_round;

  unsigned seed1;
  unsigned seed2;
  int wi = 0;
  int wd = 0;
  int ci = 0;
  int cd = 0;

  void advance_state_to_next_value();
  GraphUpdate get_edge();
  GraphUpdate correct_edge();

public:
  HashStreamer(node_id_t num_nodes, edge_id_t num_updates, edge_id_t prime,
               double er_prob, int rounds, long seed1, long seed2);

  HashStreamer(node_id_t num_nodes, edge_id_t prime, double er_prob, int
  rounds, long seed1, long seed2);

  node_id_t nodes() const;
  edge_id_t stream_length() const;

  GraphUpdate next();

  void dump_edges();

};