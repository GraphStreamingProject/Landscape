#pragma once

#include "graph.h"

/**
 * Ultra-fast stream generator for use only for Graph Zeppelin only for pure
 * speed testing.
 *
 * Round 1:
 *      Go through all edges in a graph sequentially {(0,1), (0,2), ... (0, n-1)
 *      , ... (n-2, n-1)} and insert if the endpoints have different parity.
 * Rounds 2 on:
 *      Go through all edges in a graph sequentially and toggles the status of
 *      the edge.
 * Correction round:
 *      Modifies the stream to produce an Erdos-Renyi graph with the desired
 *      edge probability by deleting or inserting edges based on a hash
 *      function.
 */
class GZSequentialStreamer {
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
  /**
   * Instantiate a streamer instance when the stream to be generated is known.
   * Useful for experiments where we need to test against a fixed stream.
   * @param num_nodes       number of nodes in the graph.
   * @param num_updates     number of updates in the stream. This MUST be
   *                        determined from an instantiation of the streamer
   *                        with the same parameters. It CANNOT be
   *                        arbitrarily set!
   * @param er_prob         Erdos-Renyi probability for the final graph.
   * @param rounds          number of times to go through the graph's edge
   *                        list when generating updates.
   * @param seed2           seed to use for E-R correction.
   */
  GZSequentialStreamer(node_id_t num_nodes, edge_id_t num_updates,
                       double er_prob, int rounds, long seed2);

  /**
   * Instantiate an experimental streamer instance. This cannot be used for
   * experiments since the number of updates is not known ahead of time.
   */
  GZSequentialStreamer(node_id_t num_nodes, double er_prob, int
  rounds, long seed2);

  node_id_t nodes() const;
  edge_id_t stream_length() const;

  GraphUpdate next();

  void dump_edges();
};