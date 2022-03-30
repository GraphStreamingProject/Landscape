#pragma once

#include "graph.h"

/**
 * Fast streamer only for use by Graph Zeppelin or any other
 * deletion-agnostic streaming system.
 *
 * The edge enumeration strategy is as follows:
 * Pick prime p, k < p. Enumerate edges (ik % p, i) for i in (0, p).
 * For j in (1, k) enumerate edges (ik % p, ij % p) similarly.
 *
 * Round 1:
 *      Go through edges according to the above enumeration strategy and insert
 *      according to hash1.
 * Rounds 2 on:
 *      Go through the enumeration and toggle the status of the edge.
 * Correction round:
 *      Modifies the stream to produce an Erdos-Renyi graph with the desired
 *      edge probability by deleting or inserting edges based on hash2.
 */
class GZNonsequentialStreamer {
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

  // hash seeds
  unsigned seed1;
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
   * @param prime           the smallest prime >= num_nodes.
   * @param er_prob         Erdos-Renyi probability for the final graph.
   * @param rounds          number of times to go through the graph's edge
   *                        list when generating updates.
   * @param seed1           seed to use for the hash function for
   *                        insertions/deletions.
   * @param seed2           seed to use for E-R correction.
   */
  GZNonsequentialStreamer(node_id_t num_nodes, edge_id_t num_updates, edge_id_t prime,
                          double er_prob, int rounds, long seed1, long seed2);

  /**
   * Instantiate an experimental streamer instance to generate streams until
   * a good stream is found. This cannot be used for experiments since the
   * number of updates is not known ahead of time.
   */
  GZNonsequentialStreamer(node_id_t num_nodes, edge_id_t prime, double er_prob, int
  rounds, long seed1, long seed2);

  node_id_t nodes() const;
  edge_id_t stream_length() const;

  GraphUpdate next();

  void dump_edges();

};