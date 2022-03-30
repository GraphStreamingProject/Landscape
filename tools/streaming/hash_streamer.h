#pragma once

#include "graph.h"

/**
 * An O(1) memory to generate Erdos-Renyi streams based on hashing.
 *
 * The edge enumeration strategy is as follows:
 * Pick prime p >= n(n-1)/2, k < p. Enumerate all p-1 numbers of the form ki %
 * p, and generate an edge from these numbers using the typical inverse pairing
 * function.
 *
 * Round 1:
 *      Go through edges according to the above enumeration strategy and insert
 *      according to hash1. If the 1st-lowest bit of hash1 is 1, insert.
 *      Otherwise, do nothing.
 * Rounds 2 on:
 *      Go through the enumeration and toggle the status of the edge based on
 *      hash1. If the r-th lowest bit is 1 and the r-1-th lowest bit is 0,
 *      insert. If the r-th lowest is 0 and r-1-th is 1, delete. Otherwise do
 *      nothing.
 * Correction round:
 *      Modifies the stream to produce an Erdos-Renyi graph with the desired
 *      edge probability by deleting or inserting edges based on hash2. If
 *      the hash value < pr * HASH_MAX_VALUE, ensure the edge exists in the
 *      final graph. Otherwise, ensure it does not.
 */
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

  // hash seed values
  unsigned seed1;
  unsigned seed2;

  // for checking number of write/correction inserts/deletes
  int wi = 0;
  int wd = 0;
  int ci = 0;
  int cd = 0;

  // advances generator values to their next valid values in the enumeration loop
  void advance_state_to_next_value();

  // gets the edge update (if there is one) from the current generator values
  GraphUpdate get_edge();

  // corrects the status of the edge (if necessary) specified by the current
  // generator values
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
  HashStreamer(node_id_t num_nodes, edge_id_t num_updates, edge_id_t prime,
               double er_prob, int rounds, long seed1, long seed2);

  /**
   * Instantiate an experimental streamer instance to generate streams until
   * a good stream is found. This cannot be used for experiments since the
   * number of updates is not known ahead of time.
   */
  HashStreamer(node_id_t num_nodes, edge_id_t prime, double er_prob, int
  rounds, long seed1, long seed2);

  node_id_t nodes() const;
  edge_id_t stream_length() const;

  /**
   * @return the next update in the stream.
   */
  GraphUpdate next();

  /**
   * Go through the (rest of) the stream and write all edges to stdout.
   */
  void dump_edges();

};
