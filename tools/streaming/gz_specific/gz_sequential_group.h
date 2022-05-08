#pragma once

#include <vector>
#include "gz_sequential_streamer.h"

class StreamerGroup {
private:
  std::vector<GZSequentialStreamer> handle;
  node_id_t num_nodes;
  edge_id_t num_edges;

  void initGroup(int num_handles, edge_id_t tot_advances);
public:
  StreamerGroup(int num_handles, node_id_t num_nodes, double er_prob, int
          rounds, long seed2);

  GZSequentialStreamer& get_handle(int i);

  /**
   * equivalent to\n
   * <code>for (auto edge : get_handle(i)) f(edge, args)</code>
   * @param i
   * @param f
   * @param args
   * @return the number of edges processed
   */
  edge_id_t forall_in_handle(int i, void (& f)(GraphUpdate&,
                                               Graph&, int), Graph& g, int thr_num);
};
