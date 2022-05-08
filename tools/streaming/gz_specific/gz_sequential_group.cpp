#include "gz_sequential_group.h"

// inverts the row-major "pairing function" used in gz_sequential_streamer
std::pair<node_id_t, node_id_t> inv_sequential_pairing_fn(edge_id_t q,
                                                          node_id_t n) {
  auto rt = (2*n - 1)*(2*n - 1) - 8*q;
  auto knum = 2*n - 1 - sqrt(rt);
  auto k = (node_id_t)(knum / 2);
  node_id_t j = q - (2*n - k - 1)*k / 2;
  if (j == 0) {
    return {k - 1, n - 1};
  }
  return {k,j + k};
}

StreamerGroup::StreamerGroup(int num_handles, node_id_t num_nodes, double
er_prob, int rounds, long seed2) : handle(num_handles,
                                                  GZSequentialStreamer
                                                  (num_nodes, 0,
                                                   er_prob, rounds, seed2)), num_nodes(num_nodes) {
  num_edges = num_nodes * (num_nodes - 1) / 2;
  edge_id_t tot_adv = num_edges * rounds;

  initGroup(num_handles, tot_adv);
}

GZSequentialStreamer &StreamerGroup::get_handle(int i) {
  return handle[i];
}

void StreamerGroup::initGroup(int num_handles, edge_id_t tot_advances) {
  edge_id_t adv_per_handle = tot_advances / num_handles;
  edge_id_t leftover = tot_advances % num_handles;

  edge_id_t curr_offset = 0;
  for (int i = 0; i < num_handles; ++i) {
    int curr_round = (int)(curr_offset / num_edges);
    auto idx = curr_offset % num_edges + 1;
    auto res = inv_sequential_pairing_fn(idx, num_nodes);

    // write values
    auto& streamer = handle[i];
    streamer._curr_i = res.first;
    streamer._curr_j = res.second - 1;
    streamer._curr_round = curr_round;
    streamer.max_num_advances = adv_per_handle;

    curr_offset += adv_per_handle;
  }

  // fix last handle
  handle[num_handles - 1].max_num_advances += leftover;
}

edge_id_t StreamerGroup::forall_in_handle(int i, void (& f)(GraphUpdate&,
      Graph&, int), Graph& g, int thr_num) {
  auto& stream = handle[i];
  edge_id_t retval = 0;
  auto edge = stream.next();
  do {
    f(edge, g, i);
    ++retval;
    edge = stream.next();
  } while (edge != GZSequentialStreamer::NO_UPDATE);
  return retval;
}
