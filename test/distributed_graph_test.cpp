#include <gtest/gtest.h>
#include "graph_distrib_update.h"
#include <file_graph_verifier.h>
#include <write_configuration.h>
#include <graph_gen.h>

TEST(DistributedGraphTest, SmallRandomGraphs) {
  write_configuration(false, false, 512, 1);
  int num_trials = 5;
  while (num_trials--) {
    generate_stream();
    std::ifstream in{"./sample.txt"};
    node_id_t n;
    edge_id_t m;
    in >> n >> m;
    GraphDistribUpdate g{n};
    int type, a, b;
    while (m--) {
      in >> type >> a >> b;
      if (type == INSERT) {
        g.update({{a, b}, INSERT});
      } else g.update({{a, b}, DELETE});
    }

    g.set_verifier(std::make_unique<FileGraphVerifier>("./cumul_sample.txt"));
    g.spanning_forest_query();
  }
}
