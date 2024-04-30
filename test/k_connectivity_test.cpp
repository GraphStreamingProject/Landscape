#include <gtest/gtest.h>
#include "graph_distrib_update.h"
#include <file_graph_verifier.h>
#include <mat_graph_verifier.h>

TEST(KConnectivityTest, SimpleTest) {
  const std::string file = "./_deps/graphzeppelin-src/test/res/multiples_graph_1024.txt";
  std::ifstream in{file};
  ASSERT_TRUE(in.is_open());
  node_id_t num_nodes;
  in >> num_nodes;
  edge_id_t m;
  in >> m;
  node_id_t a, b;

  // Create a graph with num_nodes vertices, 1 inserter, and k = 4
  GraphDistribUpdate g{num_nodes, 1, 4};
  while (m--) {
    in >> a >> b;
    g.update({{a, b}, INSERT});
  }
  g.set_verifier(std::make_unique<FileGraphVerifier>(num_nodes, file));
  std::vector<std::set<node_id_t>> adj = g.k_spanning_forests(4);

  size_t edges = 0;
  for (node_id_t src = 0; src < num_nodes; src++) {
    edges += adj[src].size();
  }
  std::cout << "number of spanning forest edges: " << edges << std::endl;
}
