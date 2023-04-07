#include <gtest/gtest.h>
#include "graph_distrib_update.h"
#include <file_graph_verifier.h>
#include <mat_graph_verifier.h>
#include <graph_gen.h>
#include "work_distributor.h"

TEST(DistributedGraphTest, SmallRandomGraphs) {
  int num_trials = 5;
  while (num_trials--) {
    generate_stream();
    std::ifstream in{"./sample.txt"};
    ASSERT_TRUE(in.is_open());
    node_id_t n;
    edge_id_t m;
    in >> n >> m;
    GraphDistribUpdate g{n, 1};
    int type, a, b;
    while (m--) {
      in >> type >> a >> b;
      if (type == INSERT) {
        g.update({{a, b}, INSERT});
      } else g.update({{a, b}, DELETE});
    }
    g.set_verifier(std::make_unique<FileGraphVerifier>(n, "./cumul_sample.txt"));
    g.spanning_forest_query();
  }
}

TEST(DistributedGraphTest, SmallGraphConnectivity) {
  const std::string file = "./_deps/graphzeppelin-src/test/res/multiples_graph_1024.txt";
  std::ifstream in{file};
  ASSERT_TRUE(in.is_open());
  node_id_t num_nodes;
  in >> num_nodes;
  edge_id_t m;
  in >> m;
  node_id_t a, b;
  GraphDistribUpdate g{num_nodes, 1};
  while (m--) {
    in >> a >> b;
    g.update({{a, b}, INSERT});
  }
  g.set_verifier(std::make_unique<FileGraphVerifier>(num_nodes, file));
  ASSERT_EQ(78, g.spanning_forest_query().size());
}

TEST(DistributedGraphTest, IFconnectedComponentsAlgRunTHENupdateLocked) {
  const std::string file = "./_deps/graphzeppelin-src/test/res/multiples_graph_1024.txt";
  std::ifstream in{file};
  ASSERT_TRUE(in.is_open());
  node_id_t num_nodes;
  in >> num_nodes;
  edge_id_t m;
  in >> m;
  node_id_t a, b;
  GraphDistribUpdate g{num_nodes, 1};
  while (m--) {
    in >> a >> b;
    g.update({{a, b}, INSERT});
  }
  g.set_verifier(std::make_unique<FileGraphVerifier>(num_nodes, file));
  ASSERT_EQ(78, g.spanning_forest_query().size());
  ASSERT_THROW(g.update({{1,2}, INSERT}), UpdateLockedException);
  ASSERT_THROW(g.update({{1,2}, DELETE}), UpdateLockedException);
}
/*
TEST(DistributedGraphTest, TestSupernodeRestoreAfterCCFailure) {
  write_configuration(false, false, 8, 1);
  const std::string file = "./_deps/graphzeppelin-src/test/res/multiples_graph_1024.txt";
  std::cout << file;
  std::ifstream in{file};
  ASSERT_TRUE(in.is_open());
  node_id_t num_nodes;
  in >> num_nodes;
  edge_id_t m;
  in >> m;
  node_id_t a, b;
  GraphDistribUpdate g{num_nodes, 1};
  while (m--) {
    in >> a >> b;
    g.update({{a, b}, INSERT});
  }
  g.set_verifier(std::make_unique<FileGraphVerifier>(file));
  g.should_fail_CC();

  // flush to make sure copy supernodes is consistent with graph supernodes
  WorkDistributor::pause_workers();
  Supernode* copy_supernodes[num_nodes];
  for (node_id_t i = 0; i < num_nodes; ++i) {
    copy_supernodes[i] = Supernode::makeSupernode(*g.supernodes[i]);
  }

  ASSERT_THROW(g.spanning_forest_query(true), OutOfQueriesException);
  for (node_id_t i = 0; i < num_nodes; ++i) {
    for (int j = 0; j < copy_supernodes[i]->get_num_sktch(); ++j) {
      ASSERT_TRUE(*copy_supernodes[i]->get_sketch(j) ==
                *g.supernodes[i]->get_sketch(j));
    }
  }
}
*/
TEST(DistributedGraphTest, TestCorrectnessOnSmallRandomGraphs) {
  int num_trials = 5;
  while (num_trials--) {
    generate_stream();
    std::ifstream in{"./sample.txt"};
    ASSERT_TRUE(in.is_open());
    node_id_t n;
    edge_id_t m;
    in >> n >> m;
    GraphDistribUpdate g{n, 1};
    int type, a, b;
    while (m--) {
      in >> type >> a >> b;
      if (type == INSERT) {
        g.update({{a, b}, INSERT});
      } else g.update({{a, b}, DELETE});
    }

    g.set_verifier(std::make_unique<FileGraphVerifier>(n, "./cumul_sample.txt"));
    g.spanning_forest_query();
  }
}

TEST(DistributedGraphTest, TestCorrectnessOnSmallSparseGraphs) {
  int num_trials = 5;
  while(num_trials--) {
    generate_stream({1024,0.002,0.5,0,"./sample.txt","./cumul_sample.txt"});
    std::ifstream in{"./sample.txt"};
    ASSERT_TRUE(in.is_open());
    node_id_t n;
    edge_id_t m;
    in >> n >> m;
    GraphDistribUpdate g{n, 1};
    int type, a, b;
    while (m--) {
      in >> type >> a >> b;
      if (type == INSERT) {
        g.update({{a, b}, INSERT});
      } else g.update({{a, b}, DELETE});
    }

    g.set_verifier(std::make_unique<FileGraphVerifier>(n, "./cumul_sample.txt"));
    g.spanning_forest_query();
  } 
}
/*

TEST_P(DistributedGraphTest, TestCorrectnessOfReheating) {
  write_configuration(GetParam());
  int num_trials = 5;
  while (num_trials--) {
    generate_stream({1024,0.002,0.5,0,"./sample.txt","./cumul_sample.txt"});
    std::ifstream in{"./sample.txt"};
    ASSERT_TRUE(in.is_open());
    node_id_t n;
    edge_id_t m;
    in >> n >> m;
    Graph *g = new Graph (n);
    int type, a, b;
    printf("number of updates = %lu\n", m);
    while (m--) {
      in >> type >> a >> b;
      if (type == INSERT) g->update({{a, b}, INSERT});
      else g->update({{a, b}, DELETE});
    }
    g->write_binary("./out_temp.txt");
    g->set_verifier(std::make_unique<FileGraphVerifier>("./cumul_sample.txt"));
    std::vector<std::set<node_id_t>> g_res;
    g_res = g->connected_components();
    printf("number of CC = %lu\n", g_res.size());
    delete g; // delete g to avoid having multiple graphs open at once. Which is illegal.

    Graph reheated {"./out_temp.txt"};
    reheated.set_verifier(std::make_unique<FileGraphVerifier>("./cumul_sample.txt"));
    auto reheated_res = reheated.connected_components();
    printf("number of reheated CC = %lu\n", reheated_res.size());
    ASSERT_EQ(g_res.size(), reheated_res.size());
    for (unsigned i = 0; i < g_res.size(); ++i) {
      std::vector<node_id_t> symdif;
      std::set_symmetric_difference(g_res[i].begin(), g_res[i].end(),
          reheated_res[i].begin(), reheated_res[i].end(),
          std::back_inserter(symdif));
      ASSERT_EQ(0, symdif.size());
    }
  }
}
*/

TEST(DistributedGraphTest, TestQueryDuringStream) {
  generate_stream({1024, 0.002, 0.5, 0, "./sample.txt", "./cumul_sample.txt"});
  std::ifstream in{"./sample.txt"};
  ASSERT_TRUE(in.is_open());
  node_id_t n;
  edge_id_t m;
  in >> n >> m;
  GraphDistribUpdate g(n, 1);
  MatGraphVerifier verify(n);

  int type;
  node_id_t a, b;
  edge_id_t tenth = m / 10;
  for(int j = 0; j < 9; j++) {
    for (edge_id_t i = 0; i < tenth; i++) {
      in >> type >> a >> b;
      g.update({{a,b}, (UpdateType)type});
      verify.edge_update(a, b);
    }
    verify.reset_cc_state();
    g.set_verifier(std::make_unique<MatGraphVerifier>(verify));
    g.spanning_forest_query(true);
  }
  m -= 9 * tenth;
  while(m--) {
    in >> type >> a >> b;
    g.update({{a,b}, (UpdateType)type});
    verify.edge_update(a, b);
  }
  verify.reset_cc_state();
  g.set_verifier(std::make_unique<MatGraphVerifier>(verify));
  g.spanning_forest_query();
}

TEST(DistributedGraphTest, TestFewBatches) {
  GraphDistribUpdate g(1024, 1);
  MatGraphVerifier verify(1024);

  // Perform many updates to 3 nodes
  Edge edge1{1, 2};
  Edge edge2{2, 3};

  for(int i = 0; i < 500001; i++) {
    g.update({edge1, INSERT});
  }

  for(int i = 0; i < 500001; i++) {
    g.update({edge2, INSERT});
  }

  verify.edge_update(edge1.src, edge1.dst);
  verify.edge_update(edge2.src, edge2.dst);

  verify.reset_cc_state();
  g.set_verifier(std::make_unique<MatGraphVerifier>(verify));
  ASSERT_EQ(g.spanning_forest_query().size(), 1022);
}
