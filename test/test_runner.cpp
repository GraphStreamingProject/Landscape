#include <gtest/gtest.h>
#include "graph_distrib_update.h"

int main(int argc, char** argv) {
  // setup cluster, must be called first
  GraphDistribUpdate::setup_cluster(argc, argv);

  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();

  // teardown cluster, must be called last
  GraphDistribUpdate::teardown_cluster();
  return ret;
}
