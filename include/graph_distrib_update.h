#pragma once
#include <graph.h>
#include <supernode.h>

class GraphDistribUpdate : public Graph {
private:
  FRIEND_TEST(DistributedGraphTest, TestSupernodeRestoreAfterCCFailure);

  static GraphConfiguration graph_conf(node_id_t num_nodes, node_id_t k);
  node_id_t k = 1; // this parameter determines the value of k for is_k_connected()
public:
  // constructor
  GraphDistribUpdate(node_id_t num_nodes, int num_inserters, node_id_t k = 1);
  ~GraphDistribUpdate();

  // some getter functions
  node_id_t get_num_nodes() const {return num_nodes;}
  uint64_t get_seed() const {return seed;}
  Supernode *get_supernode(node_id_t src) const { return supernodes[src]; }

  std::vector<std::set<node_id_t>> get_connected_components(bool cont = false);
  std::vector<std::set<node_id_t>> k_spanning_forests(node_id_t user_k);
  bool point_to_point_query(node_id_t a, node_id_t b);

  /*
   * This function must be called at the beginning of the program
   * its job is to direct the workers to the DistributedWorker class
   */
  static void setup_cluster(int argc, char** argv);
  /*
   * This function must be called at the end of the program
   * its job is to finalize all the MPI processes
   */
  static void teardown_cluster();

  // our queries are directed to get_connected_components, k_spanning_forests or
  // point_to_point_query. Therefore, we mark the Graph cc query as unusable
  std::vector<std::set<node_id_t>> connected_components(bool cont) = delete;

  bool point_query(node_id_t a, node_id_t b) = delete;

  node_id_t get_k() {
    return k;
  }
};
