#include <graph.h>

class GraphDistribUpdate : public Graph {
private:
  FRIEND_TEST(DistributedGraphTest, TestSupernodeRestoreAfterCCFailure);

  static GutteringConfiguration gutter_conf;
public:
  // constructor
  GraphDistribUpdate(node_id_t num_nodes, int num_inserters);
  ~GraphDistribUpdate();

  // some getter functions
  node_id_t get_num_nodes() const {return num_nodes;}
  uint64_t get_seed() const {return seed;}
  Supernode *get_supernode(node_id_t src) const { return supernodes[src]; }

  std::vector<std::set<node_id_t>> spanning_forest_query(bool cont = false);
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

  // our queries are directed to spanning_forest_query or batch_point_query
  // Therefore, we mark the Graph cc query as unusable
  std::vector<std::set<node_id_t>> connected_components(bool cont) = delete;

  bool point_query(node_id_t a, node_id_t b) = delete;
};
