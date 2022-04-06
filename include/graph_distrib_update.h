#include <graph.h>

class GraphDistribUpdate : public Graph {
private:
  /**
   * Overrides main graph sample code with distributed functionality.
   * @param query  an array of supernode query results
   * @param reps   an array containing node indices for the representative of each supernode
   */
  void sample_supernodes(std::pair<Edge, SampleSketchRet> *query,
                         std::vector<node_id_t> &reps) override;

  int _boruvka_round = 0;
  int _rounds_to_distribute = INT_MAX;

public:
  // constructor
  GraphDistribUpdate(node_id_t num_nodes);
  ~GraphDistribUpdate();

  // some getter functions
  node_id_t get_num_nodes() const {return num_nodes;}
  uint64_t get_seed() const {return seed;}
  Supernode *get_supernode(node_id_t src) const { return supernodes[src]; }

  std::vector<std::set<node_id_t>> spanning_forest_query(bool cont = false);

  // set the number of rounds of boruvka queries to distribute
  void num_rounds_to_distribute(int rounds) { _rounds_to_distribute = rounds; }

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
};
