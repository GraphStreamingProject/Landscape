#include <types.h>

// forward declaration
class Supernode;

class DistributedWorker {
private:
  uint64_t seed;
  node_id_t num_nodes;
  int max_msg_size = 0;

  const int init_msg_size = sizeof(seed) + sizeof(num_nodes) + sizeof(max_msg_size);
  bool running = true; // is cluster active

  // variables for storing messages to this worker
  char *msg_buffer;
  int msg_size;

  Supernode *delta_node; // the supernode object used to generate deltas
  int id; // id of the distributed worker

  uint64_t num_updates = 0; // number of updates processed by this node
  size_t num_batches = 0;

  // wait for initialize message
  void init_worker();
public:
  // Create a distributed worker and run
  DistributedWorker(int _id);

  // main loop of the distributed worker
  void run();
};
