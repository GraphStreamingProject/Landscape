#include "distributed_worker.h"
#include "worker_cluster.h"
#include "graph_distrib_update.h"

#include <mpi.h>
#include <iostream>

DistributedWorker::DistributedWorker(int _id) : id(_id) {
  init_worker();
  running = true;

  // std::cout << "Successfully started distributed worker " << id << "!" << std::endl;
  run();
}

void DistributedWorker::run() {
  while(running) {
    msg_size = max_msg_size; // reset msg_size
    MessageCode code = WorkerCluster::worker_recv_message(msg_buffer, &msg_size);
    if (code == BATCH) {
      // std::cout << "DistributedWorker " << id << " got batch to process" << std::endl;
      std::stringstream serial_str;
      std::vector<batch_t> batches;

      // deserialize data -- get id and vector of batches
      int distributor_id = WorkerCluster::parse_batches(msg_buffer, msg_size, batches);
      for (auto &batch : batches) {
        num_updates += batch.second.size();
        uint64_t node_idx = batch.first;

        delta_node = Supernode::makeSupernode(num_nodes, seed, delta_node);
        
        Graph::generate_delta_node(num_nodes, seed, node_idx, batch.second, delta_node);
        WorkerCluster::serialize_delta(node_idx, *delta_node, serial_str);
      }
      serial_str.flush();
      const std::string delta_msg = serial_str.str();
      // std::cout << "DistributedWorker " << id << " returning deltas, using tag " << distributor_id << std::endl;
      WorkerCluster::return_deltas(distributor_id, delta_msg);
    }
    else if (code == STOP) {
      // std::cout << "DistributedWorker " << id << " stopping and waiting for init" << std::endl;
      // std::cout << "# of updates processed since last init " << num_updates << std::endl;
      free(delta_node);
      free(msg_buffer);
      WorkerCluster::send_upds_processed(num_updates); // tell main how many updates we processed

      // std::cout << "Number of updates processed = " << num_updates << std::endl;

      num_updates = 0;
      init_worker(); // wait for init
    }
    else if (code == SHUTDOWN) {
      running = false;
      // std::cout << "DistributedWorker " << id << " shutting down" << std::endl;
      // if (num_updates > 0) 
      //   std::cout << "# of updates processed since last init " << num_updates << std::endl;
      return;
    }
    else throw BadMessageException("DistributedWorker run() did not recognize message code");
  }
}

void DistributedWorker::init_worker() {
  char init_buffer[init_msg_size];
  msg_size = init_msg_size;
  MessageCode code = WorkerCluster::worker_recv_message(init_buffer, &msg_size);
  if (code == SHUTDOWN) { // if we get a shutdown message than exit
    // std::cout << "DistributedWorker " << id << " shutting down " << std::endl;
    running = false;
    return;
  }

  if (code != INIT)
    throw BadMessageException("Expected INIT");

  if (msg_size != init_msg_size)
    throw BadMessageException("INIT message of wrong length");

  memcpy(&num_nodes, init_buffer, sizeof(num_nodes));
  memcpy(&seed, init_buffer + sizeof(num_nodes), sizeof(seed));
  memcpy(&max_msg_size, init_buffer + sizeof(num_nodes) + sizeof(seed), sizeof(max_msg_size));

  // std::cout << "Recieved initialize: # = " << num_nodes << ", s = " << seed << " max = " << max_msg_size << std::endl;

  Supernode::configure(num_nodes);
  delta_node = (Supernode *) malloc(Supernode::get_size());
  msg_buffer = (char *) malloc(max_msg_size);
}
