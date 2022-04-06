#include "distributed_worker.h"
#include "worker_cluster.h"
#include "graph_distrib_update.h"

#include <mpi.h>
#include <iostream>
#include <iomanip>

DistributedWorker::DistributedWorker(int _id) : id(_id) {
  init_worker();
  running = true;

  std::cout << "Successfully started distributed worker " << id << "!" << std::endl;
  run();
}

void DistributedWorker::run() {
  while(running) {
    msg_size = max_msg_size; // reset msg_size
    MessageCode code = WorkerCluster::worker_recv_message(msg_buffer, &msg_size);

    switch (code) {
      case BATCH: {
        // std::cout << "DistributedWorker " << id << " got batch to process" << std::endl;
        std::stringstream serial_str;
        std::vector<batch_t> batches;
        WorkerCluster::parse_batches(msg_buffer, msg_size, batches); // deserialize data

        for (auto &batch : batches) {
          num_updates += batch.second.size();
          uint64_t node_idx = batch.first;

          delta_node = Supernode::makeSupernode(num_nodes, seed, delta_node);

          Graph::generate_delta_node(num_nodes, seed, node_idx, batch.second, delta_node);
          WorkerCluster::serialize_delta(node_idx, *delta_node, serial_str);
        }
        serial_str.flush();
        const std::string delta_msg = serial_str.str();
        WorkerCluster::return_deltas(delta_msg);
        // std::cout << "DistributedWorker " << id << " returning deltas" << std::endl;

        break;
      }
      case QUERY: {
        auto recv_end = std::chrono::system_clock::now();
        // de-serialize
        node_id_t num_sketches = msg_size / Sketch::sketchSizeof();
        if ((int)(num_sketches * Sketch::sketchSizeof()) != msg_size) {
          num_queries = *((int*) (msg_buffer + num_sketches *
                Sketch::sketchSizeof()));
        }

        auto deserial_end = std::chrono::system_clock::now();

        // query
        std::vector<std::pair<Edge, SampleSketchRet>> samples(num_sketches);
        for (unsigned i = 0; i < num_sketches; ++i) {
          auto temp = ((Sketch*)(msg_buffer + i*Sketch::sketchSizeof()))
                ->fixed()->query();
          samples[i] = {inv_nondir_non_self_edge_pairing_fn(temp.first), temp.second};
        }

        auto query_end = std::chrono::system_clock::now();

        // serialize and send
        std::stringstream serial_str;
        WorkerCluster::serialize_samples(samples, serial_str);
        const std::string sample_msg = serial_str.str();
        serialized_ret += sample_msg;

        auto serial_end = std::chrono::system_clock::now();

        if (serialized_ret.size() == num_queries * (sizeof(vec_t) + sizeof(SampleSketchRet))) {
          WorkerCluster::return_samples(serialized_ret);
          serialized_ret = "";
          num_queries = INT_MAX;

          // print timestamps
          long disp = 1649215000;
          if (id == 2) {
            std::cout << std::setprecision(20);
            std::cout << id << "\t" << 3 << "\t" <<
                      std::chrono::duration<long double>(
                            recv_end.time_since_epoch())
                            .count() - disp << "\n";
            std::cout << id << "\t" << 4 << "\t" <<
                      std::chrono::duration<long double>(
                            deserial_end.time_since_epoch())
                            .count() - disp << "\n";
            std::cout << id << "\t" << 5 << "\t" <<
                      std::chrono::duration<long double>(
                            query_end.time_since_epoch())
                            .count() - disp << "\n";
            std::cout << id << "\t" << 6 << "\t" <<
                      std::chrono::duration<long double>(
                            serial_end.time_since_epoch())
                            .count() - disp << "\n";
          }
        }

        break;
      }
      case STOP: {
        std::cout << "DistributedWorker " << id << " stopping and waiting for init" << std::endl;
        std::cout << "# of updates processed since last init " << num_updates << std::endl;
        free(delta_node);
        free(msg_buffer);
        WorkerCluster::send_upds_processed(num_updates); // tell main how many updates we processed

        num_updates = 0;
        init_worker(); // wait for init

        break;
      }
      case SHUTDOWN: {
        running = false;
        std::cout << "DistributedWorker " << id << " shutting down" << std::endl;
        if (num_updates > 0)
          std::cout << "# of updates processed since last init " << num_updates << std::endl;

        break;
      }
      default: {
        throw BadMessageException("DistributedWorker run() did not recognize message code");
      }
    }
  }
}

void DistributedWorker::init_worker() {
  char init_buffer[init_msg_size];
  msg_size = init_msg_size;
  MessageCode code = WorkerCluster::worker_recv_message(init_buffer, &msg_size);
  if (code == SHUTDOWN) { // if we get a shutdown message than exit
    std::cout << "DistributedWorker " << id << " shutting down " << std::endl;
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
