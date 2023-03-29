#include "distributed_worker.h"
#include "worker_cluster.h"
#include "graph_distrib_update.h"

#include <mpi.h>
#include <iostream>
#include <thread>

DistributedWorker::DistributedWorker(int _id) : id(_id) {
  init_worker();
  running = true;

  // Create recieve message queue (send message queue starts empty)
  helper_threads = std::thread::hardware_concurrency();
  for (size_t i = 0; i < 2 * helper_threads; i++) {
    BatchesToDeltasHandler msg_handler(max_msg_size, WorkerCluster::num_batches);
    MsgBufferQueue<BatchesToDeltasHandler>::QueueElm* q_elm =
        new MsgBufferQueue<BatchesToDeltasHandler>::QueueElm(msg_handler);
    recv_msg_queue.emplace_back(q_elm);
  }

  // std::cout << "Successfully started distributed worker " << id << "!" << std::endl;
  run();
}
DistributedWorker::~DistributedWorker() {
  if (recv_msg_queue.size() != 2 * helper_threads) {
    std::cerr << "WARNING: recv queue not full when deleting DeltaNode -- memory leak" << std::endl;
  }
  for (auto handler : recv_msg_queue) {
    delete handler;
  }
}

void DistributedWorker::run() {
#pragma omp parallel num_threads(helper_threads + 1)
#pragma omp single
  {
    while(running) {
      msg_size = max_msg_size; // reset msg_size

      // pop a new msg handle from the queue
      MsgBufferQueue<BatchesToDeltasHandler>::QueueElm* q_elm = recv_msg_queue.front();
      recv_msg_queue.pop_front();

      // Extract stuff from the data_handler
      // std::cout << "Waiting for message. . ." << std::endl;
      char* recv_buffer = q_elm->data.batches_buffer;
      MessageCode code = WorkerCluster::worker_recv_message(recv_buffer, &msg_size);

      if (code == BATCH) {
#pragma omp task firstprivate(q_elm, msg_size) default(none)
        {
          char* recv_buffer = q_elm->data.batches_buffer;
          std::vector<delta_t>& deltas = q_elm->data.deltas;
          int& num_deltas = q_elm->data.num_deltas;

          // deserialize data -- get id and vector of batches
          std::vector<batch_t> batches;
          WorkerCluster::parse_batches(recv_buffer, msg_size, batches);
          num_deltas = batches.size();

          // create deltas 
          for (size_t i = 0; i < batches.size(); i++) {
            batch_t& batch = batches[i];
            delta_t& delta = deltas[i];

            num_updates += batch.second.size();
            delta.node_idx = batch.first;
            Graph::generate_delta_node(num_nodes, seed, delta.node_idx, batch.second,
                                       delta.supernode);
            WorkerCluster::serialize_delta(delta.node_idx, *delta.supernode, delta.serial_delta);
          }
          // this message is ready for sending back to main so push to send_msg_queue
          send_msg_queue.push(q_elm);
        }
        // back on main thread. If recv_msg_queue is empty then send a message back to main
        if (recv_msg_queue.empty()) process_send_queue_elm();
      }
      else if (code == BUFF_QUERY) {
        size_t delta_buffer_size = helper_threads * 2;
        MPI_Send(&delta_buffer_size, sizeof(delta_buffer_size), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        recv_msg_queue.push_back(q_elm);
      }
      else if (code == STOP) {
#pragma omp taskwait
        // process send queue stuff until its empty
        while (!send_msg_queue.empty()) process_send_queue_elm();
        free(delta_node);
        free(msg_buffer);
        WorkerCluster::send_upds_processed(num_updates); // tell main how many updates we processed

        // std::cout << "Number of updates processed = " << num_updates << std::endl;

        num_updates = 0;
        recv_msg_queue.push_back(q_elm);
        init_worker(); // wait for init
      }
      else if (code == SHUTDOWN) {
        // process send queue stuff until its empty
#pragma omp taskwait
        while (!send_msg_queue.empty()) process_send_queue_elm();
        running = false;
        // std::cout << "DistributedWorker " << id << " shutting down" << std::endl;
        // if (num_updates > 0) 
        //   std::cout << "# of updates processed since last init " << num_updates << std::endl;
        recv_msg_queue.push_back(q_elm);
      }
      else throw BadMessageException("DistributedWorker run() did not recognize message code");
    }
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

void DistributedWorker::process_send_queue_elm() {
  MsgBufferQueue<BatchesToDeltasHandler>::QueueElm* q_elm = send_msg_queue.pop();
  auto& delta_data = q_elm->data;

  for (int i = 0; i < delta_data.num_deltas; i++) {
    auto& stream = delta_data.deltas[i].serial_delta;
    
    WorkerCluster::return_deltas(delta_data.get_delta_str(i), stream.tellp());
    stream.reset();  // reset omemstream back to the beginning
  }
  q_elm->data.num_deltas = 0;
  recv_msg_queue.push_back(q_elm);  // we've dealt with this queue elm so place it in recv
}