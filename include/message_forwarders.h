#pragma once
#include <mpi.h>

#include "worker_cluster.h"

/*
 * Performing communication over the network benefits from
 * having many processes participating simultaneously.
 * This process recieves instructions from another process
 * on the same machine to either send a message to, or receive
 * a message from other process on a different machine.
 * In the case of a receive it then sends the results back to
 * the caller.
 */
class BatchMessageForwarder {
 private:
  char* msg_buffer = nullptr;
  int msg_size;
  int max_msg_size;
  int id;
  bool running = true;

  char** batch_msg_buffers;
  MPI_Request* batch_requests;
  int num_batch_sent = 0;
  int num_distrib = 0;
  int distrib_offset;

  void run();      // run the process
  void init();     // initialize the process
  void cleanup();  // deallocate memory before another call to INIT

  void send_batch();
  void send_flush();

 public:
  BatchMessageForwarder(int _id) : id(_id) {
    init();
    run();
  }
  static constexpr size_t init_msg_size = sizeof(max_msg_size) + sizeof(WorkerCluster::num_workers);
};

class DeltaMessageForwarder {
 private:
  char* msg_buffer = nullptr;
  int msg_size;
  int max_msg_size;
  int id;
  bool running = true;
  int num_distrib = 0;
  int num_distrib_flushed = 0;

  void run();      // run the process
  void init();     // initialize the process
  void cleanup();  // deallocate memory before another call to INIT

  void send_delta();
  void process_distrib_worker_done();

 public:
  DeltaMessageForwarder(int _id) : id(_id) {
    init();
    run();
  }
  static constexpr size_t init_msg_size = sizeof(max_msg_size) + sizeof(WorkerCluster::num_workers);
};
