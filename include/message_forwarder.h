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
class MessageForwarder {
 private:
  char* msg_buffer = nullptr;
  char* batch_buffer = nullptr;
  char* delta_buffer = nullptr;
  int msg_size;
  int max_msg_size;
  int id;
  bool running = true;
  bool first_batch = true;
  bool first_delta = true;

  MPI_Request batch_send_req;
  MPI_Request delta_send_req;

  static constexpr size_t recv_metadata = 2 * sizeof(int);
  static constexpr size_t init_msg_size = sizeof(max_msg_size) + sizeof(WorkerCluster::num_workers);

  void run();      // run the process
  void init();     // initialize the process
  void cleanup();  // deallocate memory before another call to INIT

  void send_batch();
  void send_flush();
  void send_delta();

  struct RecvInstruct {
    int recv_from;
    MessageCode tag;
  };

 public:
  MessageForwarder(int _id) : id(_id) {
    init();
    run();
  }
};
