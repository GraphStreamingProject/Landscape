#pragma once
#include <types.h>
#include <vector>

#include "msg_buffer_queue.h"
#include <supernode.h>
#include "memstream.h"

class DistributedWorker {
private:
  struct delta_t {
    node_id_t node_idx;
    Supernode* supernode;
    omemstream serial_delta;
  };
  // class for coordinating recieving batches and sending deltas
  class BatchesToDeltasHandler {
   private:
    char* serial_delta_mem;
   public:
    char* batches_buffer;         // where we place the batches message
    std::vector<delta_t> deltas;  // where we place the generated deltas
    int num_deltas = 0;

    BatchesToDeltasHandler(int max_msg_size, size_t size) {
      batches_buffer = new char[max_msg_size * sizeof(char)];
      serial_delta_mem = new char[size * Supernode::get_serialized_size()];
      //  std::cout << "BatchesToDeltas with size = " << deltas.size() << std::endl;
      for (size_t i = 0; i < size; i++)
        deltas.push_back({0, (Supernode*)new char[Supernode::get_size()],
                        omemstream(serial_delta_mem + Supernode::get_serialized_size() * i, size)});
    }

    BatchesToDeltasHandler(BatchesToDeltasHandler&& oth)
        : batches_buffer(std::exchange(oth.batches_buffer, nullptr)),
          deltas(std::move(oth.deltas)),
          num_deltas(oth.num_deltas){};

    ~BatchesToDeltasHandler() {
      delete[] batches_buffer;
      delete[] serial_delta_mem;
      for (auto& delta : deltas)
        delete[] delta.supernode;
    }

    char* get_delta_str(size_t idx) {
      return serial_delta_mem + Supernode::get_serialized_size() * idx; 
    }

    BatchesToDeltasHandler(const BatchesToDeltasHandler&) = delete;
    BatchesToDeltasHandler& operator=(const BatchesToDeltasHandler&) = delete;
  };

  uint64_t seed;
  node_id_t num_nodes;
  int max_msg_size = 0;

  // queues for coordinating with helper threads
  std::list<MsgBufferQueue<BatchesToDeltasHandler>::QueueElm*> recv_msg_queue;  // no locking
  MsgBufferQueue<BatchesToDeltasHandler> send_msg_queue;

  const int init_msg_size = sizeof(seed) + sizeof(num_nodes) + sizeof(max_msg_size);
  bool running = true; // is cluster active

  // variables for storing messages to this worker
  char *msg_buffer;
  int msg_size;

  Supernode *delta_node; // the supernode object used to generate deltas
  int id; // id of the distributed worker
  size_t helper_threads;  // number of helper threads that will process deltas for the main thread

  uint64_t num_updates = 0; // number of updates processed by this node

  // wait for initialize message
  void init_worker();
  void process_send_queue_elm();
public:
  // Create a distributed worker and run
  DistributedWorker(int _id);
  ~DistributedWorker();

  // main loop of the distributed worker
  void run();
};
