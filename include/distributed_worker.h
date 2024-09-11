#pragma once
#include <types.h>
#include <vector>
#include <atomic>

#include "msg_buffer_queue.h"
#include <supernode.h>
#include "memstream.h"

class DistributedWorker {
private:
  struct delta_t {
    node_id_t node_idx;
    Supernode* supernode;
  };
  // class for coordinating recieving batches and sending deltas
  class BatchesToDeltasHandler {
   public:
    char* serial_delta_mem;       // where we serialize the deltas
    char* batches_buffer;         // where we place the batches message
    std::vector<delta_t> deltas;  // where we place the generated deltas
    omemstream serial_stream;
    int msg_src;

    BatchesToDeltasHandler(int max_msg_size, size_t size) 
      : serial_delta_mem(new char[max_msg_size * sizeof(char)]),
        batches_buffer(new char[max_msg_size * sizeof(char)]),
        serial_stream(serial_delta_mem, max_msg_size) {
      //  std::cout << "BatchesToDeltas with size = " << deltas.size() << std::endl;
      for (size_t i = 0; i < size; i++)
        deltas.push_back({0, (Supernode*)new char[Supernode::get_size()]});
    }

    BatchesToDeltasHandler(BatchesToDeltasHandler&& oth)
        : serial_delta_mem(std::exchange(oth.serial_delta_mem, nullptr)),
          batches_buffer(std::exchange(oth.batches_buffer, nullptr)), deltas(std::move(oth.deltas)), 
          serial_stream(std::move(oth.serial_stream)) {};

    ~BatchesToDeltasHandler() {
      delete[] batches_buffer;
      delete[] serial_delta_mem;
      for (auto& delta : deltas)
        delete[] delta.supernode;
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

  static constexpr int init_msg_size =
      sizeof(seed) + sizeof(num_nodes) + sizeof(max_msg_size) + sizeof(double);
  bool running = true; // is cluster active

  // variables for storing messages to this worker
  char *msg_buffer;
  int msg_size;

  Supernode *delta_node; // the supernode object used to generate deltas
  int id; // id of the distributed worker
  size_t helper_threads;  // number of helper threads that will process deltas for the main thread

  std::atomic<size_t> num_updates; // number of updates processed by this node

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
