#pragma once
#include <mutex>
#include <sstream>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <mpi.h>

#include <guttering_system.h>
#include <worker_cluster.h>

// forward declarations
class GraphDistribUpdate;
class Supernode;

enum WorkerStatus {
  QUEUE_WAIT,
  DISTRIB_PROCESSING,
  APPLY_DELTA,
  PAUSED
};

class WorkDistributor {
public:
  /**
   * start_workers creates the WorkDistributors and sets them to query the
   * given buffering system.
   * @param _graph           the graph to update.
   * @param _bf              the buffering system to query.
   * @param _supernode_size  the size of a supernode so that we can allocate
   *                         space for a delta_node.
   */
  static void start_workers(GraphDistribUpdate *_graph, GutteringSystem *_gts);
  static uint64_t stop_workers(); // shutdown and delete WorkDistributors
  static void pause_workers();    // pause the WorkDistributors before CC
  static void unpause_workers();  // unpause the WorkDistributors to resume updates

  /**
   * Returns whether the current thread is paused.
   */
  bool get_thr_paused() {return thr_paused;}

  /*
   * Returns the status of each work distributor thread
   */
  static std::vector<std::pair<uint64_t, WorkerStatus>> get_status() { 
    std::vector<std::pair<uint64_t, WorkerStatus>> ret;
    if (shutdown) return ret; // return empty vector if shutdown
    for (int i = 0; i < work_distrib_threads; i++)
      ret.push_back({workers[i]->num_updates.load(), workers[i]->distributor_status.load()});
    return ret;
  }

  static bool is_shutdown() { return shutdown; }
  static constexpr size_t local_process_cutoff = 400;
  static constexpr size_t num_helper_threads=4;
private:
  /**
   * Create a WorkDistributor object by setting metadata and spinning up a thread.
   * @param _id     the id of the new WorkDistributor and the id of its associated distributed worker
   * @param _graph  the graph which this WorkDistributor will be updating.
   * @param _bf     the database data will be extracted from.
   */
  WorkDistributor(int _id, GraphDistribUpdate *_graph, GutteringSystem *_gts);
  ~WorkDistributor();

  /**
   * This function is used by a new thread to capture the WorkDistributor object
   * and begin running do_send_work.
   * @param obj the memory where we will store the WorkDistributor obj.
   */
  static void *start_send_worker(void *obj) {
    ((WorkDistributor *)obj)->do_send_work();
    return nullptr;
  }

  static void *start_recv_worker(void* obj) {
    ((WorkDistributor *)obj)->do_recv_work();
    return nullptr;
  }

  // send data_buffer to distributed worker for processing
  void send_batches(WorkQueue::DataNode *data);

  void do_send_work(); // function which runs to send batches
  void do_recv_work(); // function which runs to recieve deltas
  int id;
  GraphDistribUpdate *graph;
  GutteringSystem *gts;

  std::atomic<uint64_t> num_updates;
  bool thr_paused;       // indicates if this WorkDistributor is paused
  char* send_buf;
  char* recv_buf;
  std::thread thr;       // Work Distributor thread that sends batches and does other things
  std::thread delta_thr; // helper thread that recieves deltas
  size_t outstanding_deltas = 0;
  Supernode *local_supernodes[num_helper_threads]; // For processing updates locally

  // memory buffers involved in cluster communication for reuse between messages
  Supernode *network_supernode;
  std::atomic<WorkerStatus> distributor_status;

  // thread status and status management
  static bool shutdown;
  static bool paused;
  static std::condition_variable pause_condition;
  static std::mutex pause_lock;
  static int work_distrib_threads;
  static std::atomic<uint64_t> proc_locally;

  // configuration
  static node_id_t supernode_size;

  // list of all WorkDistributors
  static WorkDistributor **workers;
  static std::thread status_thread;
};
