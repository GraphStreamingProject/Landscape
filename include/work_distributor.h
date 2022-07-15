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
  PARSE_AND_SEND,
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
  static uint64_t stop_workers();    // shutdown and delete WorkDistributors
  static void pause_workers();   // pause the WorkDistributors before CC
  static void unpause_workers(); // unpause the WorkDistributors to resume updates

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
    for (int i = 0; i < num_distributors; i++)
      ret.push_back({workers[i]->num_updates.load(), workers[i]->distributor_status.load()});
    return ret;
  }

  static bool is_shutdown() { return shutdown; }

  // maximum number of Work Distributors
  static constexpr int max_work_distributors = 256;

private:
  /**
   * Create a WorkDistributor object by setting metadata and spinning up a thread.
   * @param _id     the id of the new WorkDistributor and the id of its associated distributed worker
   * @param _graph  the graph which this WorkDistributor will be updating.
   * @param _bf     the database data will be extracted from.
   */
  WorkDistributor(int _id, int _minid, int _maxid, GraphDistribUpdate *_graph, GutteringSystem *_gts);
  ~WorkDistributor();

  /**
   * This function is used by a new thread to capture the WorkDistributor object
   * and begin running do_work.
   * @param obj the memory where we will store the WorkDistributor obj.
   */
  static void *start_worker(void *obj) {
    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // CPU_SET(((WorkDistributor *)obj)->id % (NUM_CPUS - 1), &cpuset);
    // pthread_setaffinity_np(((WorkDistributor *) obj)->thr.native_handle(),
    //      sizeof(cpu_set_t), &cpuset);
    ((WorkDistributor *)obj)->do_work();
    return nullptr;
  }

  // send data_buffer to distributed worker for processing
  void send_batches(int wid, WorkQueue::DataNode *data);
  // await data_buffer from distributed worker
  int await_deltas();
  bool has_waiting = false;

  void do_work(); // function which runs the WorkDistributor process
  int id;
  int min_id;
  int max_id;
  GraphDistribUpdate *graph;
  GutteringSystem *gts;
  std::thread thr;
  bool thr_paused; // indicates if this individual thread is paused

  // memory buffers involved in cluster communication for reuse between messages
  node_sketch_pairs_t deltas{WorkerCluster::num_batches};
  std::vector<char *> msg_buffers;
  std::vector<char *> backup_msg_buffers; // used for 2 messages at once trick
  size_t cur_size;
  size_t wait_size;

  std::atomic<uint64_t> num_updates;
  std::atomic<WorkerStatus> distributor_status;

  // thread status and status management
  static bool shutdown;
  static bool paused;
  static std::condition_variable pause_condition;
  static std::mutex pause_lock;

  // configuration
  static int num_distributors;
  static node_id_t supernode_size;
  static constexpr size_t local_process_cutoff = 6000;

  // list of all WorkDistributors
  static WorkDistributor **workers;
  static std::thread status_thread;
};
