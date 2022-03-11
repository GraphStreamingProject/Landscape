#pragma once
#include <mutex>
#include <mpi.h>
#include <sstream>
#include <condition_variable>
#include <thread>
#include <guttering_system.h>

// forward declarations
class GraphDistribUpdate;
class Supernode;

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
  void flush_data_buffer(const std::vector<data_ret_t>& data_buffer);

  void do_work(); // function which runs the WorkDistributor process
  int id;
  GraphDistribUpdate *graph;
  GutteringSystem *gts;
  std::thread thr;
  bool thr_paused; // indicates if this individual thread is paused

  // thread status and status management
  static bool shutdown;
  static bool paused;
  static std::condition_variable pause_condition;
  static std::mutex pause_lock;

  // configuration
  static int num_workers;
  static node_id_t supernode_size;

  // list of all WorkDistributors
  static WorkDistributor **workers;

  // the supernode object this WorkDistributor will use for generating deltas
  Supernode *delta_node;
};
