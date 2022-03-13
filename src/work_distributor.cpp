#include "work_distributor.h"
#include "worker_cluster.h"
#include "graph_distrib_update.h"
#include <string>
#include <iostream>

bool WorkDistributor::shutdown = false;
bool WorkDistributor::paused   = false; // controls whether threads should pause or resume work
int WorkDistributor::num_workers = 1;
node_id_t WorkDistributor::supernode_size;
WorkDistributor **WorkDistributor::workers;
std::condition_variable WorkDistributor::pause_condition;
std::mutex WorkDistributor::pause_lock;

/***********************************************
 ****** WorkDistributor Static Functions *******
 ***********************************************/
/* These functions are used by the rest of the
 * code to manipulate the WorkDistributors as a whole
 */
void WorkDistributor::start_workers(GraphDistribUpdate *_graph, GutteringSystem *_gts) {
  num_workers = WorkerCluster::start_cluster(_graph->get_num_nodes(), _graph->get_seed(),
                 _gts->gutter_size());
  shutdown = false;
  paused   = false;
  supernode_size = Supernode::get_size();

  workers = (WorkDistributor **) calloc(num_workers, sizeof(WorkDistributor *));
  for (int i = 0; i < num_workers; i++) {
    workers[i] = new WorkDistributor(i+1, _graph, _gts);
  }
  workers[0]->gts->set_non_block(false); // make the WorkDistributors wait on queue
}

uint64_t WorkDistributor::stop_workers() {
  if (shutdown)
    return 0;

  shutdown = true;
  workers[0]->gts->set_non_block(true); // make the WorkDistributors bypass waiting in queue
  
  pause_condition.notify_all();      // tell any paused threads to continue and exit
  for (int i = 0; i < num_workers; i++) {
    delete workers[i];
  }
  free(workers);
  if (WorkerCluster::is_active()) // catch edge case where stop after teardown_cluster()
    return WorkerCluster::stop_cluster();
  else
    return 0;
}

void WorkDistributor::pause_workers() {
  paused = true;
  workers[0]->gts->set_non_block(true); // make the WorkDistributors bypass waiting in queue

  // wait until all WorkDistributors are paused
  while (true) {
    std::unique_lock<std::mutex> lk(pause_lock);
    pause_condition.wait_for(lk, std::chrono::milliseconds(500), []{
      for (int i = 0; i < num_workers; i++)
        if (!workers[i]->get_thr_paused()) return false;
      return true;
    });
    

    // double check that we didn't get a spurious wake-up
    bool all_paused = true;
    for (int i = 0; i < num_workers; i++) {
      if (!workers[i]->get_thr_paused()) {
        all_paused = false; // a worker still working so don't stop
        break;
      }
    }
    lk.unlock();

    if (all_paused) return; // all workers are done so exit
  }
}

void WorkDistributor::unpause_workers() {
  workers[0]->gts->set_non_block(false); // buffer-tree operations should block when necessary
  paused = false;
  pause_condition.notify_all();       // tell all paused workers to get back to work
}

WorkDistributor::WorkDistributor(int _id, GraphDistribUpdate *_graph, GutteringSystem *_gts) :
 id(_id), graph(_graph), gts(_gts), thr(start_worker, this), thr_paused(false) {}

WorkDistributor::~WorkDistributor() {
  thr.join();
}

void WorkDistributor::do_work() {
  std::vector<data_ret_t> data_buffer; // buffer of batches to send to worker
  data_ret_t data;
  while(true) { 
    if(shutdown)
      return;
    std::unique_lock<std::mutex> lk(pause_lock);
    thr_paused = true; // this thread is currently paused
    lk.unlock();
    pause_condition.notify_all(); // notify pause_workers()

    // wait until we are unpaused
    lk.lock();
    pause_condition.wait(lk, []{return !paused || shutdown;});
    thr_paused = false; // no longer paused
    lk.unlock();
    while(true) {
      // call get_data which will handle waiting on the queue
      // and will enforce locking.
      bool valid = gts->get_data(data);

      if (valid) {
        data_buffer.push_back(data);
        if (data_buffer.size() >= num_batches) {
          flush_data_buffer(data_buffer);
          data_buffer.clear();
        }
      }
      else if(shutdown)
        return;
      else if(paused) {
        if (data_buffer.size() > 0) {
          flush_data_buffer(data_buffer);
          data_buffer.clear();
        }
        break;
      }
    }
  }
}

void WorkDistributor::flush_data_buffer(const std::vector<data_ret_t>& data_buffer){
  node_sketch_pairs deltas = WorkerCluster::send_batches_recv_deltas(id, data_buffer);
  for (auto &delta : deltas) {
    node_id_t node_idx = delta.first;
    Supernode *to_apply = delta.second;
    Supernode *graph_sketch = graph->get_supernode(node_idx);
    graph_sketch->apply_delta_update(to_apply);
    free(to_apply); // TODO: reuse this memory rather than malloc and free
  }
}
