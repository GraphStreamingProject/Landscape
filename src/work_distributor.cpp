#include "work_distributor.h"
#include "worker_cluster.h"
#include "graph_distrib_update.h"

#include <string>
#include <iostream>
#include <unistd.h>
#include <cstdio>

bool WorkDistributor::shutdown = false;
bool WorkDistributor::paused   = false; // controls whether threads should pause or resume work
int WorkDistributor::num_workers = 1;
node_id_t WorkDistributor::supernode_size;
WorkDistributor **WorkDistributor::workers;
std::condition_variable WorkDistributor::pause_condition;
std::mutex WorkDistributor::pause_lock;
std::thread WorkDistributor::status_thread;

// Queries the work distributors for their current status and writes it to a file
void status_querier() {
  auto start = std::chrono::steady_clock::now();
  while(!WorkDistributor::is_shutdown()) {
    // open temporary file
    std::ofstream tmp_file{"cluster_status_tmp.txt", std::ios::trunc};
    if (!tmp_file.is_open()) {
      std::cerr << "Could not open cluster status temp file!" << std::endl;
      return;
    }

    // get status then calculate total insertions and number of each status type
    std::vector<std::pair<uint64_t, WorkerStatus>> status_vec = WorkDistributor::get_status();
    uint64_t total_insertions = 0;
    uint64_t q_total = 0, p_total = 0, d_total = 0, a_total = 0, paused = 0;
    for (auto status : status_vec) {
      total_insertions += status.first;
      switch (status.second) {
        case QUEUE_WAIT:         ++q_total; break;
        case PARSE_AND_SEND:     ++p_total; break;
        case DISTRIB_PROCESSING: ++d_total; break;
        case APPLY_DELTA:        ++a_total; break;
        case PAUSED:             ++paused; break;
      }
    }
    // output status summary
    std::chrono::duration<double> diff = std::chrono::steady_clock::now() - start;
    tmp_file << "===== Cluster Status Summary =====" << std::endl;
    tmp_file << "Number of Workers: " << status_vec.size() 
             << "\t\tUptime: " << (uint64_t) diff.count()
             << " seconds" << std::endl;
    tmp_file << "Estimated Graph Update Rate: " << total_insertions / diff.count() / 2 << std::endl;
    tmp_file << "QUEUE_WAIT         " << q_total << std::endl;
    tmp_file << "PARSE_AND_SEND     " << p_total << std::endl;
    tmp_file << "DISTRIB_PROCESSING " << d_total << std::endl;
    tmp_file << "APPLY_DELTA        " << a_total << std::endl;
    tmp_file << "PAUSED             " << paused  << std::endl;

    // rename temporary file to actual status file then sleep
    tmp_file.flush();
    if(std::rename("cluster_status_tmp.txt", "cluster_status.txt")) {
      std::perror("Error renaming cluster status file");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}

/***********************************************
 ****** WorkDistributor Static Functions *******
 ***********************************************
 * These functions are used by the rest of the
 * code to interact with the WorkDistributors
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
  status_thread = std::thread(status_querier);
}

uint64_t WorkDistributor::stop_workers() {
  if (shutdown)
    return 0;

  shutdown = true;
  status_thread.join();
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
 id(_id), graph(_graph), gts(_gts), thr(start_worker, this), thr_paused(false) {
  for (auto &delta : deltas) {
    delta.second = (Supernode *) malloc(Supernode::get_size());
  }
  msg_buffer = (char *) malloc(WorkerCluster::max_msg_size);
  waiting_msg_buffer = (char *) malloc(WorkerCluster::max_msg_size);
  num_updates = 0;
}

WorkDistributor::~WorkDistributor() {
  thr.join();
  for (auto delta : deltas)
    free(delta.second);
  free(msg_buffer);
  free(waiting_msg_buffer);
}

void WorkDistributor::do_work() {
  std::vector<WorkQueue::DataNode *> data_buffer; // buffer of batches to send to worker
  while(true) { 
    if(shutdown)
      return;
    std::unique_lock<std::mutex> lk(pause_lock);
    thr_paused = true; // this thread is currently paused
    distributor_status = PAUSED;

    lk.unlock();
    pause_condition.notify_all(); // notify pause_workers()

    // wait until we are unpaused
    lk.lock();
    pause_condition.wait(lk, []{return !paused || shutdown;});
    thr_paused = false; // no longer paused
    lk.unlock();
    while(true) {
      distributor_status = QUEUE_WAIT;
      // call get_data_batched which will handle waiting on the queue
      // and will enforce locking. 
      bool valid = gts->get_data_batched(data_buffer, WorkerCluster::num_batches);

      if (valid) {
        cur_size = data_buffer.size();
        flush_data_buffer(data_buffer);
        std::swap(msg_buffer, waiting_msg_buffer);
        if (has_waiting)
          await_deltas(wait_size);
        else
          has_waiting = true;
        std::swap(cur_size, wait_size); // now we wait for batch of cur_size
      }
      else if(shutdown) {
        if (has_waiting)
          await_deltas(wait_size);
        has_waiting = false;
        return;
      }
      else if(paused) {
        if (has_waiting)
          await_deltas(wait_size);
        has_waiting = false;
        break;
      }
    }
  }
}

void WorkDistributor::flush_data_buffer(const std::vector<WorkQueue::DataNode *>& data_buffer) {
  distributor_status = PARSE_AND_SEND;
  WorkerCluster::send_batches(id, data_buffer, msg_buffer);
  distributor_status = DISTRIB_PROCESSING;

  // add DataNodes back to work queue 
  for (auto data_node : data_buffer)
    num_updates += data_node->get_data_vec().size();
  gts->get_data_batched_callback(data_buffer);
}

void WorkDistributor::await_deltas(const size_t size) {
  // Wait for deltas to arrive
  WorkerCluster::recv_deltas(id, deltas, size, msg_buffer);

  // apply the recieved deltas to the graph supernodes
  distributor_status = APPLY_DELTA;
  for (node_id_t i = 0; i < size; i++) {
    node_id_t node_idx = deltas[i].first;
    Supernode *to_apply = deltas[i].second;
    Supernode *graph_sketch = graph->get_supernode(node_idx);
    graph_sketch->apply_delta_update(to_apply);
  }
}
