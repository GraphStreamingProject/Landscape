#include "work_distributor.h"
#include "worker_cluster.h"
#include "graph_distrib_update.h"

#include <string>
#include <iostream>
#include <unistd.h>
#include <cstdio>

bool WorkDistributor::shutdown = false;
bool WorkDistributor::paused   = false; // controls whether threads should pause or resume work
int WorkDistributor::num_distributors = 1;
constexpr int WorkDistributor::max_work_distributors;
constexpr size_t WorkDistributor::local_process_cutoff;
node_id_t WorkDistributor::supernode_size;
WorkDistributor **WorkDistributor::workers;
std::condition_variable WorkDistributor::pause_condition;
std::mutex WorkDistributor::pause_lock;
std::thread WorkDistributor::status_thread;

// Queries the work distributors for their current status and writes it to a file
void status_querier() {
  auto start = std::chrono::steady_clock::now();
  double max_ingestion = 0.0;
  double cur_ingestion = 0.0;
  auto last_time = start;
  uint64_t last_insertions = 0;
  constexpr int interval_len = 10;
  int idx = 1;

  while(!WorkDistributor::is_shutdown()) {
    // open temporary file
    std::ofstream tmp_file{"cluster_status_tmp.txt", std::ios::trunc};
    if (!tmp_file.is_open()) {
      std::cerr << "Could not open cluster status temp file!" << std::endl;
      return;
    }

    // get status
    std::vector<std::pair<uint64_t, WorkerStatus>> status_vec = WorkDistributor::get_status();
    auto now = std::chrono::steady_clock::now();
    std::chrono::duration<double> total_time = now - start;

    // calculate total insertions and number of each status type
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
    // calculate interval variables and update accounting
    if (idx % interval_len == 0) {
      std::chrono::duration<double> interval_time = now - last_time;
      uint64_t interval_insertions = total_insertions - last_insertions;
      last_insertions = total_insertions;
      last_time = now;
      cur_ingestion = interval_insertions / interval_time.count() / 2;
      if (cur_ingestion > max_ingestion)
        max_ingestion = cur_ingestion;
      idx = 1;
    }
    else idx++;

    // output status summary
    tmp_file << "===== Cluster Status Summary =====" << std::endl;
    tmp_file << "Number of Workers: " << status_vec.size() 
             << "\t\tUptime: " << (uint64_t) total_time.count()
             << " seconds" << std::endl;
    tmp_file << "Estimated Overall Graph Update Rate: " << total_insertions / total_time.count() / 2 << std::endl;
    tmp_file << "Current Graph Updates Rate: " << cur_ingestion << ", Max: " << max_ingestion << std::endl;
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
  int num_workers = WorkerCluster::start_cluster(_graph->get_num_nodes(), _graph->get_seed(),
                 _gts->gutter_size());
  _gts->set_non_block(false); // make the WorkDistributors wait on queue
  shutdown = false;
  paused   = false;
  supernode_size = Supernode::get_size();

  num_distributors = std::min(num_workers, max_work_distributors);
  int worker_idx = 1;

  workers = (WorkDistributor **) calloc(num_distributors, sizeof(WorkDistributor *));
  for (int i = 0; i < num_distributors; i++) {
    // calculate number of workers this distributor is responsible for
    int work_group = num_workers / (num_distributors - i);
    num_workers -= work_group;
    workers[i] = new WorkDistributor(i+1, worker_idx, worker_idx + work_group - 1, _graph, _gts);
    worker_idx = worker_idx + work_group;
  }
  status_thread = std::thread(status_querier);
}

uint64_t WorkDistributor::stop_workers() {
  if (shutdown)
    return 0;

  shutdown = true;
  status_thread.join();
  workers[0]->gts->set_non_block(true); // make the WorkDistributors bypass waiting in queue
  
  pause_condition.notify_all();      // tell any paused threads to continue and exit
  for (int i = 0; i < num_distributors; i++) {
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
      for (int i = 0; i < num_distributors; i++)
        if (!workers[i]->get_thr_paused()) return false;
      return true;
    });
    

    // double check that we didn't get a spurious wake-up
    bool all_paused = true;
    for (int i = 0; i < num_distributors; i++) {
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

  // wait until all WorkDistributors are unpaused
  while (true) {
    std::unique_lock<std::mutex> lk(pause_lock);
    pause_condition.wait_for(lk, std::chrono::milliseconds(500), []{
      for (int i = 0; i < num_distributors; i++)
        if (workers[i]->get_thr_paused()) return false;
      return true;
    });
    

    // double check that we didn't get a spurious wake-up
    bool all_unpaused = true;
    for (int i = 0; i < num_distributors; i++) {
      if (workers[i]->get_thr_paused()) {
        all_unpaused = false; // a worker still paused so don't return
        break;
      }
    }
    lk.unlock();

    if (all_unpaused) return; // all workers have resumed so exit
  }
}

WorkDistributor::WorkDistributor(int _id, int _minid, int _maxid, GraphDistribUpdate *_graph, 
 GutteringSystem *_gts) : id(_id), min_id(_minid), max_id(_maxid), graph(_graph), gts(_gts), 
 thr(start_worker, this), thr_paused(false), msg_buffers(max_id - min_id + 1),
 backup_msg_buffers(max_id - min_id + 1) {
  for (auto &delta : deltas) {
    delta.second = (Supernode *) malloc(Supernode::get_size());
  }

  // allocate memory for message buffers
  for (auto &msg_buffer : msg_buffers)
    msg_buffer = (char *) malloc(WorkerCluster::max_msg_size);
  for (auto &msg_buffer : backup_msg_buffers)
    msg_buffer = (char *) malloc(WorkerCluster::max_msg_size);

  num_updates = 0;

  std::cout << "WorkDistributor " << id << " with range " << min_id << "-" << max_id << std::endl;
}

WorkDistributor::~WorkDistributor() {
  thr.join();
  for (auto delta : deltas)
    free(delta.second);
  for (auto msg_buffer : msg_buffers)
    free(msg_buffer);
  for (auto msg_buffer : backup_msg_buffers)
    free(msg_buffer);
}

// DO_WORK TODOS:
// 1. The work distributor status doesn't make a lot of sense with this setup
// 2. Number of WorkDistributors as parameter that can be set elsewhere. Depends on # of CPUs available

void WorkDistributor::do_work() {
  WorkQueue::DataNode *data; // pointer to batches to send to worker

  while(true) {
    size_t unprocessed_sends = 0;
    // std::cout << "WorkDistributor " << id << " initial sends" << std::endl;
    // send two messages to each DistributedWorker
    bool no_more_data = false;
    for(int j = 0; !no_more_data && j < 2; j++) {
      for(int i = min_id; !no_more_data && i <= max_id; i++) {
        while (!no_more_data) {
          distributor_status = QUEUE_WAIT;
          // call get_data which will handle waiting on the queue
          // and will enforce locking.
          bool valid = gts->get_data(data);
          if (!valid) {
            no_more_data = true;
            break;
          }
          size_t upds_in_batches = 0;
          for (auto batch : data->get_batches())
            upds_in_batches += batch.upd_vec.size();

          if (upds_in_batches < local_process_cutoff) {
            distributor_status = DISTRIB_PROCESSING;
            // process locally instead of sending over network (TODO: OMP?)
            for (auto batch : data->get_batches())
              graph->batch_update(batch.node_idx, batch.upd_vec, deltas[0].second);
            gts->get_data_callback(data);
	    num_updates += upds_in_batches;
          } 
          else {
            send_batches(i, data);
            unprocessed_sends++;
            break;
          }
        }
      }
    }

    // now that message initialization is done begin main loop
    while(unprocessed_sends > 0) {
      distributor_status = QUEUE_WAIT;
      // call get_data which will handle waiting on the queue
      // and will enforce locking.
      bool valid = gts->get_data(data);
      if (!valid && (shutdown || paused)) {
        break;
      }
      else if (!valid) continue;

      size_t upds_in_batches = 0;
      for (auto batch : data->get_batches())
        upds_in_batches += batch.upd_vec.size();

      if (upds_in_batches < local_process_cutoff) {
        distributor_status = DISTRIB_PROCESSING;
        // process locally instead of sending over network (TODO: OMP?)
        for (auto batch : data->get_batches())
          graph->batch_update(batch.node_idx, batch.upd_vec, deltas[0].second);
        gts->get_data_callback(data);
	num_updates += upds_in_batches;
      }
      else {
        // std::cout << "WorkDistributor " << id << " got valid data" << std::endl;
        // get a delta back from some worker
        int wid = await_deltas();
        // send batches to the worker we recieved a message from
        send_batches(wid, data);
      }
    }
    // std::cout << "Work Distributor " << id << " shutdown or paused" << std::endl;

    if (shutdown) {
      // wait for and process all unprocessed sends
      for (size_t i = 0; i < unprocessed_sends; i++) {
        await_deltas();
      }
      unprocessed_sends = 0;
      // std::cout << "num updates = " << num_updates << std::endl;
      return;
    }
    else if (paused) {
      // wait for and process all unprocessed sends
      for (size_t i = 0; i < unprocessed_sends; i++) {
        await_deltas();
      }
      unprocessed_sends = 0;

      // pause the current thread and then wait to be unpaused
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
      pause_condition.notify_all(); // notify unpause_workers()
    }
  }
}

void WorkDistributor::send_batches(int wid, WorkQueue::DataNode *data) {
  // std::cout << "WorkDistributor " << id << " sending batches to DistributedWorker " << wid << std::endl;
  distributor_status = PARSE_AND_SEND;
  WorkerCluster::send_batches(wid, id, data->get_batches(), msg_buffers[wid - min_id]);
  // now that this buffer is being used for a send, swap with the backup
  std::swap(msg_buffers[wid - min_id], backup_msg_buffers[wid - min_id]);

  // add DataNodes back to work queue and increment num_updates
  for (auto &batch : data->get_batches())
    num_updates += batch.upd_vec.size();
  gts->get_data_callback(data);
}

int WorkDistributor::await_deltas() {
  // std::cout << "WorkDistributor " << id << " awaiting deltas" << std::endl;
  distributor_status = DISTRIB_PROCESSING;
  size_t size = WorkerCluster::num_batches;

  // Wait for deltas to arrive from some worker, returns the worker id and modifies size variable
  int wid = WorkerCluster::recv_deltas(id, deltas, size, msg_buffers, min_id);

  // apply the recieved deltas to the graph supernodes
  distributor_status = APPLY_DELTA;
  for (node_id_t i = 0; i < size; i++) {
    node_id_t node_idx = deltas[i].first;
    Supernode *to_apply = deltas[i].second;
    Supernode *graph_sketch = graph->get_supernode(node_idx);
    graph_sketch->apply_delta_update(to_apply);
  }

  // std::cout << "Work Distributor " << id << " got deltas from DistributedWorker " << wid << std::endl;

  // return the id of the worker that we recieved deltas from
  return wid;
}
