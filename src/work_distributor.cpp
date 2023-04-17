#include "work_distributor.h"
#include "worker_cluster.h"
#include "graph_distrib_update.h"

#include <string>
#include <iostream>
#include <unistd.h>
#include <cstdio>

#include <omp.h>

bool WorkDistributor::shutdown = false;
bool WorkDistributor::paused   = false; // controls whether threads should pause or resume work
constexpr size_t WorkDistributor::local_process_cutoff;
int WorkDistributor::work_distrib_threads;
node_id_t WorkDistributor::supernode_size;
WorkDistributor **WorkDistributor::workers;
std::condition_variable WorkDistributor::pause_condition;
std::mutex WorkDistributor::pause_lock;
std::thread WorkDistributor::status_thread;
std::atomic<size_t> WorkDistributor::proc_locally;

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
    uint64_t q_total = 0, d_total = 0, a_total = 0, paused = 0;
    for (auto status : status_vec) {
      total_insertions += status.first;
      switch (status.second) {
        case QUEUE_WAIT:         ++q_total; break;
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
  size_t buffer_size = std::max((size_t)_gts->gutter_size(), Supernode::get_serialized_size());
  WorkerCluster::start_cluster(_graph->get_num_nodes(), _graph->get_seed(), buffer_size);
  _gts->set_non_block(false); // make the WorkDistributors wait on queue
  shutdown = false;
  paused   = false;
  supernode_size = Supernode::get_size();
  work_distrib_threads = std::min(WorkerCluster::num_msg_forwarders, WorkerCluster::num_workers);

  workers = new WorkDistributor*[work_distrib_threads];
  for (int i = 0; i < work_distrib_threads; i++) {
    // calculate number of workers this distributor is responsible for
    workers[i] = new WorkDistributor(i + 1, _graph, _gts);
  }
  status_thread = std::thread(status_querier);
  proc_locally = 0;
}

uint64_t WorkDistributor::stop_workers() {
  if (shutdown)
    return 0;

  shutdown = true;
  status_thread.join();
  workers[0]->gts->set_non_block(true); // make the WorkDistributors bypass waiting in queue
  
  pause_condition.notify_all();      // tell any paused threads to continue and exit
  for (int i = 0; i < work_distrib_threads; i++) {
    delete workers[i];
  }
  delete[] workers;
  if (WorkerCluster::is_active()) // catch edge case where stop after teardown_cluster()
    return WorkerCluster::stop_cluster() + proc_locally;
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
      for (int i = 0; i < work_distrib_threads; i++)
        if (!workers[i]->get_thr_paused()) return false;
      return true;
    });
    

    // double check that we didn't get a spurious wake-up
    bool all_paused = true;
    for (int i = 0; i < work_distrib_threads; i++) {
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
      for (int i = 0; i < work_distrib_threads; i++)
        if (workers[i]->get_thr_paused()) return false;
      return true;
    });
    

    // double check that we didn't get a spurious wake-up
    bool all_unpaused = true;
    for (int i = 0; i < work_distrib_threads; i++) {
      if (workers[i]->get_thr_paused()) {
        all_unpaused = false; // a worker still paused so don't return
        break;
      }
    }
    lk.unlock();

    if (all_unpaused) return; // all workers have resumed so exit
  }
}

WorkDistributor::WorkDistributor(int _id, GraphDistribUpdate *_graph, GutteringSystem *_gts)
    : id(_id), graph(_graph), gts(_gts), num_updates(0), thr_paused(false), 
      send_buf(new char[WorkerCluster::max_msg_size]), 
      recv_buf(new char[WorkerCluster::max_msg_size]),
      thr(start_send_worker, this), delta_thr(start_recv_worker, this) {
  network_supernode = (Supernode *) malloc(Supernode::get_size());
  for (size_t i = 0; i < num_helper_threads; i++)
    local_supernodes[i] = (Supernode *) malloc(Supernode::get_size());

  // std::cout << "Done initializing WorkDistributor: " << id << std::endl;
}

WorkDistributor::~WorkDistributor() {
  thr.join();
  delta_thr.join();
  free(network_supernode);
  for (auto supernode : local_supernodes)
    free(supernode);
  delete[] send_buf;
  delete[] recv_buf;
}

void WorkDistributor::do_send_work() {
  WorkQueue::DataNode *data; // pointer to batches to send to worker

  while(true) {
    while(true) {
      distributor_status = QUEUE_WAIT;
      // call get_data which will handle waiting on the queue
      // and will enforce locking.
      bool valid = gts->get_data(data);
      if (!valid && (shutdown || paused)) {
        break;
      }
      else if (!valid) continue;

      size_t upds_in_batches = 0;
      size_t num_batches = 0;
      for (auto batch : data->get_batches()) {
        num_batches += (batch.upd_vec.size() > 0);
        upds_in_batches += batch.upd_vec.size();
      }


      if (upds_in_batches < local_process_cutoff * num_batches) {
        distributor_status = DISTRIB_PROCESSING;
        // process locally instead of sending over network
#pragma omp parallel for num_threads(num_helper_threads)
        for (size_t i = 0; i < data->get_batches().size(); i++) {
          auto& batch = data->get_batches()[i];
          if (batch.upd_vec.size() > 0)
            graph->batch_update(batch.node_idx, batch.upd_vec,
                                local_supernodes[omp_get_thread_num()]);
        }
        gts->get_data_callback(data);
        proc_locally += upds_in_batches;
      }
      else {
        // std::cout << "WorkDistributor " << id << " got valid data" << std::endl;
        // send batches to our associated worker
        send_batches(data);
      }
      num_updates += upds_in_batches;
    }

    if (shutdown) {
      // Tell the DistributedWorkers to flush their message queues and then shutdown
      // std::cout << "WorkDistributor: " << id << " send thread performing shutdown" << std::endl;
      MPI_Send(nullptr, 0, MPI_CHAR, id, FLUSH, MPI_COMM_WORLD);
      return;
    }
    else if (paused) {
      // std::cout << "WorkDistributor: " << id << " send thread performing pause" << std::endl;
      
      // Tell the DistributedWorkers to flush their message queues and then pause
      MPI_Send(nullptr, 0, MPI_CHAR, id, FLUSH, MPI_COMM_WORLD);

      // wait until we are unpaused
      std::unique_lock<std::mutex> lk(pause_lock);
      // std::cout << "WorkDistributor: " << id << " send pausing" << std::endl;
      pause_condition.wait(lk, []{return !paused || shutdown;});
      thr_paused = false; // no longer paused
      // std::cout << "WorkDistributor: " << id << " send un-pausing" << std::endl;
      lk.unlock();
      pause_condition.notify_all(); // notify unpause_workers()
    }
  }
}

void WorkDistributor::send_batches(WorkQueue::DataNode *data) {
  // std::cout << "WorkDistributor " << id << " sending batches to DistributedWorker" << std::endl;
  distributor_status = DISTRIB_PROCESSING;
  WorkerCluster::send_batches(id, data->get_batches(), send_buf);

  // add DataNodes back to work queue
  gts->get_data_callback(data);
}

void WorkDistributor::do_recv_work() {
  int recv_from = WorkerCluster::batch_fwd_to_delta_fwd(id);
  while(true) {
    int msg_size = WorkerCluster::max_msg_size;
    // std::cout << "WorkDistributor: " << id << " recieving message from: " << recv_from << std::endl; 
    MessageCode code = WorkerCluster::recv_message_from(recv_from, recv_buf, msg_size);
    if (code == DELTA) {
      distributor_status = APPLY_DELTA;
      WorkerCluster::parse_and_apply_deltas(recv_buf, msg_size, network_supernode, graph);
    } else if (code == FLUSH) {
      if (shutdown) {
        // std::cout << "WorkDistributor: " << id << " recv shutting down!" << std::endl;
        return;
      }
      if (!paused && !shutdown) {
        throw BadMessageException("We shouldn't recieve FLUSH when not paused or shutdown!");
      }
      // pause the current thread and then wait to be unpaused
      std::unique_lock<std::mutex> lk(pause_lock);
      // std::cout << "WorkDistributor: " << id << " recv pausing!" << std::endl;
      thr_paused = true; // this thread is currently paused
      distributor_status = PAUSED;
      lk.unlock();
      pause_condition.notify_all(); // notify pause_workers()

      // wait until we are unpaused
      lk.lock();
      pause_condition.wait(lk, []{return !paused || shutdown;});
      // std::cout << "WorkDistributor: " << id << " recv un-pausing!" << std::endl;
      lk.unlock();
      pause_condition.notify_all(); // notify unpause_workers()
    } else
      throw BadMessageException("do_recv_work() Did not recognize message code!");
  }
}
