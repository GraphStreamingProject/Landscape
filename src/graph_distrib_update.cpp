#include "../include/graph_distrib_update.h"
#include "work_distributor.h"
#include "distributed_worker.h"
#include "worker_cluster.h"
#include <graph_worker.h>
#include <mpi.h>

#include <iostream>

// Static functions for starting and shutting down the cluster
void GraphDistribUpdate::setup_cluster(int argc, char** argv) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  // TODO: What is this checking??
  if (provided != MPI_THREAD_MULTIPLE){
    std::cout << "ERROR!" << std::endl;
    exit(1);
  }

  int proc_id;
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);
  if (proc_id > 0) {
    // we are a worker, start working!
    DistributedWorker worker(proc_id);
    MPI_Finalize();
    exit(EXIT_SUCCESS);
  }

  int num_workers;
  MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
}

void GraphDistribUpdate::teardown_cluster() {
  WorkerCluster::shutdown_cluster();
  MPI_Finalize();
}

/***************************************
 * GraphDistribUpdate class
 ***************************************/

// Construct a GraphDistribUpdate by first constructing a Graph
GraphDistribUpdate::GraphDistribUpdate(node_id_t num_nodes) : Graph(num_nodes) {

  // TODO: figure out a better solution than this.
  GraphWorker::stop_workers(); // shutdown the graph workers because we aren't using them
  WorkDistributor::start_workers(this, gts); // start threads and distributed cluster
}

GraphDistribUpdate::~GraphDistribUpdate() {
  // inform the worker threads they should wait for new init or shutdown
  uint64_t updates = WorkDistributor::stop_workers(); 
  std::cout << "Total updates processed by cluster since last init = " << updates << std::endl;
}

inline void GraphDistribUpdate::sample_supernodes(std::pair<Edge,
                                                 SampleSketchRet> *query,
                                                 std::vector<node_id_t> &reps) {
  if (_boruvka_round >= _rounds_to_distribute) {
    ::Graph::sample_supernodes(query, reps);
    return;
  }
  ++_boruvka_round;

  bool except = false;
  std::exception_ptr err;

  node_id_t samples_per_worker = reps.size() / WorkerCluster::get_num_workers();
  if (samples_per_worker * WorkerCluster::get_num_workers() < reps.size())
    ++samples_per_worker;
  // TODO: change out stopgap for actual fix
  uint64_t sketch_size = supernodes[0]->get_sketch_size() - sizeof
        (Sketch) + 1; // count only the data buffers
  node_id_t num_safe_sketches = (WorkerCluster::get_max_msg_size() - sizeof
        (sketch_size))/ (sketch_size + sizeof(uint64_t));
  node_id_t batches_per_msg = std::min(samples_per_worker, num_safe_sketches);

  std::cout << "Mini-batch size: " << batches_per_msg << "\n";
  std::cout << "Samples per worker: " << samples_per_worker << "\n";
  
#pragma omp parallel for num_threads(WorkerCluster::get_num_workers()) default(none) shared(reps, samples_per_worker, batches_per_msg, query, except, err)
  for (int wid = 0; wid < WorkerCluster::get_num_workers(); ++wid) {
    // wrap in a try/catch because exiting through exception is undefined behavior in OMP
    try {
      node_id_t samples_left = (wid == WorkerCluster::get_num_workers() - 1) ?
                               reps.size() -
                               (WorkerCluster::get_num_workers() - 1) *
                               samples_per_worker :
                               samples_per_worker;

      // send in mini-batches
      node_id_t start_idx = samples_per_worker * wid;
      node_id_t end_idx = start_idx + samples_left;
      for (node_id_t idx = start_idx; idx < end_idx;) {
        node_id_t num_to_send = std::min(end_idx - idx, batches_per_msg);
        std::vector<Supernode *> supernode_ptrs(num_to_send);
        for (node_id_t j = 0; j < num_to_send; ++j) {
          supernode_ptrs[j] = supernodes[reps[idx + j]];
        }
        auto recvd_samples = WorkerCluster::send_sketches_recv_queries(wid + 1,
                                                                       supernode_ptrs);
        for (node_id_t j = 0; j < num_to_send; ++j) {
          query[reps[idx + j]] = recvd_samples[j];
        }
        // manually increment all pointers of sampled supernodes by 1
        for (auto supernode: supernode_ptrs) {
          supernode->incr_idx();
        }

        idx += num_to_send;
      }
    } catch (...) {
      except = true;
      err = std::current_exception();
    }
  }
  // Did one of our threads produce an exception?
  if (except) std::rethrow_exception(err);
}


std::vector<std::set<node_id_t>> GraphDistribUpdate::spanning_forest_query(bool cont) {
  flush_start = std::chrono::steady_clock::now();
  gts->force_flush(); // flush everything in buffering system to make final updates
  WorkDistributor::pause_workers(); // wait for the workers to finish applying the updates
  flush_end = std::chrono::steady_clock::now();
  // after this point all updates have been processed from the guttering system
  _boruvka_round = 0;

  if (!cont)
    return boruvka_emulation(false); // merge in place
  
  // if backing up in memory then perform copying in boruvka
  bool except = false;
  std::exception_ptr err;
  std::vector<std::set<node_id_t>> ret;
  try {
    ret = boruvka_emulation(true);
  } catch (...) {
    except = true;
    err = std::current_exception();
  }

  // get ready for ingesting more from the stream
  // reset dsu and resume graph workers
  for (node_id_t i = 0; i < num_nodes; i++) {
    supernodes[i]->reset_query_state();
    parent[i] = i;
  }
  update_locked = false;
  WorkDistributor::unpause_workers();

  // check if boruvka errored
  if (except) std::rethrow_exception(err);

  return ret;
}
