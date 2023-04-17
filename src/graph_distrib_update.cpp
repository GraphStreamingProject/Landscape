#include "../include/graph_distrib_update.h"
#include "work_distributor.h"
#include "distributed_worker.h"
#include "message_forwarders.h"
#include "worker_cluster.h"
#include <graph_worker.h>
#include <mpi.h>

#include <iostream>

GraphConfiguration GraphDistribUpdate::graph_conf(){
  auto retval = GraphConfiguration()
          .gutter_sys(CACHETREE)
          .disk_dir(".")
          .backup_in_mem(true)
          .num_groups(1024)
          .group_size(1);
  retval.gutter_conf()
          .page_factor(1)
          .buffer_exp(20)
          .fanout(64)
          .queue_factor(WorkerCluster::num_batches)
          .num_flushers(2)
          .gutter_factor(1.2)
          .wq_batch_per_elm(WorkerCluster::num_batches);
  return retval;
}

// Static functions for starting and shutting down the cluster
void GraphDistribUpdate::setup_cluster(int argc, char** argv) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  // TODO: What is this checking??
  if (provided != MPI_THREAD_MULTIPLE){
    std::cerr << "ERROR!: MPI does not support MPI_THREAD_MULTIPLE?" << std::endl;
    exit(EXIT_FAILURE);
  }

  int num_machines;
  MPI_Comm_size(MPI_COMM_WORLD, &num_machines);
  if (num_machines < WorkerCluster::distrib_worker_offset + 1) {
    std::cerr << "ERROR: Too few processes! Need at least "
              << WorkerCluster::distrib_worker_offset + 1 << std::endl;
    exit(EXIT_FAILURE);
  }

  int proc_id;
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);
  if (proc_id >= WorkerCluster::distrib_worker_offset) {
    // we are a worker, start working!
    DistributedWorker worker(proc_id);
    MPI_Finalize();
    exit(EXIT_SUCCESS);
  } else if (proc_id > WorkerCluster::num_msg_forwarders) {
    DeltaMessageForwarder forwarder(proc_id);
    MPI_Finalize();
    exit(EXIT_SUCCESS);
  } else if (proc_id > 0) {
    BatchMessageForwarder forwarder(proc_id);
    MPI_Finalize();
    exit(EXIT_SUCCESS);
  }

  if (proc_id != 0) {
    std::cout << "ERROR: Incorrect main processes ID: " << proc_id << std::endl;
    exit(EXIT_FAILURE);
  }
  // only main process continues past here
}

void GraphDistribUpdate::teardown_cluster() {
  WorkerCluster::shutdown_cluster();
  MPI_Finalize();
}

/***************************************
 * GraphDistribUpdate class
 ***************************************/

// Construct a GraphDistribUpdate by first constructing a Graph
GraphDistribUpdate::GraphDistribUpdate(node_id_t num_nodes, int num_inserters) : 
 Graph(num_nodes, graph_conf(), num_inserters) {
  // TODO: figure out a better solution than this.
  GraphWorker::stop_workers(); // shutdown the graph workers because we aren't using them
  WorkDistributor::start_workers(this, gts); // start threads and distributed cluster
#ifdef USE_EAGER_DSU
  std::cout << "USING EAGER_DSU" << std::endl;
#endif
  std::cout << "Beginning stream ingestion!" << std::endl;
}

GraphDistribUpdate::~GraphDistribUpdate() {
  // inform the worker threads they should wait for new init or shutdown
  uint64_t updates = WorkDistributor::stop_workers();
  std::cout << "Total updates processed by cluster since last init = " << updates << std::endl;
}

std::vector<std::set<node_id_t>> GraphDistribUpdate::spanning_forest_query(bool cont) {
  // DSU check before calling force_flush()
  if (dsu_valid && cont) {
    cc_alg_start = flush_start = flush_end = std::chrono::steady_clock::now();
    std::cout << "~ Used existing DSU" << std::endl;
#ifdef VERIFY_SAMPLES_F
    for (node_id_t src = 0; src < num_nodes; ++src) {
      for (const auto& dst : spanning_forest[src]) {
        verifier->verify_edge({src, dst});
      }
    }
#endif
    auto retval = cc_from_dsu();
    cc_alg_end = std::chrono::steady_clock::now();
    return retval;
  }

  flush_start = std::chrono::steady_clock::now();
  gts->force_flush(); // flush everything in buffering system to make final updates
  WorkDistributor::pause_workers(); // wait for the workers to finish applying the updates
  flush_end = std::chrono::steady_clock::now();
  // after this point all updates have been processed from the guttering system

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
  }
  update_locked = false;
  WorkDistributor::unpause_workers();

  // check if boruvka errored
  if (except) std::rethrow_exception(err);

  return ret;
}

bool GraphDistribUpdate::point_to_point_query(node_id_t a, node_id_t b) {
  // DSU check before calling force_flush()
  if (dsu_valid) {
    cc_alg_start = flush_start = flush_end = std::chrono::steady_clock::now();
    std::cout << "~ Used existing DSU" << std::endl;
#ifdef VERIFY_SAMPLES_F
    for (node_id_t src = 0; src < num_nodes; ++src) {
      for (const auto& dst : spanning_forest[src]) {
        verifier->verify_edge({src, dst});
      }
    }
#endif
    bool retval = (get_parent(a) == get_parent(b));
    cc_alg_end = std::chrono::steady_clock::now();
    return retval;
  }

  flush_start = std::chrono::steady_clock::now();
  gts->force_flush(); // flush everything in buffering system to make final updates
  WorkDistributor::pause_workers(); // wait for the workers to finish applying the updates
  flush_end = std::chrono::steady_clock::now();
  // after this point all updates have been processed from the guttering system

  // if backing up in memory then perform copying in boruvka
  bool except = false;
  std::exception_ptr err;
  bool ret;
  try {
    boruvka_emulation(true);
    ret = (get_parent(a) == get_parent(b));
  } catch (...) {
    except = true;
    err = std::current_exception();
  }

  // get ready for ingesting more from the stream
  // reset dsu and resume graph workers
  for (node_id_t i = 0; i < num_nodes; i++) {
    supernodes[i]->reset_query_state();
  }
  update_locked = false;
  WorkDistributor::unpause_workers();

  // check if boruvka errored
  if (except) std::rethrow_exception(err);

  return ret;
}
