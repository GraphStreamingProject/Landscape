#include "worker_cluster.h"
#include "work_distributor.h"
#include "memstream.h"
#include "message_forwarders.h"
#include "graph_distrib_update.h"

#include <iostream>
#include <mpi.h>

node_id_t WorkerCluster::num_nodes;
int WorkerCluster::total_processes;
int WorkerCluster::num_workers;
uint64_t WorkerCluster::seed;
int WorkerCluster::max_msg_size;
bool WorkerCluster::active = false;
constexpr int WorkerCluster::num_msg_forwarders;

int WorkerCluster::start_cluster(node_id_t n_nodes, uint64_t _seed, int batch_size) {
  num_nodes = n_nodes;
  seed = _seed;
  max_msg_size = (2*sizeof(node_id_t) + sizeof(node_id_t) * batch_size) * num_batches + sizeof(int);
  active = true;

  MPI_Comm_size(MPI_COMM_WORLD, &total_processes);
  num_workers = total_processes - distrib_worker_offset; // don't count msg forwarders and main

  // Initialize the MessageForwarders
  size_t init_fwd_size = sizeof(max_msg_size) + sizeof(num_workers);
  char init_fwd[init_fwd_size];
  memcpy(init_fwd, &max_msg_size, sizeof(max_msg_size));
  memcpy(init_fwd + sizeof(max_msg_size), &num_workers, sizeof(num_workers));
  std::cout << "Number of Message Forwarders: " << distrib_worker_offset - 1 << std::endl;
  for (int i = 0; i < distrib_worker_offset - 1; i++)
    MPI_Send(init_fwd, init_fwd_size, MPI_CHAR, i+1, INIT, MPI_COMM_WORLD);

  // Initialize the DistributedWorkers
  std::cout << "Number of workers is " << num_workers << ". Initializing!" << std::endl;
  size_t init_size = sizeof(num_nodes) + sizeof(seed) + sizeof(max_msg_size);
  char init_data[init_size];
  memcpy(init_data, &num_nodes, sizeof(num_nodes));
  memcpy(init_data + sizeof(num_nodes), &seed, sizeof(seed));
  memcpy(init_data + sizeof(num_nodes) + sizeof(seed), &max_msg_size, sizeof(max_msg_size));
  for (int i = 0; i < num_workers; i++)
    MPI_Ssend(init_data, init_size, MPI_CHAR, i + distrib_worker_offset, INIT, MPI_COMM_WORLD);

  // std::cout << "Done initializing cluster" << std::endl;
  return num_workers;
}

uint64_t WorkerCluster::stop_cluster() {
  // std::cout << "STOPPING CLUSTER!" << std::endl;
  for (int i = 1; i < distrib_worker_offset; i++) {
    // send stop message to MessageForwarder
    MPI_Send(nullptr, 0, MPI_CHAR, i, STOP, MPI_COMM_WORLD);
  }

  uint64_t total_updates = 0;
  for (int i = distrib_worker_offset; i < total_processes; i++) {
    // send stop message to worker i+1 (message is empty, just the STOP tag)
    MPI_Send(nullptr, 0, MPI_CHAR, i, STOP, MPI_COMM_WORLD);
    uint64_t upds;
    MPI_Recv(&upds, sizeof(uint64_t), MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    total_updates += upds;
  }
  return total_updates;
}

void WorkerCluster::shutdown_cluster() {
  // std::cout << "SHUTTING DOWN CLUSTER!" << std::endl;
  for (int i = 1; i < total_processes; i++) {
    // send SHUTDOWN message to worker i+1 (message is empty, just the SHUTDOWN tag)
    MPI_Send(nullptr, 0, MPI_CHAR, i, SHUTDOWN, MPI_COMM_WORLD);
  }
  active = false;
}

void WorkerCluster::send_batches(int fid, const std::vector<update_batch> &batches,
 char *msg_buffer) {
  node_id_t msg_bytes = 0;

  if (fid < 1 || fid > num_msg_forwarders) {
    throw BadMessageException("send_batches(): Bad process ID");
  }

  for (auto batch : batches) {
    if (batch.upd_vec.size() > 0) {
      // serialize batch to char *
      node_id_t node_idx = batch.node_idx;
      node_id_t dests_size = batch.upd_vec.size();

      // write header info -- node id and size of batch
      memcpy(msg_buffer + msg_bytes, &node_idx, sizeof(node_id_t));
      memcpy(msg_buffer + msg_bytes + sizeof(node_idx), &dests_size, sizeof(node_id_t));

      // write the batch data
      memcpy(msg_buffer + msg_bytes + 2*sizeof(node_idx), batch.upd_vec.data(), dests_size * sizeof(node_id_t));
      msg_bytes += dests_size * sizeof(node_id_t) + 2 * sizeof(node_id_t);
    }
  }
  // Send the message to the worker
  MPI_Send(msg_buffer, msg_bytes, MPI_CHAR, fid, BATCH, MPI_COMM_WORLD);
}

void WorkerCluster::parse_and_apply_deltas(char *msg_buffer, int msg_size, Supernode *delta,
                                           GraphDistribUpdate *graph) {
  // parse the message into Supernodes
  imemstream msg_stream(msg_buffer, msg_size);
  for (node_id_t d = 0; d < WorkerCluster::num_batches && msg_stream.tellg() < msg_size; d++) {
    // read node_idx and Supernode from message
    node_id_t node_idx;
    msg_stream.read((char *) &node_idx, sizeof(node_id_t));
    Supernode::makeSupernode(num_nodes, seed, msg_stream, delta);
    graph->get_supernode(node_idx)->apply_delta_update(delta);
  }
}

MessageCode WorkerCluster::recv_message(char *msg_addr, int &msg_size, int &msg_src) {
  MPI_Status status;
  MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
  int temp_size;
  MPI_Get_count(&status, MPI_CHAR, &temp_size);
  // ensure the message is not too large for us to recieve
  if (temp_size > msg_size) {
    throw BadMessageException("Size of recieved message is too large: " + std::to_string(temp_size));
  }
  msg_size = temp_size;
  msg_src = status.MPI_SOURCE;

  // recieve the message and write it to the msg_addr
  MPI_Recv(msg_addr, msg_size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  return (MessageCode) status.MPI_TAG;
}

MessageCode WorkerCluster::recv_message_from(int source, char* msg_addr, int& msg_size) {
  MPI_Status status;
  MPI_Probe(source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
  int temp_size;
  MPI_Get_count(&status, MPI_CHAR, &temp_size);
  // ensure the message is not too large for us to recieve
  if (temp_size > msg_size) {
    throw BadMessageException("Size of recieved message is too large: " + std::to_string(temp_size));
  }
  msg_size = temp_size;

  // recieve the message and write it to the msg_addr
  MPI_Recv(msg_addr, msg_size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  return (MessageCode) status.MPI_TAG;
}

void WorkerCluster::parse_batches(char *msg_addr, int msg_size, std::vector<batch_t> &batches) {
  int offset = 0;
  while (offset < msg_size) {
    batch_t batch;
    node_id_t batch_size;
    memcpy(&batch.first, msg_addr + offset, sizeof(node_id_t)); // node id
    memcpy(&batch_size, msg_addr + offset + sizeof(node_id_t), sizeof(node_id_t)); // batch size
    batch.second.reserve(batch_size); // TODO: memory allocation
    offset += 2 * sizeof(node_id_t);

    // parse the batch
    for (node_id_t i = 0; i < batch_size; i++) {
      batch.second.push_back(*(node_id_t *)(msg_addr + offset));
      offset += sizeof(node_id_t);
    }

    batches.push_back(batch);
  }
}

void WorkerCluster::serialize_delta(const node_id_t node_idx, Supernode &delta, 
 std::ostream &serial_str) {
  serial_str.write((const char *) &node_idx, sizeof(node_id_t));
  delta.write_binary(serial_str);
}

void WorkerCluster::return_deltas(int dst_id, char* delta_msg, size_t delta_msg_size) {
  MPI_Send(delta_msg, delta_msg_size, MPI_CHAR, dst_id, DELTA, MPI_COMM_WORLD);
}

void WorkerCluster::send_upds_processed(uint64_t num_updates) {
  MPI_Send(&num_updates, sizeof(uint64_t), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}
