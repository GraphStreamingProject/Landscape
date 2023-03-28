#include "worker_cluster.h"
#include "work_distributor.h"
#include "memstream.h"

#include <iostream>
#include <mpi.h>

node_id_t WorkerCluster::num_nodes;
int WorkerCluster::num_workers;
uint64_t WorkerCluster::seed;
int WorkerCluster::max_msg_size;
bool WorkerCluster::active = false;

int WorkerCluster::start_cluster(node_id_t n_nodes, uint64_t _seed, int batch_size) {
  num_nodes = n_nodes;
  seed = _seed;
  max_msg_size = (2 * sizeof(node_id_t) + sizeof(node_id_t) * batch_size) * num_batches;
  active = true;

  MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
  num_workers--; // don't count the main node

  std::cout << "Number of workers is " << num_workers << ". Initializing!" << std::endl;
  for (int i = 0; i < num_workers; i++) {
    // build message => number of nodes, seed of graph, and max message size
    char init_data[sizeof(num_nodes) + sizeof(seed) + sizeof(max_msg_size)];
    memcpy(init_data, &num_nodes, sizeof(num_nodes));
    memcpy(init_data + sizeof(num_nodes), &seed, sizeof(seed));
    memcpy(init_data + sizeof(num_nodes) + sizeof(seed), &max_msg_size, sizeof(max_msg_size));
    
    // send message
    MPI_Send(init_data, sizeof(node_id_t) + sizeof(seed) + sizeof(max_msg_size), MPI_CHAR, 
      i+1, INIT, MPI_COMM_WORLD);
  }

  std::cout << "Done initializing cluster" << std::endl;
  return num_workers;
}

uint64_t WorkerCluster::stop_cluster() {
  uint64_t total_updates = 0;
  for (int i = 0; i < num_workers; i++) {
    // send stop message to worker i+1 (message is empty, just the STOP tag)
    MPI_Send(nullptr, 0, MPI_CHAR, i+1, STOP, MPI_COMM_WORLD);
    uint64_t upds;
    MPI_Recv(&upds, sizeof(uint64_t), MPI_CHAR, i+1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    total_updates += upds;
  }
  return total_updates;
}

void WorkerCluster::shutdown_cluster() {
  for (int i = 0; i < num_workers; i++) {
    // send shutdown message to worker i+1 (message is empty, just the SHUTDOWN tag)
    MPI_Send(nullptr, 0, MPI_CHAR, i+1, SHUTDOWN, MPI_COMM_WORLD);
  }
  active = false;
}

void WorkerCluster::send_batches(int wid, const std::vector<update_batch> &batches,
 char *msg_buffer) {
  node_id_t msg_bytes = 0;

  // std::cout << "Sending message with send_id " << send_id << std::endl;

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
  MPI_Ssend(msg_buffer, msg_bytes, MPI_CHAR, wid, BATCH, MPI_COMM_WORLD);
}

void WorkerCluster::recv_deltas(int src_id, node_sketch_pairs_t &deltas, size_t &num_deltas, 
 char* msg_buffer) {
  // Wait for deltas to be returned
  int message_size = 0;
  MPI_Status status;
  MPI_Probe(src_id, DELTA, MPI_COMM_WORLD, &status); // wait for a message from worker wid
  MPI_Get_count(&status, MPI_CHAR, &message_size);
  if (message_size > max_msg_size) throw BadMessageException("Deltas returned too big!");
  MPI_Recv(msg_buffer, message_size, MPI_CHAR, src_id, DELTA, MPI_COMM_WORLD, &status);

  // parse the message into Supernodes
  imemstream msg_stream(msg_buffer, message_size);
  node_id_t d;
  for (d = 0; d < num_deltas && msg_stream.tellg() < message_size; d++) {
    // read node_idx and Supernode from message
    node_id_t node_idx;
    msg_stream.read((char *) &node_idx, sizeof(node_id_t));
    deltas[d].first = node_idx;
    Supernode::makeSupernode(num_nodes, seed, msg_stream, deltas[d].second);
  }
  num_deltas = d;
}

MessageCode WorkerCluster::worker_recv_message(char *msg_addr, int *msg_size) {
  MPI_Status status;
  MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
  int temp_size;
  MPI_Get_count(&status, MPI_CHAR, &temp_size);
  // ensure the message is not too large for us to recieve
  if (temp_size > *msg_size) {
    throw BadMessageException("Size of recieved message is too large");
  }
  *msg_size = temp_size;

  // recieve the message and write it to the msg_addr
  MPI_Recv(msg_addr, *msg_size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

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

void WorkerCluster::return_deltas(char* delta_msg, size_t delta_msg_size) {
  MPI_Ssend(delta_msg, delta_msg_size, MPI_CHAR, 0, DELTA, MPI_COMM_WORLD);
}

void WorkerCluster::send_upds_processed(uint64_t num_updates) {
  MPI_Send(&num_updates, sizeof(uint64_t), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}
