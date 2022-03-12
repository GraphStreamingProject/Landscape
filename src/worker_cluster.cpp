#include "worker_cluster.h"
#include <iostream>

node_id_t WorkerCluster::num_nodes;
int WorkerCluster::num_workers;
uint64_t WorkerCluster::seed;
int WorkerCluster::max_msg_size;

int WorkerCluster::start_cluster(node_id_t n_nodes, uint64_t _seed, int batch_size) {
  num_nodes = n_nodes;
  seed = _seed;
  max_msg_size = (sizeof(node_id_t) + sizeof(size_t) + batch_size) * num_batches;

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
}

node_sketch_pairs WorkerCluster::send_batches_recv_deltas(int wid, const std::vector<data_ret_t> &batches) {
  node_sketch_pairs ret(batches.size()); // vector of node_sketch_pair type to return deltas

  char *message = new char[max_msg_size];
  node_id_t msg_bytes = 0;
  for (auto &batch : batches) {
    // serialize batch to char *
    node_id_t node_idx = batch.first;
    std::vector<size_t> dests = batch.second;
    node_id_t dests_size = dests.size();

    // write header info -- node id and size of batch
    memcpy(message + msg_bytes, &node_idx, sizeof(node_id_t));
    memcpy(message + msg_bytes + sizeof(node_idx), &dests_size, sizeof(node_id_t));

    // write the batch data
    memcpy(message + msg_bytes + 2*sizeof(node_idx), dests.data(), dests_size * sizeof(size_t));
    msg_bytes += dests_size * sizeof(size_t) + 2 * sizeof(node_id_t);
  }
  // Send the message to the worker
  MPI_Send(message, msg_bytes, MPI_CHAR, wid, BATCH, MPI_COMM_WORLD);
  delete[] message; // TODO: would be more efficient to reuse this memory

  // Wait for deltas to be returned
  int message_size = 0;
  MPI_Status status;
  MPI_Probe(wid, 0, MPI_COMM_WORLD, &status); // wait for a message from worker wid
  MPI_Get_count(&status, MPI_CHAR, &message_size);
  char *msg_data = new char[message_size];
  MPI_Recv(msg_data, message_size, MPI_CHAR, wid, 0, MPI_COMM_WORLD, &status);

  // parse the message into Supernodes
  std::stringstream msg_stream(std::string(msg_data, message_size));
  for (node_id_t i = 0; i < batches.size(); i++) {
    // read node_idx and Supernode from message
    node_id_t node_idx;
    msg_stream.read((char *) &node_idx, sizeof(node_id_t));
    Supernode *delta = Supernode::makeSupernode(num_nodes, seed, msg_stream);
    ret[i] = {node_idx, delta};
  }

  delete[] msg_data; // TODO: would be more efficient to reuse this memory
  return ret;
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

void WorkerCluster::parse_batches(char *msg_addr, int msg_size, std::vector<data_ret_t> &batches) {
  int offset = 0;
  while (offset < msg_size) {
    data_ret_t data;
    node_id_t batch_size;
    memcpy(&data.first, msg_addr + offset, sizeof(node_id_t));
    memcpy(&batch_size, msg_addr + offset + sizeof(node_id_t), sizeof(node_id_t));
    offset += 2 * sizeof(node_id_t);

    // parse the batch
    for (node_id_t i = 0; i < batch_size; i++) {
      data.second.push_back(*(size_t *)(msg_addr + offset));
      offset += sizeof(size_t);
    }

    batches.push_back(data);
  }
}

void WorkerCluster::serialize_delta(const node_id_t node_idx, Supernode &delta, 
 std::stringstream &serial_str) {
  serial_str.write((const char *) &node_idx, sizeof(node_id_t));
  delta.write_binary(serial_str);
}

void WorkerCluster::return_deltas(const std::string delta_msg) {
  MPI_Send(delta_msg.data(), delta_msg.length(), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}

void WorkerCluster::send_upds_processed(uint64_t num_updates) {
  MPI_Send(&num_updates, sizeof(uint64_t), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}
