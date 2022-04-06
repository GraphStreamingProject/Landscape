#include "worker_cluster.h"
#include "work_distributor.h"

#include <iostream>
#include <mpi.h>
#include <iomanip>
#include <cstring>

node_id_t WorkerCluster::num_nodes;
int WorkerCluster::num_workers;
uint64_t WorkerCluster::seed;
int WorkerCluster::max_msg_size;
bool WorkerCluster::active = false;
char* WorkerCluster::msg_buffer;

int WorkerCluster::start_cluster(node_id_t n_nodes, uint64_t _seed, int batch_size) {
  num_nodes = n_nodes;
  seed = _seed;
  max_msg_size = (2 * sizeof(node_id_t) + sizeof(node_id_t) * batch_size) * num_batches;
  active = true;

  MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
  num_workers--; // don't count the main node

  msg_buffer = static_cast<char *>(malloc(num_workers * max_msg_size));

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

void WorkerCluster::send_batches(int wid, const std::vector<WorkQueue::DataNode *> &batches,
 char *msg_buffer) {
  node_id_t msg_bytes = 0;
  for (auto batch : batches) {
    // serialize batch to char *
    node_id_t node_idx = batch->get_node_idx();
    std::vector<node_id_t> dests = batch->get_data_vec();
    node_id_t dests_size = dests.size();

    // write header info -- node id and size of batch
    memcpy(msg_buffer + msg_bytes, &node_idx, sizeof(node_id_t));
    memcpy(msg_buffer + msg_bytes + sizeof(node_idx), &dests_size, sizeof(node_id_t));

    // write the batch data
    memcpy(msg_buffer + msg_bytes + 2*sizeof(node_idx), dests.data(), dests_size * sizeof(node_id_t));
    msg_bytes += dests_size * sizeof(node_id_t) + 2 * sizeof(node_id_t);
  }
  // Send the message to the worker, use a non-blocking synchronous call to avoid copying data
  // to a local buffer and without blocking the calling process.
  MPI_Request request; // used for verifying message has been completed but that is unnecessary here
  MPI_Issend(msg_buffer, msg_bytes, MPI_CHAR, wid, BATCH, MPI_COMM_WORLD, &request);
}

void WorkerCluster::recv_deltas(int wid, node_sketch_pairs_t &deltas, node_id_t num_deltas, 
 char *msg_buffer) {
  // Wait for deltas to be returned
  int message_size = 0;
  MPI_Status status;
  MPI_Probe(wid, 0, MPI_COMM_WORLD, &status); // wait for a message from worker wid
  MPI_Get_count(&status, MPI_CHAR, &message_size);
  if (message_size > max_msg_size) throw BadMessageException("Deltas returned too big!");
  MPI_Recv(msg_buffer, message_size, MPI_CHAR, wid, 0, MPI_COMM_WORLD, &status);

  // parse the message into Supernodes
  std::stringstream msg_stream(std::string(msg_buffer, message_size));
  for (node_id_t i = 0; i < num_deltas; i++) {
    // read node_idx and Supernode from message
    node_id_t node_idx;
    msg_stream.read((char *) &node_idx, sizeof(node_id_t));
    deltas[i].first = node_idx;
    Supernode::makeSupernode(num_nodes, seed, msg_stream, deltas[i].second);
  }
}

std::vector<std::pair<Edge, SampleSketchRet>> WorkerCluster::send_sketches_recv_queries
(int wid, const std::vector<Supernode*>& supernode_ptrs) {
  std::vector<std::pair<Edge, SampleSketchRet>> retval(supernode_ptrs.size());

  auto serial_begin = std::chrono::system_clock::now();
  // TODO: but we are going to run out of space if we have more than ~256*logV
  //  supernodes
  char *message = msg_buffer + (wid-1)*max_msg_size;
  int msg_bytes = supernode_ptrs.size() * Sketch::sketchSizeof();

  for (size_t i = 0; i < supernode_ptrs.size(); ++i) {
    const auto supernode = supernode_ptrs[i];
    if (supernode->out_of_queries()) throw OutOfQueriesException();
    auto sketch = supernode->get_const_sketch(supernode->curr_idx());
    std::memcpy(message + i*Sketch::sketchSizeof(), (char*) sketch,
           Sketch::sketchSizeof());
  }

  auto send_begin = std::chrono::system_clock::now();

  // Send the message to the worker
  MPI_Send(message, msg_bytes, MPI_CHAR, wid, QUERY, MPI_COMM_WORLD);

  auto send_end = std::chrono::system_clock::now();

  // Wait for deltas to be returned
  int message_size = 0;
  MPI_Status status;
  MPI_Probe(wid, 0, MPI_COMM_WORLD, &status); // wait for a message from worker wid
  MPI_Get_count(&status, MPI_CHAR, &message_size);
  char *msg_data = msg_buffer + (wid-1)*max_msg_size;
  MPI_Recv(msg_data, message_size, MPI_CHAR, wid, 0, MPI_COMM_WORLD, &status);

  auto recv_end = std::chrono::system_clock::now();

  // parse the message into query results
  std::stringstream msg_stream(std::string(msg_data, message_size));
  for (size_t i = 0; i < supernode_ptrs.size(); ++i) {
    msg_stream.read((char*) &retval[i], sizeof(retval[i]));
  }

  auto deserial_end = std::chrono::system_clock::now();

  // print timestamps
  long disp = 1649215000;
  if (wid == 2) {
    std::cout << std::setprecision(20);
    std::cout << wid << "\t" << 0 << "\t" <<
              std::chrono::duration<long double>(
                    serial_begin.time_since_epoch())
                    .count() - disp << "\n";
    std::cout << wid << "\t" << 1 << "\t" <<
              std::chrono::duration<long double>(send_begin.time_since_epoch())
                    .count() - disp << "\n";
    std::cout << wid << "\t" << 2 << "\t" <<
              std::chrono::duration<long double>(send_end.time_since_epoch())
                    .count() - disp << "\n";
    std::cout << wid << "\t" << 7 << "\t" <<
              std::chrono::duration<long double>(recv_end.time_since_epoch())
                    .count() - disp << "\n";
    std::cout << wid << "\t" << 8 << "\t" <<
              std::chrono::duration<long double>(
                    deserial_end.time_since_epoch())
                    .count() - disp << "\n";
  }
  return retval;
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
    memcpy(&batch.first, msg_addr + offset, sizeof(node_id_t));
    memcpy(&batch_size, msg_addr + offset + sizeof(node_id_t), sizeof(node_id_t));
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
 std::stringstream &serial_str) {
  serial_str.write((const char *) &node_idx, sizeof(node_id_t));
  delta.write_binary(serial_str);
}

void WorkerCluster::return_deltas(const std::string delta_msg) {
  MPI_Ssend(delta_msg.data(), delta_msg.length(), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}

void WorkerCluster::send_upds_processed(uint64_t num_updates) {
  MPI_Send(&num_updates, sizeof(uint64_t), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}

void WorkerCluster::serialize_samples(std::vector<std::pair<Edge,
        SampleSketchRet>>& samples, std::stringstream& serial_str) {
  for (auto & sample : samples) {
    serial_str.write((const char*) &sample, sizeof(sample));
  }
  serial_str.flush();
}

void WorkerCluster::return_samples(const std::string& sample_msg) {
  MPI_Send(sample_msg.data(), sample_msg.size(), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}
