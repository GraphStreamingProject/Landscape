#include "message_forwarder.h"

#include "mpi.h"

void MessageForwarder::run() {
  while(running) {
    msg_size = max_msg_size;
    int msg_src;
    // std::cout << "MessageForwarder: " << id << " waiting for message ..." << std::endl;
    MessageCode code = WorkerCluster::recv_message(msg_buffer, msg_size, msg_src);
    switch (code) {
      case BATCH:
        // The MessageForwarder sends to one of the associated DistributedWorkers
        send_batch();
        break;
      case FLUSH:
        send_flush();
        break;
      case DELTA:
        // The MessageForwarder sends this Delta back to main
        send_delta();
        break;
      case STOP:
        cleanup();
        init();
        break;
      case SHUTDOWN:
        cleanup();
        running = false;
        break;
      default:
        throw BadMessageException("MessageForwarder did not recognize MessageCode!");
        break;
    }
  }
}

void MessageForwarder::send_batch() {
  int which_buf;
  if (num_batch_sent < num_distrib) {
    which_buf = num_batch_sent;
    ++num_batch_sent;
  }
  else {
    // wait for a previous message to finish
    // std::cout << "MessageForwarder: " << id << " waiting for previous batch to complete ..." << std::endl;
    MPI_Waitany(num_distrib, batch_requests, &which_buf, MPI_STATUS_IGNORE);
  }

  std::swap(msg_buffer, batch_msg_buffers[which_buf]);
  MPI_Isend(batch_msg_buffers[which_buf], msg_size, MPI_CHAR, which_buf + distrib_offset,
            BATCH, MPI_COMM_WORLD, &batch_requests[which_buf]);
}

void MessageForwarder::send_delta() {
  int which_buf;
  if (num_delta_sent < num_distrib) {
    which_buf = num_delta_sent;
    ++num_delta_sent;
  } 
  else {
    // wait for a previous message to finish
    // std::cout << "MessageForwarder: " << id << " waiting for previous delta to complete ..." << std::endl;
    MPI_Waitany(num_distrib, delta_requests, &which_buf, MPI_STATUS_IGNORE);
  }

  std::swap(msg_buffer, delta_msg_buffers[which_buf]);
  MPI_Isend(delta_msg_buffers[which_buf], msg_size, MPI_CHAR, 0, DELTA, MPI_COMM_WORLD,
            &delta_requests[which_buf]);
}

void MessageForwarder::send_flush() {
  // std::cout << "MessageForwarder: " << id << " sending flush to workers" << std::endl;
  for (int i = 0; i < num_distrib; i++) {
    int destination_id = i + distrib_offset;
    MPI_Send(nullptr, 0, MPI_CHAR, destination_id, FLUSH, MPI_COMM_WORLD);
  }
}

void MessageForwarder::cleanup() {
  if (msg_buffer != nullptr)
    free(msg_buffer);

  for (int i = 0; i < num_distrib; i++) {
    delete[] batch_msg_buffers[i];
    delete[] delta_msg_buffers[i];
  }
  delete[] batch_msg_buffers;
  delete[] batch_requests;
  delete[] delta_msg_buffers;
  delete[] delta_requests;
  msg_buffer = nullptr;
}

void MessageForwarder::init() {
  char init_buffer[init_msg_size];
  msg_size = init_msg_size;
  MessageCode code = WorkerCluster::recv_message(init_buffer, msg_size);
  if (code == SHUTDOWN) { // if we get a shutdown message than exit
    // std::cout << "MessageForwarder " << id << " shutting down " << std::endl;
    running = false;
    return;
  }

  if (code != INIT)
    throw BadMessageException("MessageForwarder: Expected INIT");

  if (msg_size != init_msg_size)
    throw BadMessageException("MessageForwarder: INIT message of wrong length");

  memcpy(&max_msg_size, init_buffer, sizeof(max_msg_size));
  memcpy(&WorkerCluster::num_workers, init_buffer + sizeof(max_msg_size), sizeof(WorkerCluster::num_workers));
  msg_buffer = new char[max_msg_size];

  // calculate the number of DistributedWorkers we will communicate with
  int min = (id-1) * ceil((double)WorkerCluster::num_workers / WorkerCluster::num_msg_forwarders);
  int max = id * ceil((double)WorkerCluster::num_workers / WorkerCluster::num_msg_forwarders);

  // std::cout << "MessageForwarder: " << id << " min = " << min << " max = " << max << std::endl;
  num_distrib = max - min;
  distrib_offset = min + WorkerCluster::distrib_worker_offset;

  // build message structs
  batch_msg_buffers = new char*[num_distrib];
  batch_requests = new MPI_Request[num_distrib];
  delta_msg_buffers = new char*[num_distrib];
  delta_requests = new MPI_Request[num_distrib];
  for (int i = 0; i < num_distrib; i++) {
    batch_msg_buffers[i] = new char[max_msg_size];
    delta_msg_buffers[i] = new char[max_msg_size];
  }

  num_batch_sent = 0;
  num_delta_sent = 0;
}
