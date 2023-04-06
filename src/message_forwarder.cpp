#include "message_forwarder.h"

#include "mpi.h"

void MessageForwarder::run() {
  while(running) {
    msg_size = max_msg_size;
    int msg_src;
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
  // Wait for the previous send to complete if there was one
  if (!first_batch) MPI_Wait(&batch_send_req, MPI_STATUS_IGNORE);
  first_batch = false;
  std::swap(msg_buffer, batch_buffer); // prepare for recieving next message

  int destination_id;
  memcpy(&destination_id, batch_buffer, sizeof(int));

  MPI_Isend(batch_buffer + sizeof(int), msg_size - sizeof(int), MPI_CHAR, destination_id, BATCH,
            MPI_COMM_WORLD, &batch_send_req);
}

void MessageForwarder::send_flush() {
  if (!first_batch) MPI_Wait(&batch_send_req, MPI_STATUS_IGNORE);
  first_batch = false;
  std::swap(msg_buffer, batch_buffer); // prepare for recieving next message

  int destination_id = *(int*)batch_buffer;

  MPI_Isend(nullptr, 0, MPI_CHAR, destination_id, FLUSH, MPI_COMM_WORLD, &batch_send_req);
}

void MessageForwarder::send_delta() {
  // Wait for the previous send to complete if there was one
  if (!first_delta) MPI_Wait(&delta_send_req, MPI_STATUS_IGNORE);
  first_delta = false;
  std::swap(msg_buffer, delta_buffer); // prepare for recieving next message

  MPI_Isend(delta_buffer, msg_size, MPI_CHAR, 0, DELTA, MPI_COMM_WORLD, &delta_send_req);
}

void MessageForwarder::cleanup() {
  if (msg_buffer != nullptr)
    free(msg_buffer);
  if (batch_buffer != nullptr)
    free(batch_buffer);
  if (delta_buffer != nullptr)
    free(delta_buffer);
  msg_buffer = nullptr;
  batch_buffer = nullptr;
  delta_buffer = nullptr;
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
  msg_buffer = (char*) malloc(max_msg_size * sizeof(char));
  batch_buffer = (char*) malloc(max_msg_size * sizeof(char));
  delta_buffer = (char*) malloc(max_msg_size * sizeof(char));

  first_delta = true;
  first_batch = true;
}
