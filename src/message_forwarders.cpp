#include "message_forwarders.h"

#include "mpi.h"


/*******************************************************\
|  BatchMessageForwarder class: Recieves BATCH messages |
|  from main process and then forwards the message over |
|  the network to DistributedWorkers.                   |
\*******************************************************/
void BatchMessageForwarder::run() {
  while(running) {
    msg_size = max_msg_size;
    int msg_src;
    // std::cout << "BatchMessageForwarder: " << id << " waiting for message ..." << std::endl;
    MessageCode code = WorkerCluster::recv_message(msg_buffer, msg_size, msg_src);
    switch (code) {
      case BATCH:
        // The BatchMessageForwarder sends to one of the associated DistributedWorkers
        send_batch();
        break;
      case FLUSH:
        send_flush();
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
        throw BadMessageException("BatchMessageForwarder did not recognize MessageCode!");
        break;
    }
  }
}

void BatchMessageForwarder::send_batch() {
  int which_buf;
  if (num_batch_sent < num_distrib) {
    which_buf = num_batch_sent;
    ++num_batch_sent;
  }
  else {
    // wait for a previous message to finish
    // std::cout << "BatchMessageForwarder: " << id << " waiting for previous batch to complete ..." << std::endl;
    MPI_Waitany(num_distrib, batch_requests, &which_buf, MPI_STATUS_IGNORE);
  }

  // std::cout << "BatchMessageForwarder: " << id << " sending to " << which_buf + distrib_offset << std::endl;
  std::swap(msg_buffer, batch_msg_buffers[which_buf]);
  MPI_Isend(batch_msg_buffers[which_buf], msg_size, MPI_CHAR, which_buf + distrib_offset,
            BATCH, MPI_COMM_WORLD, &batch_requests[which_buf]);
}

void BatchMessageForwarder::send_flush() {
  // std::cout << "BatchMessageForwarder: " << id << " sending flush to workers" << std::endl;
  for (int i = 0; i < num_distrib; i++) {
    int destination_id = i + distrib_offset;
    MPI_Send(nullptr, 0, MPI_CHAR, destination_id, FLUSH, MPI_COMM_WORLD);
  }
}

void BatchMessageForwarder::cleanup() {
  delete[] msg_buffer;
  for (int i = 0; i < num_distrib; i++)
    delete[] batch_msg_buffers[i];
  delete[] batch_msg_buffers;
  delete[] batch_requests;
}

void BatchMessageForwarder::init() {
  char init_buffer[init_msg_size];
  msg_size = init_msg_size;
  MessageCode code = WorkerCluster::recv_message(init_buffer, msg_size);
  if (code == SHUTDOWN) { // if we get a shutdown message than exit
    // std::cout << "BatchMessageForwarder " << id << " shutting down " << std::endl;
    running = false;
    return;
  }
  if (code != INIT)
    throw BadMessageException("BatchMessageForwarder: Expected INIT");
  if (msg_size != init_msg_size)
    throw BadMessageException("BatchMessageForwarder: INIT message of wrong length");

  memcpy(&max_msg_size, init_buffer, sizeof(max_msg_size));
  memcpy(&WorkerCluster::num_workers, init_buffer + sizeof(max_msg_size), sizeof(WorkerCluster::num_workers));
  msg_buffer = new char[max_msg_size];

  // calculate the number of DistributedWorkers we will communicate with
  int min = ceil((id-1) * (double)WorkerCluster::num_workers / WorkerCluster::num_msg_forwarders);
  int max = ceil(id * (double)WorkerCluster::num_workers / WorkerCluster::num_msg_forwarders);

  // Edge case for when num_workers < num_msg_forwarders
  if (WorkerCluster::num_workers < WorkerCluster::num_msg_forwarders) {
    min = id-1;
    max = id-1 < WorkerCluster::num_workers ? id : id-1;
  }

  // std::cout << "BatchMessageForwarder: " << id << " min = " << min << " max = " << max << std::endl;
  num_distrib = max - min;
  distrib_offset = min + WorkerCluster::distrib_worker_offset;

  // build message structs
  batch_msg_buffers = new char*[num_distrib];
  batch_requests = new MPI_Request[num_distrib];
  for (int i = 0; i < num_distrib; i++)
    batch_msg_buffers[i] = new char[max_msg_size];

  num_batch_sent = 0;
}

/*******************************************************\
|  DeltaMessageForwarder class: Recieves DELTA messages |
|  over the network from DistributedWorkers and then    |
|  forwards that message locally to the main process.   |
\*******************************************************/
void DeltaMessageForwarder::run() {
  while(running) {
    msg_size = max_msg_size;
    int msg_src;
    // std::cout << "DeltaMessageForwarder: " << id << " waiting for message ..." << std::endl;
    MessageCode code = WorkerCluster::recv_message(msg_buffer, msg_size, msg_src);
    switch (code) {
      case DELTA:
        // The DeltaMessageForwarder sends to one of the associated DistributedWorkers
        send_delta();
        break;
      case FLUSH:
        process_distrib_worker_done();
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
        std::cerr << code << " " << msg_src << std::endl;
        throw BadMessageException("DeltaMessageForwarder did not recognize MessageCode!");
        break;
    }
  }
}

void DeltaMessageForwarder::send_delta() {
  // std::cout << "DeltaMessageForwarder " << id << " forwarding delta" << std::endl;
  MPI_Send(msg_buffer, msg_size, MPI_CHAR, WorkerCluster::leader_proc, DELTA, MPI_COMM_WORLD);
}

void DeltaMessageForwarder::process_distrib_worker_done() {
  num_distrib_flushed += 1;
  // std::cout << "DeltaMessageForwarder " << id << " got flush from " << num_distrib_flushed << "/"
  //           << num_distrib << std::endl;
  if (num_distrib_flushed >= num_distrib) {
    MPI_Send(nullptr, 0, MPI_CHAR, WorkerCluster::leader_proc, FLUSH, MPI_COMM_WORLD);
    num_distrib_flushed = 0;
  }
}

void DeltaMessageForwarder::cleanup() {
  delete[] msg_buffer;
}

void DeltaMessageForwarder::init() {
  char init_buffer[init_msg_size];
  msg_size = init_msg_size;
  MessageCode code = WorkerCluster::recv_message(init_buffer, msg_size);
  if (code == SHUTDOWN) { // if we get a shutdown message than exit
    // std::cout << "DeltaMessageForwarder " << id << " shutting down " << std::endl;
    running = false;
    return;
  }
  if (code != INIT)
    throw BadMessageException("DeltaMessageForwarder: Expected INIT");
  if (msg_size != init_msg_size)
    throw BadMessageException("DeltaMessageForwarder: INIT message of wrong length");

  memcpy(&max_msg_size, init_buffer, sizeof(max_msg_size));
  memcpy(&WorkerCluster::num_workers, init_buffer + sizeof(max_msg_size),
         sizeof(WorkerCluster::num_workers));
  msg_buffer = new char[max_msg_size];

  // calculate the number of DistributedWorkers we will communicate with
  int fid = WorkerCluster::delta_fwd_to_batch_fwd(id);
  int min = ceil((fid-1) * (double)WorkerCluster::num_workers / WorkerCluster::num_msg_forwarders);
  int max = ceil(fid * (double)WorkerCluster::num_workers / WorkerCluster::num_msg_forwarders);

  // Edge case for when num_workers < num_msg_forwarders
  if (WorkerCluster::num_workers < WorkerCluster::num_msg_forwarders) {
    min = fid-1;
    max = fid-1 < WorkerCluster::num_workers ? fid : fid-1;
  }
  
  

  num_distrib = max - min;
  num_distrib_flushed = 0;
  // std::cout << "DeltaMessageForwarder: " << id << " min = " << min << " max = " << max << std::endl;
}
