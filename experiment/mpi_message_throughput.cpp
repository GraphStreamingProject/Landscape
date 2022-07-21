#include <mpi.h>
#include <iostream>
#include <chrono>
#include <thread>

constexpr size_t MB = 1024 * 1024;

/*
 * This code is designed to test the speed of MPI in a variety of contexts
 * Ideas for parameters:
 * 1. Number of threads sending (num recv determined by -np)
 * 2. Thread support level
 * 3. Probe before Recv or not
 * 4. Ssend vs Send vs Issend vs Isend
 * 5. Message size
 * 6. Number of messages
 * 7. Random vs identical messages
 * 8. Prepopulate messages or generate on the fly
 * 9. Recievers send ACK or messages of their own
 */

int main(int argc, char **argv) {
  // TODO: Allow threading mode to be configurable
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);
  if (provided < MPI_THREAD_SINGLE){
    std::cout << "ERROR! Could not achieve MPI_THREAD_SINGLE" << std::endl;
    exit(EXIT_FAILURE);
  }
  int num_processes = 0;
  MPI_Comm_size(MPI_COMM_WORLD, &num_processes);
  if (num_processes < 2) {
    throw std::invalid_argument("number of mpi processes must be at least 2!");
  }

  size_t num_messages  = 1000000;
  size_t message_bytes = 100;
  char message[message_bytes];

  size_t ack_bytes = 4;

  // Get MPI Rank
  int proc_id;
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);
  if (proc_id > 0) {
    // prepare ack buffer
    char ack[ack_bytes] = "ACK\0";

    for (size_t i = 0; i < num_messages; i++) {
      // Receive message from sender
      MPI_Recv(message, message_bytes, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, nullptr);
      
      // Send ack
      MPI_Send(ack, ack_bytes, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
  } else {
    // prepare message
    for (size_t i = 0; i < message_bytes; i++) {
      message[i] = i % 93 + 33;
    }
    message[message_bytes - 1] = 0;

    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < num_messages; i++) {
      // prepare buffer for response
      char ack_buffer[ack_bytes];

      // send message
      MPI_Send(message, message_bytes, MPI_CHAR, 1, 0, MPI_COMM_WORLD);

      // recv ack message
      MPI_Recv(ack_buffer, ack_bytes, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, nullptr);
    }

    std::chrono::duration<double> time = std::chrono::steady_clock::now() - start;
    MPI_Finalize();
    std::cout << "Total time:          " << time.count() << "(seconds)" << std::endl;
    std::cout << "Latency per message: " << time.count() / num_messages * 1e9 << "(ns)" << std::endl;
    std::cout << "Messages per second: " << num_messages / time.count() / 1e6 << "(millions)" << std::endl;
    std::cout << "Data Throughput:     " << num_messages * message_bytes / time.count() / MB << "(MiB)" << std::endl;
  }
}
