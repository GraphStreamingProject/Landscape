#include <mpi.h>
#include <omp.h>
#include <iostream>
#include <chrono>
#include <thread>

#include "program_arguments.h"

constexpr size_t MB = 1024 * 1024;

enum TAG {
  STANDARD_MSG,
  SHUTDOWN
};

ArgumentResult thread_support_parse(char *arg) {
  ArgumentResult result;
  result.str_result = arg;
  if (result.str_result == "SINGLE" || result.str_result == "FUNNELED" || 
   result.str_result == "SERIALIZED" || result.str_result == "MULTIPLE")
    result.parsed_correctly = true;
  else {
    result.parsed_correctly = false;
    result.err = "Threading value: " + std::string(arg) + " is not valid";
  }
  return result;
};

ArgumentResult send_command_parse(char *arg) {
  ArgumentResult result;
  result.str_result = arg;
  if (result.str_result == "Send" || result.str_result == "Isend" || 
   result.str_result == "Issend" || result.str_result == "Ssend")
    result.parsed_correctly = true;
  else {
    result.parsed_correctly = false;
    result.err = "MPI send command value: " + std::string(arg) + " is not valid";
  }
  return result;
};

/*
 * This code is designed to test the speed of MPI in a variety of contexts
 */
int main(int argc, char **argv) {
  // parse program arguments
  std::vector<ArgumentDefinition> arguments;
  arguments.emplace_back("num_senders", "The number of threads or processes on main that send data.", &int_parser<1, 100>);
  arguments.emplace_back("thread_support", "MPI threading support level.", &thread_support_parse);
  arguments.emplace_back("send_command", "MPI send command used by main.", &send_command_parse);
  arguments.emplace_back("num_messages", "The number of messages to send.", &int_parser<1000, 10000000>);
  arguments.emplace_back("message_size", "The size of the message sent.", &int_parser<1,10000000>);
  arguments.emplace_back("probe", "[OPTIONAL] If present then Probe before Recv", &str_parser, true);
  arguments.emplace_back("random", "[OPTIONAL] If present then randomize data sent over network.", &str_parser, true);
  arguments.emplace_back("prepopulate", "[OPTIONAL] If present then create messages before starting timer.", &str_parser, true);
  arguments.emplace_back("recv_send", "[OPTIONAL] If present recievers send their own messages back.", &str_parser, true);
  ProgramArguments args_parser(arguments);

  std::unordered_map<std::string, ArgumentResult> args = args_parser.parse_arguments(argc, argv);

  // parse arguments
  int provided;
  int desired_threading = MPI_THREAD_SINGLE;
  if (args["thread_support"].str_result == "FUNNELED") desired_threading = MPI_THREAD_FUNNELED;
  else if (args["thread_support"].str_result == "SERIALIZED") desired_threading = MPI_THREAD_SERIALIZED;
  else if (args["thread_support"].str_result == "MULTIPLE") desired_threading = MPI_THREAD_MULTIPLE;

  MPI_Init_thread(&argc, &argv, desired_threading, &provided);
  if (provided < desired_threading){
    std::cout << "ERROR! Could not achieve desired threading level." << std::endl;
    exit(EXIT_FAILURE);
  }
  int num_processes = 0;
  MPI_Comm_size(MPI_COMM_WORLD, &num_processes);
  if (num_processes < 2) {
    throw std::invalid_argument("number of mpi processes must be at least 2!");
  }

  size_t sender_processes = 1;
  size_t num_senders   = args["num_senders"].int_result;
  size_t num_recievers = num_processes - 1;
  size_t num_messages  = args["num_messages"].int_result;
  size_t message_bytes = args["message_size"].int_result;

  if (num_senders > num_recievers)
    throw std::invalid_argument("Number of senders (" + std::to_string(num_senders) 
          + ") must be <= number of recievers (" + std::to_string(num_recievers) + ")");

  if (num_senders > 1 && args["thread_support"].str_result == "SINGLE")
    std::swap(sender_processes, num_senders); // multi-process rather than multi-thread

  // if using multiple sender_processes, limit number of recievers
  num_recievers = num_processes - sender_processes;

  size_t ack_bytes = 4;

  // Get MPI Rank
  int proc_id;
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);
  if (proc_id >= (int) sender_processes) {
    char message[message_bytes];
    // prepare ack buffer
    char ack[ack_bytes] = "ACK\0";

    while(true) {
      // Receive message from sender
      MPI_Status st;
      if (args.count("probe") > 0) {
        MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &st); // wait for a message
        MPI_Recv(message, message_bytes, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
      } else {
        MPI_Recv(message, message_bytes, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
      }

      if (st.MPI_TAG == SHUTDOWN) break;
      
      // Send ack
      MPI_Send(ack, ack_bytes, MPI_CHAR, st.MPI_SOURCE, 0, MPI_COMM_WORLD);
    }
    MPI_Finalize();
  } else {
    // print header with configuration
    std::cout << "recv: " << num_recievers << ", send: " << num_senders;
    std::cout << ", thread_support: " << args["thread_support"].str_result;
    std::cout << ", send_command: " << args["send_command"].str_result;
    std::cout << ", num_messages: " << num_messages << ", message_size: " << message_bytes;
    std::cout << std::endl << "Additional options:";
    if (args.count("probe") > 0)       std::cout << " probe";
    if (args.count("random") > 0)      std::cout << " random";
    if (args.count("prepopulate") > 0) std::cout << " prepopulate";
    if (args.count("recv_send") > 0)   std::cout << " recv_send";
    std::cout << std::endl;

    char message[message_bytes];
    std::chrono::time_point<std::chrono::steady_clock> start;
    #pragma omp parallel num_threads(num_senders) shared(start)
    {
      // prepare buffer for response
      char ack_buffer[ack_bytes];

      // prepare message
      for (size_t i = 0; i < message_bytes; i++) {
        message[i] = i % 93 + 33;
      }
      message[message_bytes - 1] = 0;
      int thr_id = omp_get_thread_num() + 1;

      // define the recievers this thread is responsible for sending to
      size_t senders = std::max(sender_processes, num_senders);
      int temp_r = num_recievers;
      int min_recieve = 1; // above and including this id
      for (int i = 0; i < thr_id-1; i++) {
        min_recieve += temp_r / (senders-i);
        temp_r -= temp_r / (senders-i);
      }
      // up to but not including this id
      int max_recieve = min_recieve + temp_r / (senders-thr_id+1);
      int cur_recv = min_recieve;

      #pragma omp barrier
      #pragma omp single
      {
        start = std::chrono::steady_clock::now();
      }
      #pragma omp barrier

      if (args["send_command"].str_result == "Isend" || args["send_command"].str_result == "Issend") {
        // send some messages in advance to take advantage of async
        #pragma omp for
        for (int i = min_recieve; i < max_recieve; i++) {
          for (size_t j = 0; j < 2; j++) {
            if (args["send_command"].str_result == "Isend") {
              MPI_Request rq;
              MPI_Isend(message, message_bytes, MPI_CHAR, i, STANDARD_MSG, MPI_COMM_WORLD, &rq);
            }
            else {
              MPI_Request rq;
              MPI_Issend(message, message_bytes, MPI_CHAR, i, STANDARD_MSG, MPI_COMM_WORLD, &rq);
            }
          }
        }
      }

      #pragma omp for
      for (size_t i = 0; i < num_messages; i++) {
        std::cout << "Sending to: " << cur_recv << std::endl;
        // send message
        if (args["send_command"].str_result == "Send")
          MPI_Send(message, message_bytes, MPI_CHAR, cur_recv, STANDARD_MSG, MPI_COMM_WORLD);
        else if (args["send_command"].str_result == "Ssend")
          MPI_Ssend(message, message_bytes, MPI_CHAR, cur_recv, STANDARD_MSG, MPI_COMM_WORLD);
        else if (args["send_command"].str_result == "Isend") {
          MPI_Request rq;
          MPI_Isend(message, message_bytes, MPI_CHAR, cur_recv, STANDARD_MSG, MPI_COMM_WORLD, &rq);
        }
        else {
          MPI_Request rq;
          MPI_Issend(message, message_bytes, MPI_CHAR, cur_recv, STANDARD_MSG, MPI_COMM_WORLD, &rq);
        }

        // recv ack message
        MPI_Status st;
        if (args.count("probe") > 0) {
          MPI_Probe(cur_recv, MPI_ANY_TAG, MPI_COMM_WORLD, &st); // wait for a message
          MPI_Recv(ack_buffer, ack_bytes, MPI_CHAR, cur_recv, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
        } else {
          MPI_Recv(ack_buffer, ack_bytes, MPI_CHAR, cur_recv, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
        }

        ++cur_recv;
        if (cur_recv >= max_recieve) cur_recv = min_recieve;
      }

      // If Isend or Issend deal with remaining messages via Recv
      if (args["send_command"].str_result == "Isend" || args["send_command"].str_result == "Issend") {
        #pragma omp for
        for (int i = min_recieve; i < max_recieve; i++) {
          for (size_t j = 0; j < 2; j++)
            MPI_Recv(ack_buffer, ack_bytes, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, nullptr);
        }
      }
    }
    // Stop cluster
    std::chrono::duration<double> time = std::chrono::steady_clock::now() - start;
    for (int i = 1; i < num_processes; i++) {
      MPI_Send(nullptr, 0, MPI_CHAR, i, SHUTDOWN, MPI_COMM_WORLD);
    }
    MPI_Finalize();

    std::cout << "Total time:          " << time.count() << " (seconds)" << std::endl;
    std::cout << "Latency per message: " << time.count() / num_messages * 1e9 << " (ns/sec)" << std::endl;
    std::cout << "Data Throughput:     " << num_messages * message_bytes / time.count() / MB << " (MiB/sec)" << std::endl;
  }
}
