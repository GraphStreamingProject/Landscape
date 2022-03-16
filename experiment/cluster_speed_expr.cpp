#include <graph_distrib_update.h>
#include <binary_graph_stream.h>
#include <work_distributor.h>

#include <string>
#include <iostream>
#include <thread>
#include <unistd.h>

static bool shutdown = false;

// Queries the work distributors for their current status and displays it on screen
void status_querier() {
  std::ofstream stats_file{"worker_status.txt", std::ios::trunc};
  while(!shutdown) {
    stats_file.seekp(0); // seek to beginning of file
    std::string output;
    std::vector<std::pair<uint64_t, WorkerStatus>> status_vec = WorkDistributor::get_status();
    for (auto status : status_vec) {
      std::string status_str = "QUEUE_WAIT";
      if (status.second == DISTRIB_PROCESSING)
        status_str = "DISTRIB_PROCESSING";
      else if (status.second == APPLY_DELTA)
        status_str = "APPLY_DELTA";
      else if (status.second == PAUSED)
        status_str = "PAUSED";

      output += "Worker Status: " + status_str + ", Number of updates processed: " 
                  + std::to_string(status.first) + "\n";
    
    }
    stats_file << output;
    stats_file.flush();
    usleep(100);
  }
}

int main(int argc, char **argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 3) {
    std::cout << "Incorrect number of arguments. "
                 "Expected two but got " << argc-1 << std::endl;
    std::cout << "Arguments are: input_stream, output_file" << std::endl;
    exit(EXIT_FAILURE);
  }
  std::string input  = argv[1];
  std::string output = argv[2];
  shutdown = false;

  BinaryGraphStream stream(input, 32 * 1024);

  node_id_t num_nodes = stream.nodes();
  long m              = stream.edges();
  long total          = m;
  GraphDistribUpdate g{num_nodes};

  auto start = std::chrono::steady_clock::now();

  std::thread querier(status_querier);

  while (m--) {
    g.update(stream.get_edge());
  }

  std::cout << "Starting CC" << std::endl;
  uint64_t num_CC = g.spanning_forest_query().size();
  
  std::chrono::duration<double> runtime = g.flush_end - start;
  std::chrono::duration<double> CC_time = g.cc_alg_end - g.cc_alg_start;

  std::ofstream out{output,  std::ofstream::out | std::ofstream::app}; // open the outfile
  std::cout << "Number of connected components is " << num_CC << std::endl;
  std::cout << "Writing runtime stats to " << output << std::endl;

  // calculate the insertion rate and write to file
  // insertion rate measured in stream updates 
  // (not in the two sketch updates we process per stream update)
  float ins_per_sec = (((float)(total)) / runtime.count());
  out << "Procesing " << total << " updates took " << runtime.count() << " seconds, " << ins_per_sec << " per second\n";

  out << "Connected Components algorithm took " << CC_time.count() << " and found " << num_CC << " CC\n";
  out.close();

  shutdown = true;
  querier.join();

  GraphDistribUpdate::teardown_cluster();
}
