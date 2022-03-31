#include <graph_distrib_update.h>
#include <binary_graph_stream.h>
#include <work_distributor.h>

#include <string>
#include <iostream>
#include "../tools/streaming/gz_specific/gz_nonsequential_streamer.h"

int main(int argc, char **argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 5) {
    std::cout << "Incorrect number of arguments. "
                 "Expected two but got " << argc-1 << std::endl;
    std::cout << "Arguments are: num_nodes, prime, num rounds to distribute, "
                 "output_file" << std::endl;
    exit(EXIT_FAILURE);
  }
  node_id_t num_nodes = atoll(argv[1]);
  node_id_t prime     = atoll(argv[2]);
  int rounds_to_distr = atoi(argv[3]);
  std::string output  = argv[4];

  double erp  = 0.5;
  int rounds  = 2;
  long seed1  = 437650290;
  long seed2  = 1268991550;
  long m      = 4e6;
  long total  = m;

  GZNonsequentialStreamer stream(num_nodes, prime, erp, rounds, seed1, seed2);
  GraphDistribUpdate g{num_nodes};

  auto start = std::chrono::steady_clock::now();

  while (m--) {
    g.update(stream.next());
  }

  std::cout << "Starting CC" << std::endl;
  g.num_rounds_to_distribute(rounds_to_distr);
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

  GraphDistribUpdate::teardown_cluster();
}
