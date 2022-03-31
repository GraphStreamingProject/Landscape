#include <graph_distrib_update.h>
#include <binary_graph_stream.h>
#include <work_distributor.h>

#include <string>
#include <iostream>

int main(int argc, char **argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 3) {
    std::cout << "Incorrect number of arguments. "
                 "Expected three but got " << argc-1 << std::endl;
    std::cout << "Arguments are: insert_threads, input_stream, output_file" << std::endl;
    exit(EXIT_FAILURE);
  }
  int inserter_threads = std::atoi(argv[1]);
  if (inserter_threads < 1 || inserter_threads > 50) {
    std::cout << "Number of inserter threads is invalid. Require in [1, 50]" << std::endl;
    exit(EXIT_FAILURE);
  }
  std::string input  = argv[2];
  std::string output = argv[3];

  BinaryGraphStream_MT stream(input, 32 * 1024);

  node_id_t num_nodes = stream.nodes();
  long m              = stream.edges();
  long total          = m;
  GraphDistribUpdate g{num_nodes};

  std::vector<std::thread> threads;
  threads.reserve(inserter_threads);

  auto task = [&]() {
    MT_StreamReader reader(stream);
    GraphUpdate upd;
    while(true) {
      upd = reader.get_edge();
      if (upd.second == END_OF_FILE) break;
      g.update(upd);
    }
  };

  auto start = std::chrono::steady_clock::now();

  // start inserters
  for (int i = 0; i < inserter_threads; i++) {
    threads.emplace_back(task);
  }
  // wait for inserters to be done
  for (int i = 0; i < inserter_threads; i++) {
    threads[i].join();
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

  GraphDistribUpdate::teardown_cluster();
}
