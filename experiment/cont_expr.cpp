#include <fstream>
#include <unordered_map>
#include <iostream>

#include <graph_distrib_update.h>
#include <mat_graph_verifier.h>
#include <binary_graph_stream.h>

unsigned test_continuous(std::string input_file, unsigned samples) {
  // create input stream
  BinaryGraphStream stream(input_file, 32 * 1024);

  node_id_t n = stream.nodes();
  uint64_t  m = stream.edges();

  GraphDistribUpdate g{n, 1};
  MatGraphVerifier verify(n);

  size_t total_edges = static_cast<size_t>(n - 1) * n / 2;
  node_id_t updates_per_sample = m / samples;
  std::vector<bool> adj(total_edges);
  unsigned long num_failures = 0;

  for (unsigned long i = 0; i < samples; i++) {
    std::cout << "Starting updates" << std::endl;
    for (unsigned long j = 0; j < updates_per_sample; j++) {
      GraphUpdate upd = stream.get_edge();
      g.update(upd);
      verify.edge_update(upd.edge.src, upd.edge.dst);
    }
    verify.reset_cc_state();
    g.set_verifier(std::make_unique<MatGraphVerifier>(verify));
    std::cout << "Running cc" << std::endl;
    try {
      g.spanning_forest_query(true);
    } catch (std::exception& ex) {
      ++num_failures;
      std::cout << ex.what() << std::endl;
    }
  }
  std::clog << "Sampled " << samples << " times with " << num_failures
      << " failures" << std::endl;
  return num_failures;
}

int main(int argc, char** argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 4) {
    std::cout << "Incorrect number of arguments. "
                 "Expected three but got " << argc-1 << std::endl;
    std::cout << "Arguments are: input_stream, samples, runs" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string input_file = argv[1];
  unsigned samples = std::stoi(argv[2]);
  unsigned runs = std::stoi(argv[3]);

  unsigned tot_failures = 0;
  for (unsigned i = 0; i < runs; i++) {
    std::cout << "run: " << i << std::endl;
    tot_failures += test_continuous(input_file, samples);
  }
  std::cout << "Did " << runs << " runs, with " << samples
      << " sample each. Total failures: " << tot_failures << std::endl;
  GraphDistribUpdate::teardown_cluster();
}
