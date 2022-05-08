#include <graph_distrib_update.h>
#include <work_distributor.h>

#include <string>
#include <iostream>
#include "../tools/streaming/gz_specific/gz_sequential_group.h"

void task(GraphUpdate& edge, Graph& g, const int thr_id) {
  g.update(edge, thr_id);
}

void forall_task(StreamerGroup& group, std::vector<edge_id_t>&
      updates_processed, Graph& g, const int thr_id) {
  updates_processed[thr_id] = group.forall_in_handle(thr_id, task, std::ref(g),
                                                     thr_id);
}

int main(int argc, char **argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 6) {
    std::cout << "Incorrect number of arguments. "
                 "Expected three but got " << argc-1 << std::endl;
    std::cout << "Arguments are: insert_threads, num_nodes, er_prob, rounds, "
                 "output_file" << std::endl;
    exit(EXIT_FAILURE);
  }
  int inserter_threads = std::atoi(argv[1]);
  if (inserter_threads < 1 || inserter_threads > 50) {
    std::cout << "Number of inserter threads is invalid. Require in [1, 50]" << std::endl;
    exit(EXIT_FAILURE);
  }
  double er_prob = std::atof(argv[3]);
  int rounds = std::atoi(argv[4]);
  std::string output = argv[5];
  long seed2 = 1268991550;

  node_id_t num_nodes  = std::atol(argv[2]);
  GraphDistribUpdate g{num_nodes, inserter_threads};

  std::vector<std::thread> threads;
  threads.reserve(inserter_threads);
  std::vector<edge_id_t> updates_processed(inserter_threads, 0);

  StreamerGroup group (inserter_threads, num_nodes, er_prob, rounds,
                       seed2);

  auto start = std::chrono::steady_clock::now();

  // start inserters
  for (int i = 0; i < inserter_threads; i++) {
    threads.emplace_back(forall_task, std::ref(group),
                         std::ref(updates_processed), std::ref(g), i);
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
  edge_id_t total = 0;
  for (int i = 0; i < inserter_threads; ++i) {
    std::cout << updates_processed[i] << std::endl;
  }
  for (auto upd : updates_processed) total += upd;

  double ins_per_sec = (((double)(total)) / runtime.count());
  out << "Procesing " << total << " updates took " << runtime.count() << " seconds, " << ins_per_sec << " per second\n";

  out << "Connected Components algorithm took " << CC_time.count() << " and found " << num_CC << " CC\n";
  out.close();

  GraphDistribUpdate::teardown_cluster();
}
