#include <binary_graph_stream.h>
#include <graph_distrib_update.h>
#include <math.h>
#include <work_distributor.h>

#include "simple_stream.h"

#include <atomic>
#include <iostream>
#include <string>

#include <sys/resource.h> // for rusage

static double get_max_mem_used() {
  struct rusage data;
  getrusage(RUSAGE_SELF, &data);
  return (double) data.ru_maxrss / 1024.0;
}

int main(int argc, char** argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 7) {
    std::cerr << "Incorrect number of arguments. "
                 "Expected six but got "
              << argc - 1 << std::endl;
    std::cerr << "Arguments are: insert_threads, num_forests, format" << std::endl;
    std::cerr << "Format='file' args are:  repeats, input_stream, output_file" << std::endl;
    std::cerr << "Format='erdos' args are: vertices, edges, output_file" << std::endl;
    exit(EXIT_FAILURE);
  }
  int inserter_threads = std::atoi(argv[1]);
  if (inserter_threads < 1 || inserter_threads > 50) {
    std::cerr << "Number of inserter threads is invalid. Require in [1, 50]" << std::endl;
    exit(EXIT_FAILURE);
  }

  node_id_t num_forests = std::atol(argv[2]);
  if (num_forests < 1) {
    std::cerr << "Number of Forests must be at least 1!" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string format = argv[3];
  if (format != "file" && format != "erdos") {
    std::cerr << "Format is not valid! Expect: 'file' or 'erdos'" << std::endl;
    exit(EXIT_FAILURE);
  }

  if (format == "file") {
    int repeats = std::atoi(argv[4]);
    if (repeats < 1 || repeats > 21 || repeats % 2 == 0) {
      std::cerr << "Number of repeats is invalid. Require in [1, 21] and odd" << std::endl;
      exit(EXIT_FAILURE);
    }

    std::string input = argv[5];
    std::string output = argv[6];

    BinaryGraphStream_MT stream(input, 32 * 1024);

    node_id_t num_nodes = stream.nodes();
    long m = stream.edges();
    long total = m;
    std::cout << "Processing stream" << std::endl;
    std::cout << "Vertices = " << num_nodes << std::endl;
    std::cout << "Edges    = " << m << std::endl;
    GraphDistribUpdate g{num_nodes, inserter_threads, num_forests};

    std::vector<std::thread> threads;
    threads.reserve(inserter_threads);

    auto task = [&](const int thr_id) {
      MT_StreamReader reader(stream);
      GraphUpdate upd;
      while (true) {
        upd = reader.get_edge();
        if (upd.type == BREAKPOINT) break;
        g.update(upd, thr_id);
      }
    };

    auto start = std::chrono::steady_clock::now();

    for (int r = 0; r < repeats; r++) {
      // start inserters
      for (int t = 0; t < inserter_threads; t++) {
        threads.emplace_back(task, t);
      }
      // wait for inserters to be done
      for (int t = 0; t < inserter_threads; t++) {
        threads[t].join();
      }
      stream.stream_reset();
      threads.clear();
    }
    std::cout << "Starting CC" << std::endl;
    std::vector<std::set<node_id_t>> sf_adj = g.k_spanning_forests(num_forests);

    std::chrono::duration<double> runtime = g.flush_end - start;
    std::chrono::duration<double> CC_time = g.cc_alg_end - g.cc_alg_start;

    // calculate the number of edges in the spanning forest
    size_t edges = 0;
    for (node_id_t src = 0; src < num_nodes; src++) {
      edges += sf_adj[src].size();
    }
    std::cout << "number of spanning forest edges: " << edges << std::endl;

    // calculate the insertion rate and print
    // insertion rate measured in stream updates
    // (not in the two sketch updates we process per stream update)
    float ins_per_sec = (((float)(total * repeats)) / runtime.count());

    std::cout << "Processing " << total * repeats << " updates took " << runtime.count()
              << " seconds, " << ins_per_sec << " per second\n";

    std::cout << "Finding " << num_forests << " Spanning Forests took " << CC_time.count()
              << " and found " << edges << " edges\n";

    std::ofstream out{output, std::ofstream::out | std::ofstream::app};  // open the outfile
    std::cout << "Writing runtime stats to " << output << std::endl;

    out << ins_per_sec << ", " << CC_time.count() << ", " << get_max_mem_used();
    out.close();
  } else {
    node_id_t num_nodes = std::stoull(argv[4]);
    edge_id_t num_edges = std::stoull(argv[5]);

    std::cout << "Running a SimpleStream with" << std::endl;
    std::cout << "Vertices = " << num_nodes << std::endl;
    std::cout << "Edges    = " << num_edges << std::endl;

    std::string output = argv[6];

    SimpleStream stream(time(nullptr), num_nodes);
    stream.set_break_point(num_edges);
    GraphDistribUpdate g{num_nodes, inserter_threads, num_forests};

    std::vector<std::thread> threads;
    threads.reserve(inserter_threads);

    auto task = [&](const int thr_id) {
      GraphStreamUpdate upds[256];
      bool running = true;
      while (running) {
        size_t num_upds = stream.get_update_buffer(upds, 256);
        for (size_t i = 0; i < num_upds; i++) {
          GraphStreamUpdate upd = upds[i];
          if (upd.type == BREAKPOINT) {
            running = false;
            break;
          }
          GraphUpdate g_upd;
          g_upd.edge = upd.edge;
          g_upd.type = static_cast<UpdateType>(upd.type);
          g.update(g_upd, thr_id);
        }
      }
    };

    auto start = std::chrono::steady_clock::now();

    // start inserters
    for (int t = 0; t < inserter_threads; t++) {
      threads.emplace_back(task, t);
    }
    // wait for inserters to be done
    for (int t = 0; t < inserter_threads; t++) {
      threads[t].join();
    }
    threads.clear();

    std::cout << "Starting CC" << std::endl;
    std::vector<std::set<node_id_t>> sf_adj = g.k_spanning_forests(num_forests);

    std::chrono::duration<double> runtime = g.flush_end - start;
    std::chrono::duration<double> CC_time = g.cc_alg_end - g.cc_alg_start;

    // calculate the number of edges in the spanning forest
    size_t edges = 0;
    for (node_id_t src = 0; src < num_nodes; src++) {
      edges += sf_adj[src].size();
    }
    std::cout << "number of spanning forest edges: " << edges << std::endl;
    float ins_per_sec = (((float)(num_edges)) / runtime.count());

    std::cout << "Processing " << num_edges << " updates took " << runtime.count()
              << " seconds, " << ins_per_sec << " per second\n";

    std::cout << "Finding " << num_forests << " Spanning Forests took " << CC_time.count()
              << " and found " << edges << " edges\n";

    std::ofstream out{output, std::ofstream::out | std::ofstream::app};  // open the outfile
    std::cout << "Writing runtime stats to " << output << std::endl;

    out << ins_per_sec << ", " << CC_time.count() << ", " << get_max_mem_used();
    out.close();
  }

  GraphDistribUpdate::teardown_cluster();
}
