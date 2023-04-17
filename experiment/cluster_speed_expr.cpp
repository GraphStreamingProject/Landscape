#include <binary_graph_stream.h>
#include <graph_distrib_update.h>
#include <math.h>
#include <work_distributor.h>

#include <atomic>
#include <iostream>
#include <string>

#pragma pack(push,1)
struct GraphStreamUpdate {
  uint8_t type;
  Edge edge;
};
#pragma pack(pop)

static constexpr edge_id_t END_OF_STREAM = (edge_id_t) -1;

class SimpleStream {
 public:
  SimpleStream(size_t seed, node_id_t nodes, node_id_t offset = 0) : seed(seed) {
    node_id_t temp_verts = 1 << (size_t)ceil(log2(nodes));  // round up to nearest power of 2
    if (temp_verts != nodes) {
      std::cerr << "WARNING: Rounding up number of vertices: " << nodes << " to nearest power of 2"
                << std::endl;
    }
    num_vertices = temp_verts;
    vertices_mask = num_vertices - 1;
    upd_idx = 0;
    breakpoint_idx = END_OF_STREAM;
    total_edges = num_vertices * (num_vertices - 1) / 2;
    vertex_offset = offset;
  }

  inline size_t get_update_buffer(GraphStreamUpdate* upd_buf, size_t num_updates) {
    edge_id_t local_idx = upd_idx.fetch_add(num_updates, std::memory_order_relaxed);
    size_t ret = 0;
    if (local_idx + num_updates > breakpoint_idx) {
      upd_idx = breakpoint_idx.load();
      num_updates = local_idx > breakpoint_idx ? 0 : breakpoint_idx - local_idx;
      upd_buf[num_updates] = {BREAKPOINT, {0, 0}};
      ret++;
    }

    // populate the update buffer
    for (size_t i = 0; i < num_updates; i++) upd_buf[i] = create_update(local_idx++);
    return ret + num_updates;
  }

  inline bool get_update_is_thread_safe() { return true; }

  inline bool set_break_point(edge_id_t break_idx) {
    if (break_idx < upd_idx) return false;
    breakpoint_idx = break_idx;
    return true;
  }

 private:
  // some basic stream parameters
  const size_t seed;
  node_id_t num_vertices;
  node_id_t vertices_mask;
  node_id_t total_edges;
  node_id_t vertex_offset;
  static constexpr col_hash_t node_id_bits = sizeof(node_id_t) * 8;
  static constexpr col_hash_t node_id_mask = (1ull << node_id_bits) - 1;

  inline node_id_t get_dst_mask(node_id_t first_dst) {
    return ~(node_id_t(-1) << (node_id_bits - __builtin_clzl(num_vertices - first_dst) - 1));
  }

  // current state of the stream
  std::atomic<edge_id_t> upd_idx;
  std::atomic<edge_id_t> breakpoint_idx;

  // Helper functions
  inline GraphStreamUpdate create_update(edge_id_t idx) {
    node_id_t src = get_random_src(idx);
    node_id_t mask = get_dst_mask(src + 1);
    node_id_t dst = src + 1 + (mask & col_hash(&src, sizeof(src), idx));

    return {INSERT, {src + vertex_offset, dst + vertex_offset}};
  }

  inline node_id_t get_random_src(edge_id_t update_idx) {
    col_hash_t hash = col_hash(&update_idx, sizeof(update_idx), seed);
    node_id_t even_hash = (hash >> node_id_bits) & (vertices_mask - 1);
    node_id_t odd_hash = ((hash & node_id_mask) & vertices_mask) | 1;
    return std::min(even_hash, odd_hash);
  }
};

int main(int argc, char** argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 6) {
    std::cerr << "Incorrect number of arguments. "
                 "Expected five but got "
              << argc - 1 << std::endl;
    std::cerr << "Arguments are: insert_threads, format" << std::endl;
    std::cerr << "Format='file' args are:  repeats, input_stream, output_file" << std::endl;
    std::cerr << "Format='erdos' args are: vertices, edges, output_file" << std::endl;
    exit(EXIT_FAILURE);
  }
  int inserter_threads = std::atoi(argv[1]);
  if (inserter_threads < 1 || inserter_threads > 50) {
    std::cerr << "Number of inserter threads is invalid. Require in [1, 50]" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string format = argv[2];
  if (format != "file" && format != "erdos") {
    std::cerr << "Format is not valid! Expect: 'file' or 'erdos'" << std::endl;
    exit(EXIT_FAILURE);
  }

  if (format == "file") {
    int repeats = std::atoi(argv[3]);
    if (repeats < 1 || repeats > 21 || repeats % 2 == 0) {
      std::cerr << "Number of repeats is invalid. Require in [1, 21] and odd" << std::endl;
      exit(EXIT_FAILURE);
    }

    std::string input = argv[4];
    std::string output = argv[5];

    BinaryGraphStream_MT stream(input, 32 * 1024);

    node_id_t num_nodes = stream.nodes();
    long m = stream.edges();
    long total = m;
    std::cout << "Processing stream" << std::endl;
    std::cout << "Vertices = " << num_nodes << std::endl;
    std::cout << "Edges    = " << m << std::endl;
    GraphDistribUpdate g{num_nodes, inserter_threads};

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
    uint64_t num_CC = g.spanning_forest_query().size();

    std::chrono::duration<double> runtime = g.flush_end - start;
    std::chrono::duration<double> CC_time = g.cc_alg_end - g.cc_alg_start;

    std::ofstream out{output, std::ofstream::out | std::ofstream::app};  // open the outfile
    std::cout << "Number of connected components is " << num_CC << std::endl;
    std::cout << "Writing runtime stats to " << output << std::endl;

    // calculate the insertion rate and write to file
    // insertion rate measured in stream updates
    // (not in the two sketch updates we process per stream update)
    float ins_per_sec = (((float)(total * repeats)) / runtime.count());
    out << "Procesing " << total * repeats << " updates took " << runtime.count() << " seconds, "
        << ins_per_sec << " per second\n";

    out << "Connected Components algorithm took " << CC_time.count() << " and found " << num_CC
        << " CC\n";
    out.close();
  } else {
    node_id_t num_vertices = std::stoull(argv[3]);
    edge_id_t num_edges = std::stoull(argv[4]);

    std::cout << "Running a SimpleStream with" << std::endl;
    std::cout << "Vertices = " << num_vertices << std::endl;
    std::cout << "Edges    = " << num_edges << std::endl;

    std::string output = argv[5];

    SimpleStream stream(time(nullptr), num_vertices);
    stream.set_break_point(num_edges);
    GraphDistribUpdate g{num_vertices, inserter_threads};

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
    uint64_t num_CC = g.spanning_forest_query().size();

    std::chrono::duration<double> runtime = g.flush_end - start;
    std::chrono::duration<double> CC_time = g.cc_alg_end - g.cc_alg_start;

    std::ofstream out{output, std::ofstream::out | std::ofstream::app};  // open the outfile
    std::cout << "Number of connected components is " << num_CC << std::endl;
    std::cout << "Writing runtime stats to " << output << std::endl;

    // calculate the insertion rate and write to file
    // insertion rate measured in stream updates
    // (not in the two sketch updates we process per stream update)
    float ins_per_sec = (((float)(num_edges)) / runtime.count());
    out << "Procesing " << num_edges << " updates took " << runtime.count() << " seconds, "
        << ins_per_sec << " per second\n";

    out << "Connected Components algorithm took " << CC_time.count() << " and found " << num_CC
        << " CC\n";
    out.close();
  }

  GraphDistribUpdate::teardown_cluster();
}
