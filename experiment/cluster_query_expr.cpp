#include <graph_distrib_update.h>
#include <binary_graph_stream.h>
#include <work_distributor.h>

#include <string>
#include <iostream>
#include <unordered_map>
#include <random>

int main(int argc, char **argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);
  {
    int inserter_threads;
    int num_queries;
    std::string input;
    std::string output;
    bool bursts = false;
    int repeats = 1;
    int num_grouped = 1;
    int ins_btwn_qrys = 0;
    bool point_queries = false;
    std::vector<std::function<bool(char*)>> parse;
    std::unordered_map<std::string, std::function<void(void)>> long_options;

    size_t queries_done = 0;

    const auto arg_insert_threads = [&](char* arg) -> bool {
      inserter_threads = std::atoi(arg);
      if (inserter_threads < 1 || inserter_threads > 50) {
        std::cout << "Number of inserter threads is invalid. Require in [1, 50]" << std::endl;
        return false;
      }
      return true;
    };

    const auto arg_num_queries = [&](char* arg) -> bool {
      num_queries = std::atoi(arg);
      if (num_queries < 0 || num_queries > 10000) {
        std::cout << "Number of num_queries is invalid. Require in [0, 10000]" << std::endl;
        return false;
      }
      return true;
    };

    const auto arg_input = [&](char* arg) -> bool {
      input = arg;
      return true;
    };

    const auto arg_output = [&](char* arg) -> bool {
      output = arg;
      return true;
    };

    const auto arg_repeats = [&](char* arg) -> bool {
      repeats = std::atoi(arg);
      if (repeats < 1 || repeats > 50) {
        std::cout << "Number of repeats is invalid. Require in [1, 50]" << std::endl;
        return false;
      }
      return true;
    };

    const auto opt_repeats = [&]() {
      parse.push_back(arg_repeats);
    };

    const auto arg_num_grouped = [&](char* arg) -> bool {
      num_grouped = std::atoi(arg);
      if (num_grouped < 1) {
        std::cout << "Invalid num_grouped in burst." << std::endl;
        return false;
      }
      return true;
    };

    const auto arg_ins_btwn_qrys = [&](char* arg) -> bool {
      ins_btwn_qrys = std::atoi(arg);
      if (ins_btwn_qrys < 1 || ins_btwn_qrys >= 1000000) {
        std::cout << "Invalid ins_btwn_qrys in burst. Must be > 0 and <= 1,000,000." << std::endl;
        return false;
      }
      return true;
    };

    const auto opt_burst = [&]() {
      bursts = true;
      parse.push_back(arg_ins_btwn_qrys);
      parse.push_back(arg_num_grouped);
    };

    const auto opt_point = [&]() {
      point_queries = true;
    };

    parse.push_back(arg_output);
    parse.push_back(arg_input);
    parse.push_back(arg_num_queries);
    parse.push_back(arg_insert_threads);

    long_options["point"] = opt_point;
    long_options["repeat"] = opt_repeats;
    long_options["burst"] = opt_burst;

    const auto print_usage = [&]() {
      std::cout << "Arguments are: insert_threads, num_queries, input_stream, output_file, ";
      std::cout << "[--point], [--repeat <num_repeats>], [--burst <num_grouped> <ins_btwn_qry>]" << std::endl;
      std::cout << "insert_threads:  number of threads inserting to guttering system" << std::endl;
      std::cout << "num_queries:     number of queries to issue during the stream." << std::endl;
      std::cout << "input_stream:    the binary stream to ingest." << std::endl;
      std::cout << "output_file:     where to place the experiment results." << std::endl;
      std::cout << "--point: [OPTIONAL] if present then use point queries instead of spanning forest queries" << std::endl;
      std::cout << "--repeat <num_repeats>: [OPTIONAL] if present then repeat stream" << std::endl;
      std::cout << "  num_repeats:   number of times to repeat the stream. Must be odd." << std::endl;
      std::cout << "--burst <num_grouped> <ins_btwn_qry>: [OPTIONAL] if present then queries should be bursty" << std::endl;
      std::cout << "  num_grouped:   specifies how many queries should be grouped together" << std::endl;
      std::cout << "  ins_btwn_qry:  specifies the number of insertions to perform between each query" << std::endl;
    };

    for (int i = 1; i < argc; ++i) {
      char* arg = argv[i];
      if (arg[0] == '-') {
        if (arg[1] == '-') {
          // GNU-style long options
          std::string option = arg + 2;
          auto it = long_options.find(option);
          if (it == long_options.end()) {
            std::cout << "Invalid option --" << option << std::endl;
            print_usage();
            exit(EXIT_FAILURE);
          }
          it->second();
        } else {
          // If we need unix-style options, technically could be implemented here
        }
      } else {
        if (parse.empty()) {
          std::cout << "Too many arguments." << std::endl;
          print_usage();
          exit(EXIT_FAILURE);
        }
        if (!parse.back()(arg)) {
          exit(EXIT_FAILURE);
        }
        parse.pop_back();
      }
    }

    if (!parse.empty()) {
      std::cout << "Too few arguments." << std::endl;
      print_usage();
      return EXIT_FAILURE;
    }

    BinaryGraphStream_MT stream(input, 32 * 1024);

    node_id_t num_nodes   = stream.nodes();
    edge_id_t num_updates = stream.edges();
    
    if (repeats > 1)
      std::cout << "REPEATING the stream " << repeats << " times." << std::endl;

    std::cout << "Vertices = " << num_nodes << std::endl;
    std::cout << "Edges    = " << num_updates << std::endl;

    GraphDistribUpdate g{num_nodes, inserter_threads};

    std::vector<std::thread> threads;
    threads.reserve(inserter_threads);

    // variables for coordination between inserter_threads
    bool query_done = false;
    int num_query_ready = 0;
    std::condition_variable q_ready_cond;
    std::condition_variable q_done_cond;
    std::mutex q_lock;

    // prepare evenly spaced queries
    size_t upd_per_query;
    size_t num_bursts = bursts? (num_queries - 1) / num_grouped + 1 : num_queries;
    size_t group_left = num_grouped;
    size_t query_idx;
    std::atomic<size_t> repeated;
    std::atomic<size_t> stream_reset_query;
    repeated = 0;
    stream_reset_query = 0;

    if (num_bursts > 0) {
      if (num_updates / num_bursts < (size_t) ins_btwn_qrys * (num_grouped - 1)) {
        std::cout << "Too many bursts or too many insertion between queries, "
           "updates between bursts is not positive." << std::endl;
        exit(EXIT_FAILURE);
      }
      upd_per_query = num_updates * (repeats / 2 + 1) / num_bursts - ins_btwn_qrys * (num_grouped - 1);
      query_idx     = upd_per_query;
      stream.register_query(query_idx); // register first query
      if (bursts) {
        std::cout << "Total number of updates = " << num_updates * repeats
            << " perfoming " << num_queries
            << " queries in bursts with " << ins_btwn_qrys
            << " updates between queries within a burst, " << num_grouped
            << " queries in a burst, and " << upd_per_query
            << " updates between bursts" << std::endl;
      } else {
        std::cout << "Total number of updates = " << num_updates * repeats << " perfoming " 
            << num_queries << " queries: one every " << upd_per_query << std::endl;
      }
    }
    std::ofstream cc_status_out{output, std::ios_base::out | std::ios_base::app};

    std::atomic<bool> next_stream_repeat;
    next_stream_repeat = false;

    auto seed = std::random_device()();
    // task for threads that insert to the graph and perform queries
    auto task = [&](const int thr_id) {
      std::default_random_engine rand_engine(seed + thr_id);
      std::uniform_int_distribution<node_id_t> rand_node(0, num_nodes - 1);
      MT_StreamReader reader(stream);
      GraphUpdate upd;
      while(true) {
        upd = reader.get_edge();
        if (upd.type == BREAKPOINT && num_queries == 0) return;
        else if (upd.type == BREAKPOINT) { // do a query
          auto cc_start = std::chrono::steady_clock::now();
          if (next_stream_repeat)
            return; // this breakpoint is a stream repeat

          query_done = false;
          if (thr_id > 0) {
            // pause this thread and wait for query to be done
            std::unique_lock<std::mutex> lk(q_lock);
            num_query_ready++;
            lk.unlock();
            q_ready_cond.notify_one();

            // wait for query to finish
            lk.lock();
            q_done_cond.wait(lk, [&](){return query_done;});
            num_query_ready--;
            lk.unlock();
          } else {
            // this thread will actually perform the query
            // wait for other threads to be done applying updates
            std::unique_lock<std::mutex> lk(q_lock);
            num_query_ready++;
            q_ready_cond.wait(lk, [&](){
              return num_query_ready >= inserter_threads;
            });

            // perform query
            if (point_queries) {
              node_id_t a = rand_node(rand_engine);
              node_id_t b = rand_node(rand_engine);
              bool connected = g.point_to_point_query(a, b);
              std::cout << "QUERY DONE at index " << query_idx << ", " << a << " and " << b 
                << " connected: " << (connected? "true" : "false") << std::endl;
              std::chrono::duration<double>flush(g.flush_end - g.flush_start);

              // do 99 more random point queries
              auto cc_temp = g.cc_alg_start;
              int x = 0;
              for(int i = 0; i < 99; i++) {
                      a = rand_node(rand_engine);
                      b = rand_node(rand_engine);
                      connected = g.point_to_point_query(a, b);
                x += connected;
              }

              std::cout << x << std::endl;

              std::chrono::duration<double> q_latency = g.cc_alg_end - cc_start;
              std::chrono::duration<double> flush_latency = g.flush_end - g.flush_start;
              std::chrono::duration<double> alg_latency = g.cc_alg_end - g.cc_alg_start;

              std::cout << "Query completed, " << a << " and " << b << " connected: " << connected << std::endl;
              std::cout << "Total query latency = " << q_latency.count() << std::endl;
              std::cout << "Flush latency       = " << flush_latency.count() << std::endl;
              std::cout << "CC alg latency      = " << alg_latency.count() << std::endl;
              cc_status_out << queries_done / num_bursts << ", " << flush_latency.count() << ", " << alg_latency.count() << ", P2P" << std::endl;

            } else {
              size_t num_CC = g.get_connected_components(true).size();
              std::cout << "QUERY DONE at index " << query_idx << " Found " << num_CC << " connected components" << std::endl;

              std::chrono::duration<double> q_latency = g.cc_alg_end - cc_start;
              std::chrono::duration<double> flush_latency = g.flush_end - g.flush_start;
              std::chrono::duration<double> alg_latency = g.cc_alg_end - g.cc_alg_start;

              std::cout << "Query completed, number of CCs: " << num_CC << std::endl;
              std::cout << "Total query latency = " << q_latency.count() << std::endl;
              std::cout << "Flush latency       = " << flush_latency.count() << std::endl;
              std::cout << "CC alg latency      = " << alg_latency.count() << std::endl;
              cc_status_out << queries_done / num_bursts << ", " << flush_latency.count() << ", " << alg_latency.count() << ", GLOBAL" << std::endl;
            }

            // inform other threads that we're ready to continue processing queries
            stream.post_query_resume();
            if(num_queries > 1) {
              // prepare next query
              if (--group_left > 0) {
                query_idx += ins_btwn_qrys;
              } else {
                query_idx += upd_per_query;
                group_left = num_grouped;
              }
              if (query_idx < num_updates * (repeated/2 + 1)) { // then do register as normal
                if(!stream.register_query((query_idx % num_updates) == 0 ? 1 : query_idx % num_updates )) {
                  std::cout << "Failed to register query at index " << query_idx << std::endl;
                  exit(EXIT_FAILURE);
                }
              } else { // special case because we will hit end of stream first
                next_stream_repeat = true;
                stream_reset_query = query_idx % num_updates;
              }
              std::cout << "Registered next query at " << query_idx << " -> " << query_idx % num_updates<< std::endl;
            }
            queries_done++;
            num_queries--;
            std::cout << "Queries Remaining = " << num_queries << std::endl;
            num_query_ready--;
            query_done = true;
            lk.unlock();
            q_done_cond.notify_all();
          }
        }
        else if (upd.type == INSERT || upd.type == DELETE)
          g.update(upd, thr_id);
        else
          throw std::invalid_argument("Did not recognize edge code!");
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
      repeated++;
      threads.clear();
      if (repeats - r > 1) {
        std::cout << "REPEATING the stream!" << std::endl;
        if(repeated % 2 == 0) {
          std::cout << "Querying for this repeat" << std::endl;
          next_stream_repeat = false;
          if(!stream.register_query(stream_reset_query == 0 ? 1 : stream_reset_query.load())) {
            std::cout << "Failed to register query, when resetting stream, at index " << query_idx << std::endl;
            exit(EXIT_FAILURE);
          }
        } else next_stream_repeat = true;
      }
    }

    // perform final query
    auto cc_start = std::chrono::steady_clock::now();
    size_t num_CC;
    std::cout << "Starting CC" << std::endl;
    num_CC = g.get_connected_components().size();
    std::cout << "Number of connected components is " << num_CC << std::endl;

    std::chrono::duration<double> runtime = g.flush_end - start;
    std::chrono::duration<double> CC_time = g.cc_alg_end - g.cc_alg_start;

    std::cout << "Writing runtime stats to " << output << std::endl;

    // calculate the insertion rate and write to file
    // insertion rate measured in stream updates 
    // (not in the two sketch updates we process per stream update)
    double ins_per_sec = (((double)num_updates * repeats) / runtime.count());
    cc_status_out << "Procesing " << num_updates * repeats << " updates took ";
    cc_status_out << runtime.count() << " seconds, " << ins_per_sec << " per second\n";

    cc_status_out << "Final query completed! Number of CCs: " << num_CC << std::endl;
    cc_status_out << "Total query latency = " << std::chrono::duration<double>(g.cc_alg_end - cc_start).count() << std::endl;
    cc_status_out << "Flush latency       = " << std::chrono::duration<double>(g.flush_end - g.flush_start).count() << std::endl;
    cc_status_out << "CC alg latency      = " << std::chrono::duration<double>(g.cc_alg_end - g.cc_alg_start).count() << std::endl;

    cc_status_out.close();
  }

  GraphDistribUpdate::teardown_cluster();
}
