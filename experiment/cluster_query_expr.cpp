#include <graph_distrib_update.h>
#include <binary_graph_stream.h>
#include <work_distributor.h>

#include <string>
#include <iostream>

int main(int argc, char **argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 6 && argc != 9) {
    std::cout << "Incorrect number of arguments. "
                 "Expected five (or optionally eight) but got " << argc-1 << std::endl;
    std::cout << "Arguments are: insert_threads, num_repeats, num_queries";
    std::cout << ", input_stream, output_file, [--burst <num_grouped> <ins_btwn_qry>]" << std::endl;
    std::cout << "insert_threads:  number of threads inserting to guttering system" << std::endl;
    std::cout << "num_repeats:     number of times to repeat the stream. Must be odd." << std::endl;
    std::cout << "num_queries:     number of queries to issue during the stream." << std::endl;
    std::cout << "input_stream:    the binary stream to ingest." << std::endl;
    std::cout << "output_file:     where to place the experiment results." << std::endl;
    std::cout << "--burst <num_grouped> <ins_btwn_qry>: [OPTIONAL] if present then queries should be bursty" << std::endl;
    std::cout << "  num_grouped:   specifies how many queries should be grouped together" << std::endl;
    std::cout << "  ins_btwn_qry:  specifies the number of insertions to perform between each query" << std::endl;

    return EXIT_FAILURE;
  }

  int inserter_threads = std::atoi(argv[1]);
  if (inserter_threads < 1 || inserter_threads > 50) {
    std::cout << "Number of inserter threads is invalid. Require in [1, 50]" << std::endl;
    exit(EXIT_FAILURE);
  }

  // Still a work in progress -- right now doesn't do anything
  int repeats = std::atoi(argv[2]);
  if (repeats < 1 || repeats > 50) {
    std::cout << "Number of repeats is invalid. Require in [1, 50]" << std::endl;
    exit(EXIT_FAILURE);
  }

  int num_queries = std::atoi(argv[3]);
  if (num_queries < 0 || num_queries > 10000) {
    std::cout << "Number of num_queries is invalid. Require in [0, 10000]" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string input  = argv[4];
  std::string output = argv[5];

  bool bursts = false;
  int num_grouped   = 1;
  int ins_btwn_qrys = 0;

  if (argc > 6) {
    // specifying query bursts
    if (std::string(argv[6]) != "--burst") {
      std::cout << argv[6] << "is invalid. Must match '--burst'" << std::endl;
      exit(EXIT_FAILURE); 
    }
    bool bursts = true;
    num_grouped   = std::atoi(argv[7]);
    if (num_grouped < 1 || num_grouped > num_queries) {
      std::cout << "Invalid num_grouped in burst. Must be > 0 and < num_queries." << std::endl;
      exit(EXIT_FAILURE);
    }
    ins_btwn_qrys = std::atoi(argv[8]);
    if (ins_btwn_qrys < 1 || ins_btwn_qrys > 1000000) {
      std::cout << "Invalid ins_btwn_qrys in burst. Must be > 0 and < 1,000,000." << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  // TODO: Actually implement bursty queries!
  // TODO: Issue calls to point to point connectivity queries! (requires that to be implemented in rest of code)
  // TODO: Command line argument for said binary queries

  BinaryGraphStream_MT stream(input, 32 * 1024);

  node_id_t num_nodes   = stream.nodes();
  edge_id_t num_updates = stream.edges();
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
  size_t num_bursts = (num_queries - 1) / num_grouped + 1;
  size_t group_left = num_grouped;
  size_t query_idx;
  if (num_bursts > 0) {
    if (num_updates / num_bursts < ins_btwn_qrys * (num_grouped - 1)) {
      std::cout << "Too many bursts or too many insertion between queries, "
         "updates between bursts is not positive." << std::endl;
      exit(EXIT_FAILURE);
    }
    upd_per_query = num_updates / num_bursts - ins_btwn_qrys * (num_grouped - 1);
    query_idx     = upd_per_query;
    stream.register_query(query_idx); // register first query
    if (num_bursts > 1) {
      std::cout << "Total number of updates = " << num_updates
          << " perfoming queries in bursts with " << ins_btwn_qrys
          << " updates between queries within a burst, " << num_grouped
          << " queries in a burst, and " << upd_per_query
          << " updates between bursts" << std::endl;
    } else {
      std::cout << "Total number of updates = " << num_updates << " perfoming queries every " << upd_per_query << std::endl;
    }
  }
  std::ofstream cc_status_out{output};

  // task for threads that insert to the graph and perform queries
  auto task = [&](const int thr_id) {
    MT_StreamReader reader(stream);
    GraphUpdate upd;
    while(true) {
      upd = reader.get_edge();
      if (upd.second == END_OF_FILE) return;
      else if (upd.second == NXT_QUERY) {
        auto cc_start = std::chrono::steady_clock::now();
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
          size_t num_CC = g.spanning_forest_query(true).size();
          std::cout << "QUERY DONE at index " << query_idx << " Found " << num_CC << " connected components" << std::endl;
          cc_status_out << "Query completed, number of CCs: " << num_CC << std::endl;
          cc_status_out << "Total query latency = " << std::chrono::duration<double>(g.cc_alg_end - cc_start).count() << std::endl;
          cc_status_out << "Flush latency       = " << std::chrono::duration<double>(g.flush_end - g.flush_start).count() << std::endl;
          cc_status_out << "CC alg latency      = " << std::chrono::duration<double>(g.cc_alg_end - g.cc_alg_start).count() << std::endl;

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
            if(!stream.register_query(query_idx))
              std::cout << "Failed to register query at index " << query_idx << std::endl;
            num_queries--;
            std::cout << "Registered next query at " << query_idx << std::endl;
          }
          num_query_ready--;
          query_done = true;
          lk.unlock();
          q_done_cond.notify_all();
        }
      }
      else if (upd.second == INSERT || upd.second == DELETE)
        g.update(upd, thr_id);
      else
        throw std::invalid_argument("Did not recognize edge code!");
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

  // perform final query
  std::cout << "Starting CC" << std::endl;
  auto cc_start = std::chrono::steady_clock::now();
  size_t num_CC = g.spanning_forest_query().size();

  std::chrono::duration<double> runtime = g.flush_end - start;
  std::chrono::duration<double> CC_time = g.cc_alg_end - g.cc_alg_start;

  std::cout << "Number of connected components is " << num_CC << std::endl;
  std::cout << "Writing runtime stats to " << output << std::endl;

  // calculate the insertion rate and write to file
  // insertion rate measured in stream updates 
  // (not in the two sketch updates we process per stream update)
  double ins_per_sec = (((double)num_updates) / runtime.count());
  cc_status_out << "Procesing " << num_updates << " updates took ";
  cc_status_out << runtime.count() << " seconds, " << ins_per_sec << " per second\n";

  cc_status_out << "Final query completed! Number of CCs: " << num_CC << std::endl;
  cc_status_out << "Total query latency = " << std::chrono::duration<double>(g.cc_alg_end - cc_start).count() << std::endl;
  cc_status_out << "Flush latency       = " << std::chrono::duration<double>(g.flush_end - g.flush_start).count() << std::endl;
  cc_status_out << "CC alg latency      = " << std::chrono::duration<double>(g.cc_alg_end - g.cc_alg_start).count() << std::endl;

  GraphDistribUpdate::teardown_cluster();
}
