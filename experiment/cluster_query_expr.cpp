#include <graph_distrib_update.h>
#include <binary_graph_stream.h>
#include <work_distributor.h>

#include <string>
#include <iostream>

int main(int argc, char **argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 5) {
    std::cout << "Incorrect number of arguments. "
                 "Expected four but got " << argc-1 << std::endl;
    std::cout << "Arguments are: insert_threads, num_queries, input_stream, output_file" << std::endl;
    return EXIT_FAILURE;
  }

  int inserter_threads = std::atoi(argv[1]);
  if (inserter_threads < 1 || inserter_threads > 50) {
    std::cout << "Number of inserter threads is invalid. Require in [1, 50]" << std::endl;
    exit(EXIT_FAILURE);
  }

  int num_queries = std::atoi(argv[2]);
  if (num_queries < 0 || num_queries > 10000) {
    std::cout << "Number of num_queries is invalid. Require in [0, 10000]" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string input  = argv[3];
  std::string output = argv[4];

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
  int upd_per_query;
  int query_idx;
  if (num_queries > 0) {
    upd_per_query = num_updates / num_queries;
    query_idx     = upd_per_query;
    stream.register_query(query_idx); // register first query
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
          std::cout << "QUERY DONE! Found " << num_CC << " connected components" << std::endl;
          cc_status_out << "Query completed, number of CCs: " << num_CC << std::endl;
          cc_status_out << "Total query latency = " << std::chrono::duration<double>(g.cc_alg_end - cc_start).count() << std::endl;
          cc_status_out << "Flush latency       = " << std::chrono::duration<double>(g.flush_end - g.flush_start).count() << std::endl;
          cc_status_out << "CC alg latency      = " << std::chrono::duration<double>(g.cc_alg_end - g.cc_alg_start).count() << std::endl;

          // inform other threads that we're ready to continue processing queries
          stream.post_query_resume();
          if(num_queries > 1) {
            // prepare next query
            query_idx += upd_per_query;
            if(!stream.register_query(query_idx))
              std::cout << "Failed to register query at index " << query_idx << std::endl;
            num_queries--;
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
