#include <fstream>
#include <iostream>
#include <thread>

#include <graph_distrib_update.h>
#include <mat_graph_verifier.h>
#include <binary_graph_stream.h>

int main(int argc, char** argv) {
  GraphDistribUpdate::setup_cluster(argc, argv);

  if (argc != 5) {
    std::cout << "Usage: " << argv[0] << " num_queries runs inserter_threads input_file" << std::endl;
    exit(EXIT_FAILURE);
  }

  int samples = std::atoi(argv[1]);
  int runs = std::atoi(argv[2]);
  int inserter_threads = std::atoi(argv[3]);
  std::string input_file = argv[4];

  int num_failures = 0;

  for (int i = 0; i < runs; ++i) {
    BinaryGraphStream_MT stream(input_file, 32 * 1024);
    BinaryGraphStream verify_stream(input_file, 32 * 1024);
    node_id_t num_nodes = verify_stream.nodes();
    edge_id_t num_edges = verify_stream.edges();
    MatGraphVerifier verify(num_nodes);

    std::vector<std::thread> threads;
    GraphDistribUpdate g{num_nodes, inserter_threads};

    // variables for coordination between inserter_threads
    bool query_done = false;
    int num_query_ready = 0;
    std::condition_variable q_ready_cond;
    std::condition_variable q_done_cond;
    std::mutex q_lock;

    // prepare evenly spaced queries
    int num_queries = samples;
    uint64_t upd_per_query = num_edges / num_queries;
    uint64_t query_idx = upd_per_query;
    if (!stream.register_query(query_idx)) { // register first query
      std::cout << "Failed to register query" << std::endl;
      exit(EXIT_FAILURE);
    }

    // task for threads that insert to the graph and perform queries
    auto task = [&](const int thr_id) {
      MT_StreamReader reader(stream);
      GraphUpdate upd;
      while(true) {
        upd = reader.get_edge();
        if (upd.second == END_OF_FILE) return;
        else if (upd.second == NXT_QUERY) {
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

            // add updates to verifier and perform query
            for (uint64_t j = 0; j < upd_per_query; j++) {
              GraphUpdate upd = verify_stream.get_edge();
              verify.edge_update(upd.first.first, upd.first.second);
            }
            verify.reset_cc_state();
            g.set_verifier(std::make_unique<MatGraphVerifier>(verify));
            try {
              g.spanning_forest_query(true);
            } catch (std::exception& ex) {
              ++num_failures;
              std::cout << ex.what() << std::endl;
            }

            // inform other threads that we're ready to continue processing queries
            stream.post_query_resume();
            if(num_queries > 1) {
              // prepare next query
              query_idx += upd_per_query;
              if (!stream.register_query(query_idx)) { // register first query
                std::cout << "Failed to register query" << std::endl;
                exit(EXIT_FAILURE);
              }
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

    // start inserters
    for (int t = 0; t < inserter_threads; t++) {
      threads.emplace_back(task, t);
    }
    // wait for inserters to be done
    for (int t = 0; t < inserter_threads; t++) {
      threads[t].join();
    }

    // process the rest of the stream into the MatGraphVerifier
    for(size_t i = query_idx; i < num_edges; i++) {
      GraphUpdate upd = verify_stream.get_edge();
      verify.edge_update(upd.first.first, upd.first.second);
    }

    // perform final query
    std::cout << "Starting CC" << std::endl;
    verify.reset_cc_state();
    g.set_verifier(std::make_unique<MatGraphVerifier>(verify));
    try {
      g.spanning_forest_query();
    } catch (std::exception& ex) {
      ++num_failures;
      std::cout << ex.what() << std::endl;
    }
  }
  std::cout << "Did " << runs << " runs, with " << samples << " samples each. Total failures: " << num_failures << std::endl;
  GraphDistribUpdate::teardown_cluster();
}
