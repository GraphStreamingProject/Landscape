#include "benchmark/benchmark.h"

//#include "../tools/streaming/hash_streamer.h"
//#include "../tools/streaming/gz_specific/gz_nonsequential_streamer.h"
#include "gz_specific/gz_sequential_streamer.h"

constexpr uint64_t KB   = 1024;
constexpr uint64_t MB   = KB * KB;
const node_id_t num_nodes =   1234;
const edge_id_t num_updates = 3804488;
const edge_id_t prime =       1237;
const double    er_prob =     0.4;
const int       rounds =      6;
const long      seed1 =       437650290;
const long      seed2 =       1268991550;

// Test the speed of reading all the data in the kron16 graph stream
static void BM_StreamIngest(benchmark::State &state) {
  // perform benchmark
  for (auto _ : state) {
//    HashStreamer stream = HashStreamer(num_nodes, num_updates, prime,
//                                       er_prob, rounds, seed1, seed2);
//    GZNonsequentialStreamer stream = GZNonsequentialStreamer(num_nodes,
//            num_updates, prime, er_prob, rounds, seed1, seed2);
    GZSequentialStreamer stream = GZSequentialStreamer(num_nodes, num_updates,
                                                       er_prob, rounds, seed2);
    uint64_t m = stream.stream_length();
    GraphUpdate upd;
    while (m--) {
      benchmark::DoNotOptimize(upd = stream.next());
    }
  }
  state.counters["Ingestion_Rate"] = benchmark::Counter(state.iterations() *
        num_updates, benchmark::Counter::kIsRate);
}
BENCHMARK(BM_StreamIngest);

BENCHMARK_MAIN();
