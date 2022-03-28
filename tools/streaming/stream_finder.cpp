#include <iostream>

#include "hash_streamer.h"
#include "gz_specific/gz_nonsequential_streamer.h"
#include "gz_specific/gz_sequential_streamer.h"

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cout << "Incorrect number of arguments. "
                 "Expected four but got " << argc-1 << std::endl;
    std::cout << "Arguments are: number of nodes, (least) prime larger or "
                 "equal to number nodes, E-R probability, "
                 "number of rounds" << std::endl;
    exit(EXIT_FAILURE);
  }
  srand(time(NULL));
  node_id_t n = atoll(argv[1]);
  edge_id_t prime = atoll(argv[2]);
  double p = atof(argv[3]);
  int rounds = atoi(argv[4]);

  if (n > 1518500200) {
    // inverting pairing function requires 128 bits above this rough number
    std::cout << "Num nodes above 1518500200 not supported at this time" <<
              std::endl;
    exit(EXIT_FAILURE);
  }
  if (p >= 1 || p <= 0) {
    std::cout << "Probability must fall between (0,1) exclusive" << std::endl;
    exit(EXIT_FAILURE);
  }
  if (rounds < 1) {
    std::cout << "Must choose a number of rounds of updates in range [1,32] "
                 "inclusive" << std::endl;
    exit(EXIT_FAILURE);
  }

//  for (int i = 0; i < 200; ++i) {
//    auto streamer = HashStreamer(n,prime,p,rounds,rand(), rand());
//    // good seeds: 437650290, 1268991550
//    streamer.dump_edges();
//  }

  long seed1 = 437650290;
  long seed2 = 1268991550;
  const long num = 3804488;
//  FastStreamer(n,prime,p,rounds,seed1,seed2).dump_edges();
//  auto e = FastStreamer(n, num, prime, p, rounds, seed1, seed2);
//  for (int i = 0; i < num; ++i) {
//    auto f = e.next();
//    std::cout << f.second << "\t" << f.first.first << "\t" << f.first.second
//    << "\n";
//  }

  GZSequentialStreamer(n, p, rounds, seed2).dump_edges();
//  auto e = WrongAndFast(n, num, p, rounds, seed2);
//  for (int i = 0; i < num; ++i) {
//    auto f = e.next();
//    std::cout << f.second << "\t" << f.first.first << "\t" << f.first.second
//    << "\n";
//  }
}
