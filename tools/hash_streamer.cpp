#include <types.h>
#include <util.h>

#include <iostream>
#include <cstdlib>
#include <cmath>

edge_id_t num_edges;
edge_id_t prime;
edge_id_t cutoff;
double p;

// driver function that creates a permutation list and executes a function
// for each value in the permutation
void permutation(int round, void (*func)(int, edge_id_t)) {
  edge_id_t step = prime / 13ull;
  edge_id_t curr = step;
  while (curr != 0) {
    if (curr <= num_edges) {
      func(round, curr);
    }
    curr += step;
    curr %= prime;
  }
}

// based on the round, either insert or delete
void write_edge(int round, edge_id_t val) {
  vec_hash_t hashed = vec_hash(&val, sizeof(edge_id_t), 42069);
  if (round == 0) {
    auto ins = hashed & 1;
    if (ins) {
      auto pa = inv_nondir_non_self_edge_pairing_fn(val);
      std::cout << "0\t" << pa.first << "\t" << pa.second << "\n";
    }
  } else {
    auto curr = (hashed & (1 << round)) >> round;
    auto prev = (hashed & (1 << (round - 1))) >> (round - 1);
    if (curr ^ prev) {
      auto pa = inv_nondir_non_self_edge_pairing_fn(val);
      if (curr) {
        std::cout << "0\t";
      } else {
        std::cout << "1\t";
      }
      std::cout << pa.first << "\t" << pa.second << "\n";
    }
  }
}

void correct_edge(int rounds, edge_id_t val) {
  vec_hash_t hashed = vec_hash(&val, sizeof(edge_id_t), 42069);
  col_hash_t filter = col_hash(&val, sizeof(edge_id_t), 69420);
  hashed &= 1 << (rounds - 2);
  if (hashed ^ (filter >= cutoff)) { // edge is not in proper state
    auto pa = inv_nondir_non_self_edge_pairing_fn(val);
    if (hashed) { // delete if present, insert if not
      std::cout << "1\t";
    } else {
      std::cout << "0\t";
    }
    std::cout << pa.first << "\t" << pa.second << "\n";
  }
}

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cout << "Incorrect number of arguments. "
                 "Expected four but got " << argc-1 << std::endl;
    std::cout << "Arguments are: number of nodes, (least) prime larger or "
                 "equal to number nodes, E-R probability, "
                 "number of rounds" << std::endl;
    exit(EXIT_FAILURE);
  }
  node_id_t n = atoll(argv[1]);
  prime = atoll(argv[2]);
  p = atof(argv[3]);
  int rounds = atoi(argv[4]);
  num_edges = ((edge_id_t) n)*((edge_id_t) n - 1)/2ull;
  cutoff = (edge_id_t) std::round(ULONG_LONG_MAX*p); // TODO: check to make sure this works properly

  if (p >= 1 || p <= 0) {
    std::cout << "Probability must fall between (0,1) exclusive" << std::endl;
    exit(EXIT_FAILURE);
  }
  if (rounds < 1) {
    std::cout << "Must choose a number of rounds of updates in range [1,32] "
                 "inclusive" << std::endl;
    exit(EXIT_FAILURE);
  }

  for (int i = 0; i < rounds - 1; ++i) {
    permutation(i, write_edge);
  }

  // final round to get everything lined up properly
  permutation(rounds, correct_edge);

  return 0;
}
