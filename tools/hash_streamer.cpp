//
// Created by victor on 3/15/22.
//
#include <graph.h>
#include <util.h>
#include <iostream>
#include <cstdlib>

edge_id_t num_edges;
edge_id_t prime;
edge_id_t cutoff;
double p;

bool is_prime(node_id_t x) {
  // TODO
  return true;
}

edge_id_t find_prime_larger_than(edge_id_t x) {
  // TODO
  return 0;
}

// driver function that creates a permutation list and executes a function
// for each value in the permutation
void permutation(int round, void (*func)(int, edge_id_t)) {
  edge_id_t step = prime / 13;
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
  if (hashed ^ filter >= cutoff) { // edge is not in proper state
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
  if (argc != 4) {
    std::cout << "Incorrect number of arguments. "
                 "Expected three but got " << argc-1 << std::endl;
    std::cout << "Arguments are: number of nodes, E-R probability, "
                 "number of rounds" << std::endl;
    exit(EXIT_FAILURE);
  }
  node_id_t n = atoll(argv[1]);
  p = atof(argv[2]);
  int rounds = atoi(argv[3]);
  num_edges = ((edge_id_t) n)*((edge_id_t) n - 1)/2ull;
  prime = find_prime_larger_than(num_edges);
  cutoff = num_edges*p;

  if (p >= 1 || p <= 0) {
    std:cout << "Probability must fall between (0,1) exclusive" << std::endl;
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
