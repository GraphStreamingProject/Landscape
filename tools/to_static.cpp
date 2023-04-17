#include <binary_graph_stream.h>

#include <fstream>
#include <iostream>
#include <cassert>

/*
 * Converts a binary graph stream to a static ascii edge list
 */

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cout << "Incorrect number of arguments. "
                 "Expected two but got " << argc-1 << std::endl;
    std::cout << "Arguments are: input_stream, output_file" << std::endl;
    exit(EXIT_FAILURE);
  }
  std::string input  = argv[1];
  std::string output = argv[2];

  assert(input != output);

  BinaryGraphStream stream(input, 32 * 1024);
  std::ofstream out_file(output);


  node_id_t num_nodes = stream.nodes();
  long m              = stream.edges();

  std::vector<std::vector<bool>> adj_mat(num_nodes);
  for(node_id_t i = 0; i < num_nodes; i++)
    adj_mat[i] = std::vector<bool>(num_nodes-i);

  while (m--) {
    GraphUpdate upd = stream.get_edge();
    node_id_t src = upd.edge.src;
    node_id_t dst = upd.edge.dst;
    if (src > dst) std::swap(src, dst);
    dst = dst - src;
    adj_mat[src][dst] = !adj_mat[src][dst];
  }

  std::cout << "Updating adjacency matrix done. Writing static graph to file." << std::endl;
  uint64_t edges = 0;
  for (node_id_t i = 0; i < num_nodes; i++) {
    for(node_id_t j = 0; j < num_nodes - i; j++) {
      if (adj_mat[i][j]) edges++;
    }
  }

  out_file << num_nodes << " " << edges << std::endl;
  for (node_id_t i = 0; i < num_nodes; i++) {
    for(node_id_t j = 0; j < num_nodes - i; j++) {
      if (adj_mat[i][j])
        out_file << i << "\t" << j + i << std::endl;
    }
  }
}
