#include <binary_graph_stream.h>

#include <string>
#include <iostream>

int main(int argc, char **argv) {

  if (argc != 4) {
    std::cout << "Incorrect number of arguments. "
                 "Expected three but got " << argc-1 << std::endl;
    std::cout << "Arguments are: insert_threads, input_stream, output_directory" << std::endl;
    exit(EXIT_FAILURE);
  }
  int inserter_threads = std::atoi(argv[1]);
  if (inserter_threads < 1 || inserter_threads > 50) {
    std::cout << "Number of inserter threads is invalid. Require in [1, 50]" << std::endl;
    exit(EXIT_FAILURE);
  }
  std::string input  = argv[2];
  std::string output_dir = argv[3];

  BinaryGraphStream_MT stream(input, 32 * 1024);

  std::vector<std::thread> threads;
  threads.reserve(inserter_threads);

  auto task = [&](const unsigned int id) {
    MT_StreamReader reader(stream);
    std::ofstream output_file(output_dir + "/thread_" + std::to_string(id));
    GraphUpdate upd;
    while(true) {
      upd = reader.get_edge();
      if (upd.second == END_OF_FILE) break;
      output_file << upd.second << " " << upd.first.first << " " << upd.first.second << std::endl;
    }
  };

  // start inserters
  for (int i = 0; i < inserter_threads; i++) {
    threads.emplace_back(task);
  }
  // wait for inserters to be done
  for (int i = 0; i < inserter_threads; i++) {
    threads[i].join();
  }
}
