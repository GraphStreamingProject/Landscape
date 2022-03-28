#include <string>
#include <chrono>
#include <iostream>

#include "mpi.h"

using Msg = std::pair<uint64_t, uint64_t>;
using secs = std::chrono::seconds;
using ms = std::chrono::milliseconds;
const double M = 1000000.0; 
const double K = 1000.0; 

void run_test(uint64_t msg_per_batch, uint64_t batch_per_worker);

int main(int argc, char **argv) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &provided);
  
  int shifts = 29;

  uint64_t batch_size = 1;
  uint64_t loops = 1 << shifts;
  for (int i = 10; i < shifts-2; i++)
    run_test(batch_size << i, loops >> i);

  MPI_Finalize();
}

void run_test(uint64_t msg_per_batch, uint64_t batch_per_worker)
{
  Msg* msgs = new Msg[msg_per_batch];

  int proc_id;
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);
  if (proc_id > 0) {
    // we are a worker, start working!
    MPI_Status status;
    for (uint64_t j = 0; j < batch_per_worker; j++)
    {
      MPI_Recv(msgs, msg_per_batch * sizeof(Msg) / sizeof(MPI_LONG_LONG), MPI_LONG_LONG, 0, 0, MPI_COMM_WORLD, &status);
    }
  }
  else
  {
  auto start = std::chrono::steady_clock::now();
    int num_workers;
    MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
    //We are the master!
    for (uint64_t j = 0; j < batch_per_worker; j++)
    {
      for (int i = 1; i < num_workers; i++)
      {
        MPI_Send(msgs, msg_per_batch * sizeof(Msg)/ sizeof(MPI_LONG_LONG), MPI_LONG_LONG, i, 0, MPI_COMM_WORLD);
      }
    }
    auto stop = std::chrono::steady_clock::now();
    std::chrono::duration<double> runtime = stop - start;

    uint64_t total = batch_per_worker * msg_per_batch;
    double ins_per_sec = (num_workers-1) * total / runtime.count();
    std::cout << "Sending " << total/M << "M updates (batch size of " << msg_per_batch << ") to each of " << num_workers-1 << " workers took " 
      << (uint64_t)runtime.count() << " seconds, " << ins_per_sec / M << "M per second" << std::endl;
    std::cerr << msg_per_batch << ", " << ins_per_sec << std::endl;
  }

  delete[] msgs;
}
