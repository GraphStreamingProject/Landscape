#pragma once
#include <supernode.h>
#include <types.h>
#include <guttering_system.h>

#include <sstream>

typedef std::vector<std::pair<node_id_t, Supernode *>> node_sketch_pairs_t;
typedef std::pair<node_id_t, std::vector<node_id_t>> batch_t;

enum MessageCode {
  INIT,       // Initialize a distributed worker
  BATCH,      // Process a batch of updates for main
  DELTA,      // Supernode deltas computed by distributed worker
  QUERY,      // Perform a query across a set of sketches for main
  BUFF_QUERY, // Ask a DistributedWorker how many delta responses it will buffer
  FLUSH,      // Tell worker to flush all its local buffers
  STOP,       // Tell the worker we are no longer providing updates and to wait for init message
  SHUTDOWN    // Tell the worker to shutdown
};

/*
 * This class provides communication infrastructure for the DistributedWorkers
 * and WorkDistributors.
 * The main node has many WorkDistributor threads running to communicate with
 * the worker nodes in the cluster.
 */
class WorkerCluster {
private:
  static int num_workers;
  static node_id_t num_nodes;
  static uint64_t seed;
  static int max_msg_size;
  static bool active;

  /*
   * DistributedWorker: call this function to recieve messages
   * @param msg_addr   the address at which to place the message data
   * @param msg_size   pass in the maximum allowed size, function modifies 
                       this variable to contain size of message recieved
   * @return           a message code signifying the type of message recieved
   */
  static MessageCode worker_recv_message(char *msg_addr, int *msg_size);

  /*
   * DistributedWorker: Take a message and parse it into a vector of batches
   * @param msg_addr   The address of the message
   * @param msg_size   The size of the message
   * @param batches    A reference to the vector where we should store the batches
   */
  static void parse_batches(char *msg_addr, int msg_size, std::vector<batch_t> &batches);

  /*
   * DistributedWorker: Serialize a supernode delta to a chunk of memory
   * @param node_idx   The node id the supernode delta refers to
   * @param delta      The Supernode delta to serialize
   * @param serialstr  A serial string to place serialized delta into
   */
  static void serialize_delta(const node_id_t node_idx, Supernode &delta, std::ostream &serial_str);

  friend class WorkDistributor;   // class that sends out work
  friend class DistributedWorker; // class that does work
public:
  /*
   * WorkDistributor: Starts a worker cluster and spins up WorkDistributor threads
   * @param num_nodes   Number of nodes in the graph
   * @param seed        Random seed utilized by graph
   * @param batch_size  The size, in bytes, of a single batch
   * @return            The number of workers in the cluster
   */
  static int start_cluster(node_id_t num_nodes, uint64_t seed, int batch_size);

  /*
   * WorkDistributor: Tell the cluster that the current GraphDistribUpdate is stopping
   * the cluster will wait for a new initialize message
   * @return the number of updates processed by the cluster
   */
  static uint64_t stop_cluster();

  /*
   * WorkDistributor: Tell cluster to shutdown and exit
   */
  static void shutdown_cluster();

  /*
   * WorkDistributor: use this function to send a batch of updates to
   * a DistributedWorker
   * @param wid         The id of the DistributedWorker to send to
   * @param batches     The data to send to the distributed worker
   * @param msg_buffer  Memory buffer to use for recieving a message
   */
  static void send_batches(int wid, const std::vector<update_batch> &batches, char *msg_buffer);

  /*
   * WorkDistributor: use this function to wait for the deltas to be returned
   * @param src_id      The source index of the DistributedWorker to receive from
   * @param deltas      A vector of src node and Supernode delta pairs where we place recieved data
   * @param num_deltas  The number of deltas to recieve
   * @param msg_buffers Depending upon sender of deltas use one of these memory buffers to recieve
   * @param min_id      The smallest wid the thread calling this function is responsible for
   */
  static void recv_deltas(int src_id, node_sketch_pairs_t &deltas, size_t &num_deltas,
                          char *msg_buffer);

  /*
   * DistributedWorker: return a supernode delta to the main node
   * @param delta_msg       String containing the serialized deltas
   * @param delta_msg_size  Size of the serialized deltas message
   */
  static void return_deltas(char* delta_msg, size_t delta_msg_size);

  static void send_tag_to(MessageCode tag, size_t first, size_t num);

  /*
   * DistributedWorker: Return the number of updates processed by this worker to main
   * @param num_updates  The number of updates processed by this worker
   */
  static void send_upds_processed(uint64_t num_updates);

  static bool is_active() { return active; }

  static constexpr size_t num_batches = 8; // the number of Supernodes updated by each batch_msg
};

class BadMessageException : public std::exception {
private:
  const char *message;
public:
  BadMessageException(const char *msg) : message(msg) {}
  virtual const char *what() const throw() {
    return message;
  }
};
