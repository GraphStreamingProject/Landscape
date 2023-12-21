#pragma once
#include <supernode.h>
#include <types.h>
#include <guttering_system.h>

#include <sstream>

typedef std::pair<node_id_t, std::vector<node_id_t>> batch_t;
enum MessageCode {
  INIT,            // Initialize a process
  BATCH,           // Process a batch of updates for main
  DELTA,           // Supernode deltas computed by distributed worker
  QUERY,           // Perform a query across a set of sketches for main
  FLUSH,           // Tell worker to flush all its local buffers
  STOP,            // Tell the process to wait for new init message
  SHUTDOWN         // Tell the process to shutdown
};

class GraphDistribUpdate;

/*
 * This class provides communication infrastructure for the DistributedWorkers
 * and WorkDistributors.
 * The main node has many WorkDistributor threads running to communicate with
 * the worker nodes in the cluster.
 */
class WorkerCluster {
private:
  static int total_processes;
  static int num_workers;
  static node_id_t num_nodes;
  static uint64_t seed;
  static int max_msg_size;
  static bool active;

  static inline int batch_fwd_to_delta_fwd(int fid) {
    return fid + num_msg_forwarders;
  }

  static inline int delta_fwd_to_batch_fwd(int fid) {
    return fid - num_msg_forwarders;
  }

  /*
   * Call this function to recieve messages
   * @param msg_addr   the address at which to place the message data
   * @param msg_size   pass in the maximum allowed size, function modifies 
                       this variable to contain size of message recieved
   * @param msg_src    this optional variable will contain the source process id when returning
   * @return           a message code signifying the type of message recieved
   */
  static MessageCode recv_message(char* msg_addr, int& msg_size, int& msg_src);
  static inline MessageCode recv_message(char* msg_addr, int& msg_size) {
    int src;
    return recv_message(msg_addr, msg_size, src);
  }

  /*
   * Call this function to receive a message from a specific source process
   */
  static MessageCode recv_message_from(int source, char* msg_addr, int& msg_size);

  /*
   * DistributedWorker: Take a message and parse it into a vector of batches
   * @param msg_addr   The address of the message
   * @param msg_size   The size of the message
   * @param batches    A reference to the vector where we should store the batches
   */
  static void parse_batches(char* msg_addr, int msg_size, std::vector<batch_t>& batches);

  /*
   * DistributedWorker: Serialize a supernode delta to a chunk of memory
   * @param node_idx   The node id the supernode delta refers to
   * @param delta      The Supernode delta to serialize
   * @param serialstr  A serial string to place serialized delta into
   */
  static void serialize_delta(const node_id_t node_idx, Supernode &delta, std::ostream &serial_str);

  friend class WorkDistributor;       // class that sends out work
  friend class DistributedWorker;     // class that does work
  friend class BatchMessageForwarder; // class that forwards messages from WD to DW
  friend class DeltaMessageForwarder; // class that forwards messages from DW to WD
public:
  /*
   * WorkDistributor: Starts a worker cluster and spins up WorkDistributor threads
   * @param num_nodes   Number of nodes in the graph
   * @param seed        Random seed utilized by graph
   * @param batch_size  The size, in bytes, of a single batch
   * @return            The number of workers in the cluster
   */
 static int start_cluster(node_id_t num_nodes, uint64_t seed, int batch_size,
                          double sketches_factor);

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
 static void send_batches(int wid, const std::vector<update_batch>& batches, char* msg_buffer);

 /*
  * WorkDistributor: use this function to wait for the deltas to be returned
  * @param msg_buffer  Message buffer containing the serialized deltas
  * @param msg_size    The size of the serialized deltas
  * @param delta       The Supernode delta memory location
  * @param num_deltas  The number of deltas to recieve
  * @param graph       The graph to update with the delta
  */
 static void parse_and_apply_deltas(char* msg_buffer, int msg_size, Supernode* delta,
                                    GraphDistribUpdate* graph);

 /*
  * DistributedWorker: return a supernode delta to the main node
  * @param delta_msg       String containing the serialized deltas
  * @param delta_msg_size  Size of the serialized deltas message
  */
 static void return_deltas(int dst_id, char* delta_msg, size_t delta_msg_size);

 static void flush_workers();

 /*
  * DistributedWorker: Return the number of updates processed by this worker to main
  * @param num_updates  The number of updates processed by this worker
  */
 static void send_upds_processed(uint64_t num_updates);

 static bool is_active() { return active; }

 static constexpr size_t num_batches = 32;  // the number of Supernodes updated by each batch_msg

 // leader process and forwarder processes on the main node
 static constexpr int leader_proc = 0;          // main node
 static constexpr int num_msg_forwarders = 10;  // sending/recieving messages for main
 static constexpr int distrib_worker_offset = 2 * num_msg_forwarders + 1;
};

class BadMessageException : public std::exception {
private:
  const std::string message;
public:
  BadMessageException(const std::string msg) : message(msg) {}
  virtual const char *what() const throw() {
    return message.c_str();
  }
};
