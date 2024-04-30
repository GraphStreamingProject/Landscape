#pragma once

#include <binary_graph_stream.h>
#include <math.h>

#include <atomic>
#include <iostream>
#include <string>

#pragma pack(push,1)
struct GraphStreamUpdate {
  uint8_t type;
  Edge edge;
};
#pragma pack(pop)

static constexpr edge_id_t END_OF_STREAM = (edge_id_t) -1;

class SimpleStream {
 public:
  SimpleStream(size_t seed, node_id_t nodes, node_id_t offset = 0) : seed(seed) {
    node_id_t temp_verts = 1 << (size_t)ceil(log2(nodes));  // round up to nearest power of 2
    if (temp_verts != nodes) {
      std::cerr << "WARNING: Rounding up number of vertices: " << nodes << " to nearest power of 2"
                << std::endl;
    }
    num_vertices = temp_verts;
    vertices_mask = num_vertices - 1;
    upd_idx = 0;
    breakpoint_idx = END_OF_STREAM;
    total_edges = num_vertices * (num_vertices - 1) / 2;
    vertex_offset = offset;
  }

  inline size_t get_update_buffer(GraphStreamUpdate* upd_buf, size_t num_updates) {
    edge_id_t local_idx = upd_idx.fetch_add(num_updates, std::memory_order_relaxed);
    size_t ret = 0;
    if (local_idx + num_updates > breakpoint_idx) {
      upd_idx = breakpoint_idx.load();
      num_updates = local_idx > breakpoint_idx ? 0 : breakpoint_idx - local_idx;
      upd_buf[num_updates] = {BREAKPOINT, {0, 0}};
      ret++;
    }

    // populate the update buffer
    for (size_t i = 0; i < num_updates; i++) upd_buf[i] = create_update(local_idx++);
    return ret + num_updates;
  }

  inline bool get_update_is_thread_safe() { return true; }

  inline bool set_break_point(edge_id_t break_idx) {
    if (break_idx < upd_idx) return false;
    breakpoint_idx = break_idx;
    return true;
  }

 private:
  // some basic stream parameters
  const size_t seed;
  node_id_t num_vertices;
  node_id_t vertices_mask;
  node_id_t total_edges;
  node_id_t vertex_offset;
  static constexpr col_hash_t node_id_bits = sizeof(node_id_t) * 8;
  static constexpr col_hash_t node_id_mask = (1ull << node_id_bits) - 1;

  inline node_id_t get_dst_mask(node_id_t first_dst) {
    return ~(node_id_t(-1) << (node_id_bits - __builtin_clzl(num_vertices - first_dst) - 1));
  }

  // current state of the stream
  std::atomic<edge_id_t> upd_idx;
  std::atomic<edge_id_t> breakpoint_idx;

  // Helper functions
  inline GraphStreamUpdate create_update(edge_id_t idx) {
    node_id_t src = get_random_src(idx);
    node_id_t mask = get_dst_mask(src + 1);
    node_id_t dst = src + 1 + (mask & col_hash(&src, sizeof(src), idx));

    return {INSERT, {src + vertex_offset, dst + vertex_offset}};
  }

  inline node_id_t get_random_src(edge_id_t update_idx) {
    col_hash_t hash = col_hash(&update_idx, sizeof(update_idx), seed);
    node_id_t even_hash = (hash >> node_id_bits) & (vertices_mask - 1);
    node_id_t odd_hash = ((hash & node_id_mask) & vertices_mask) | 1;
    return std::min(even_hash, odd_hash);
  }
};
