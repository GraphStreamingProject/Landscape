#pragma once

#include <cassert>
#include <condition_variable>
#include <list>
#include <mutex>

// A MsgBufferQueue is for the synchronization of a thread
// sending or recieving messages and the other threads
// that either produce or consume them.
// Many messages require more than just a buffer to parse
// them. For this reason this structure is templatized.

// TODO: For now this is a locking queue. If we still don't
// get good performance we can try a lock-free design.
template <class MsgData>
class MsgBufferQueue {
 public:
  struct QueueElm {
    QueueElm* next = nullptr;
    MsgData data;

    QueueElm(MsgData& data) : data(std::move(data)){};
  };

  /*
   * Construct an initialized MsgBufferQueue.
   * msg_data_list: An initialized list to construct the
   *                queue from
   * NOTE: its helpful to performace to have
   *       buffers > number of threads doing work
   */
  MsgBufferQueue(std::list<MsgData>& msg_data_list);
  MsgBufferQueue() = default;  // construct an empty queue
  ~MsgBufferQueue();

  // push a message class to the back of the queue
  void push(QueueElm* elm);

  // pop an message class from the front of the queue
  QueueElm* pop();

  // is the MsgBufferQueue empty?
  bool empty() {
    if (head != nullptr) return false;
    list_mutex.lock();
    bool ret = head == nullptr;
    list_mutex.unlock();
    return ret;
  }

 private:
  QueueElm* head = nullptr;
  QueueElm* tail = nullptr;

  std::mutex list_mutex;
  std::condition_variable empty_condition;
};

// Implementations of these functions. Has to be here because reasons
template <class MsgData>
MsgBufferQueue<MsgData>::MsgBufferQueue(std::list<MsgData>& msg_data_list) {
  for (MsgData& msg_data : msg_data_list) {
    MsgBufferQueue::QueueElm* q_elm(new MsgBufferQueue::QueueElm(msg_data));
    push(q_elm);
  }
}

template <class MsgData>
MsgBufferQueue<MsgData>::~MsgBufferQueue() {
  while (head != nullptr) {
    MsgBufferQueue::QueueElm* cur = head;
    head = head->next;
    delete cur;
  }
}

template <class MsgData>
void MsgBufferQueue<MsgData>::push(MsgBufferQueue::QueueElm* elm) {
  elm->next = nullptr;
  list_mutex.lock();

  if (tail == nullptr) {
    head = elm;
    empty_condition.notify_one();
  } else
    tail->next = elm;

  tail = elm;

  list_mutex.unlock();
}

template <class MsgData>
typename MsgBufferQueue<MsgData>::QueueElm* MsgBufferQueue<MsgData>::pop() {
  // optimization: we only have one thread popping from queue so try without a lock
  //               this should work so long as we don't touch tail.
  if (head != nullptr && head->next != nullptr) {
    MsgBufferQueue::QueueElm* ret = head;
    head = head->next;
    return ret;
  }

  std::unique_lock<std::mutex> lk(list_mutex);
  while (head == nullptr) empty_condition.wait(lk, [&]() { return head != nullptr; });

  MsgBufferQueue::QueueElm* ret = head;
  head = head->next;
  if (head == nullptr) tail = nullptr;
  lk.unlock();

  return ret;
}
