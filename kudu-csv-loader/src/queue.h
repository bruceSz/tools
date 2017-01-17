#include <future>
#include <vector>

#include "kudu/util/status.h"

#include <mutex>
#include <condition_variable>
#include "sem.h"

struct EmptyQueueException: std::exception {
    const char* what() const noexcept {return "Error: there will be no more val in queue";}
};

// a one producer , multiple consumer blocking queue.
template <typename ValType> 
class BlockQueue {
  public:
  	//BlockQueue(int c_num):consumer_num_(c_num),sem_(c_num) {}
  	BlockQueue(int queue_size):q_size_(queue_size),curr_num_(0),closed_(false) {}
  	BlockQueue():q_size_(1),curr_num_(0),closed_(false) {}
  	kudu::Status Get(ValType* val);
  	kudu::Status Put(ValType val);
    bool IsEmpty() {return curr_num_==0;};
  	void Close();

  	BlockQueue(const BlockQueue& other) = delete;
  	BlockQueue& operator=(const BlockQueue&) = delete;

  private:
    std::vector<ValType> data_vec_;
    unsigned int q_size_;
    unsigned int curr_num_;
    bool closed_;

    std::mutex lock_;
    std::condition_variable data_arrival_cv_;
    std::condition_variable data_removal_cv_;
};


template<typename ValType>
kudu::Status BlockQueue<ValType>::Put(ValType val) {
    std::unique_lock<std::mutex> l(lock_);
    if (closed_) return kudu::Status::Aborted("Put return due to queue abort/finish");
    while (curr_num_ >= q_size_ && !closed_) {
        data_removal_cv_.wait(l);
        data_arrival_cv_.notify_one();
    }
    curr_num_++;
    data_vec_.push_back(val);
    data_arrival_cv_.notify_one();
    return kudu::Status::OK();
 }

template<typename ValType>
kudu::Status BlockQueue<ValType>::Get(ValType* val) {
    std::unique_lock<std::mutex> l(lock_);
    if (closed_) return kudu::Status::Aborted("Get return due to queue abort/finish");

    while(curr_num_ <= 0 && !closed_) {
        data_arrival_cv_.wait(l);
        data_removal_cv_.notify_one();
    }
    if (closed_) return kudu::Status::Aborted("Get return due to queue abort/finish");
    curr_num_--;
    *val = data_vec_.back();
    data_vec_.pop_back();
    data_removal_cv_.notify_one();
    return kudu::Status::OK();
 }

template<typename T>
void BlockQueue<T>::Close() {
    std::lock_guard<std::mutex> l(lock_);
    closed_ = true;
    data_arrival_cv_.notify_all();
    data_removal_cv_.notify_all();
}
