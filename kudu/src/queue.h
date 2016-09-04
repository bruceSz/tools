#include <future>

#include "kudu/util/status.h"

#include "sem.h"

// a one producer , multiple consumer blocking queue.
template <typename ValType> 
class BlockQueue {
  public:
  	//BlockQueue(int c_num):consumer_num_(c_num),sem_(c_num) {}
  	BlockQueue():closed_(false) {}
  	kudu:Status Start();
  	ValType Get();
  	void Put(ValType val);
  	void Close();
  	BlockQueue(const BlockQueue& other) = delete;
  	BlockQueue& operator=(const BlockQueue&) = delete;

  private:
  	int consumer_num_;
  	Sem sem_;
  	bool closed_;
  	std::promize<ValType> prom_;
}