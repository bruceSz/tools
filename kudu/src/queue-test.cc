#include <thread>
#include <memory>
#include "queue.h"

void TestFastProducerSlowConsumer() {
	std::shared_ptr<BlockQueue> bq();
	auto fun1 = [] (std::shared_ptr<BlockQueue> q) {
		for(int i=0; i< 1000; i++) {
			q.Put(i);
		}
	}
	std::thread(fun1, bq);
	auto fun2 = [](std::shared_ptr<BlockQueue> q) {
		
	}
}

int main() {

	std::thread 
}