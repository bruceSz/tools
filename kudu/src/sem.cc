#include "sem.h"
#include <thread>
// p means produce and increase internal val, maybe wait here
void Sem::Sp() {
	std::unique_lock <std::mutex> l(mtx_);
	while(val_ >= concurrent_producer_)
		cv_.wait(l);
	val_++;
}

// v means produce and increase internal val, should not wait.
void Sem::Sv() {
	std::unique_lock<std::mutex> l(mtx_);
	// TODO: maybe don't need a while here, just make max to 1.
	/*while(!val <1)
		cv_.wait(l);*/
	// below implementation is specified for usage of BlockQueue
	if (val_ > 0)
		val_--;
}