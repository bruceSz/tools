#include <thread>

#include "sem.h"

// this is typically a binary sem
void Sem::Sp() {
    std::unique_lock<std::mutex> l(mtx_);
    if ( val_ <= 0)
        val_++;
    cv_.notify_one();
}

void Sem::Sv() {
	std::unique_lock <std::mutex> l(mtx_);
    // actually, there is no way we get in to the val_ < 0 situation
	while(val_ <= 0)
		cv_.wait(l);
	val_--;
}
