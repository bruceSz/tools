#include "queue.h"
 
 ValType BlockQueue::Get() {
 	if (closed_)
 		return nullptr;
 	std::shared_future<ValType> data_future(prom_.get_future());
 	ValType val = data_future.get();
 	sem_.Sv();
 	
 }
 void BlockQueue::Put(ValType val) {
 	// should return Status.
 	if (closed_)
 		return;
 	sem_.Sp();
 	prom_.set_value(val);
 }

 void BlockQueue::Close() {
 	closed_ = true;
 }

 kudu::Status start() {
 	// TODO.
 }
