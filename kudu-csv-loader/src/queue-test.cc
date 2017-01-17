#include <iostream>
#include <thread>
#include <memory>
#include <chrono>
#include <stdlib.h>
#include "queue.h"
#include "util.h"

#include "kudu/util/status.h"


using namespace std;
template<typename T>
kudu::Status producer(std::shared_ptr<BlockQueue<T>> q, int sleep_time ) {
		for(int i=0; i< 10; i++) {
			KUDU_EXIT_NOT_OK(q->Put(i),"Error Put ");
                if (sleep_time == -1) {
                    std::this_thread::sleep_for(std::chrono::seconds(rand()%2));
                } else {
                    std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
                }
		}
        std::cout << "pro finish!" << endl;
    return kudu::Status::OK();
}

template<class T>
kudu::Status consumer( std::shared_ptr<BlockQueue<T>> q, int sleep_time) {
            int z = -1;
            while(1) {
                KUDU_RETURN_NOT_OK(q->Get(&z));
                if (sleep_time == -1) {
                    std::this_thread::sleep_for(std::chrono::seconds(rand()%2));
                } else {
                    std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
                }
                cout << "Get: " << z << endl;
            }
        std::cout << "consumer die!" << endl;
    return kudu::Status::OK();
}

void TestFastProducerSlowConsumer(int consumer_num) {
	std::shared_ptr<BlockQueue<int>> q(new BlockQueue<int>());
	std::thread tp(producer<int>, q, 0 );
    std::vector<std::thread> threads;
    for(int i = 0; i<consumer_num; i++) {
        threads.push_back(std::thread(consumer<int>, q, 1));
    }
    //tp.join();
    tp.join();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::cout << "sleep for 1 sec" << std::endl;
    q->Close();
    for(int i=0;i<consumer_num;i++) {
        threads.at(i).join();
    }
}

void TestSlowProducerFastConsumer(int producer_num) {
	std::shared_ptr<BlockQueue<int>> q(new BlockQueue<int>());
	std::thread tp(producer<int>, q, 1 );
    std::vector<std::thread> threads;
    for(int i = 0; i<producer_num; i++) {
        threads.push_back(std::thread(consumer<int>, q, 0));
    }
    //tp.join();
    tp.join();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::cout << "sleep for 1 sec" << std::endl;
    q->Close();
    for(int i=0;i<producer_num;i++) {
        threads.at(i).join();
    }
    
}

void TestLargeQueueSize(int queue_size) {
	std::shared_ptr<BlockQueue<int>> q(new BlockQueue<int>(queue_size));
	std::thread tp(producer<int>, q, -1 );
    std::vector<std::thread> threads;
    for(int i = 0; i<5; i++) {
        threads.push_back(std::thread(consumer<int>, q, -1));
    }
    //tp.join();
    tp.join();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    std::cout << "sleep for 1 sec" << std::endl;
    q->Close();
    for(int i=0;i<5;i++) {
        threads.at(i).join();
    }
}


// This test should fail, as promise can not set_value multi times.
void TestMultiFuture() {
    std::promise<int> prom;
    std::future<int> fu =  prom.get_future();
    auto f1 = [&] () {
        for(int i=0; i< 3; i++)
        {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                prom.set_value(1);
        }
        
    };
    auto f2 = [&] () {
        std::cout << fu.get() << std::endl;
    };
    std::thread t1(f1);
    std::thread t2(f2);
    std::thread t3(f2);
    std::thread t4(f2);
    t1.join();
    t2.join();
    t3.join();
    t4.join();
}

int main() {
    //TestMultiFuture();
    TestFastProducerSlowConsumer(3); 
    TestSlowProducerFastConsumer(3);
}
