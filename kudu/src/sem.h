#include <mutex>
#include <condition_variable>

class Sem {
  public:
	~Sem(int val = 0):val_(val),concurrent_producer_(val_) {}
	
	Sem(const Sem& other) = delete;
	Sem& operator=(const Sem& ) = delete;
	// consume , may wait
	void Sp();
	// produce/release
	void Sv();

  private:
  	int val_;
  	int concurrent_producer_;
  	std::mutex mtx_;
  	std::condition_variable cv_;
};