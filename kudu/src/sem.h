#include <mutex>
#include <condition_variable>

class Sem {
  public:
	Sem(int val = 1):val_(val) {}
	
	Sem(const Sem& other) = delete;
	Sem& operator=(const Sem& ) = delete;
	// increase , release 
	void Sp();
	// decrease , acquire
	void Sv();

  private:
  	int val_;
  	std::mutex mtx_;
  	std::condition_variable cv_;
};
