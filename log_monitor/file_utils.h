/*
 * Author: zhangsong5@jd.com
 * Date: 2016/12/26
 *
 * */

#ifndef LOG_MONITOR_FILE_UTIL_H_
#define LOG_MONITOR_FILE_UTIL_H_

#include <string>
#include <fstream>
#include <kudu/util/status.h>

namespace log {
namespace monitor {

class TextFile;

// incremental iterator should never get to the end, it just sleep;
class TextFileIncrementalIterator {
	TextFile* tf_;
	std::string row;
	size_t timeout_;
	size_t wait_internal_{1};
	bool get_end_;
	void next_line();
public:
	typedef std::input_iterator_tag iterator_category;
    typedef std::string value_type;
    typedef ptrdiff_t difference_type;
    typedef const std::string* pointer;
    typedef const std::string& reference;

     reference operator*() {
        return row;
    }
    pointer operator->() {
        return &row;
    }
    TextFileIncrementalIterator():tf_(nullptr),timeout_(-1){}
    TextFileIncrementalIterator(TextFile* t_file):tf_(t_file),timeout_(-1) {
    	next_line();
    }
    TextFileIncrementalIterator operator++() {
        next_line();
        return *this;
    }

    TextFileIncrementalIterator operator++(int) {
        TextFileIncrementalIterator tmp = *this;
        next_line();
        return tmp;
    }
    bool operator==(const TextFileIncrementalIterator& rhs) {
        if (tf_ ==  rhs.tf_ && row == rhs.row)
            return true;
        return false;
    }
    bool operator!=(const TextFileIncrementalIterator& rhs) {
        return !(*this==rhs);
    }
};

class TextFile {
public:
    TextFile(const std::string& file_name):file_name_(file_name) {}
    kudu::Status Init();
    kudu::Status GetNextLine(std::string* str);
    size_t getSize();
    TextFileIncrementalIterator begin()  {
        return  TextFileIncrementalIterator(this);
    }
    TextFileIncrementalIterator end()  {
        return TextFileIncrementalIterator();
    }
private:
    kudu::Status doGetNextLine(std::ifstream& ifs, std::string& row);
	friend class TextFileIncrementalIterator;
	void ResetFileIndicator();
	size_t seek() const {return seek_;}
	TextFile(const TextFile& o) = delete;
	TextFile& operator=(const TextFile& o) = delete;

    std::string file_name_;
    std::ifstream ifs_;
    size_t seek_;
    std::string curr_row_;
};



} // monitor 
} // log


#endif

