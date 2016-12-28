/*
 * Author: zhangsong5@jd.com
 * Date: 2016/12/26
 *
 * */

#include <cstdlib>
#include <iostream>
#include <unistd.h>
#include <fstream>
#include "file_utils.h"

using std::string;
using std::cout;
using std::endl;
using std::ios;
using kudu::Status;
namespace log {
namespace monitor {

Status TextFile::Init() {
    ifs_.open(file_name_, ios::in);
    if (!ifs_) {
        return Status::IOError("Error open file:" + file_name_);
    }
    ifs_.seekg (0, ifs_.end);
    seek_ = ifs_.tellg();
    return Status::OK();
}

kudu::Status TextFile::doGetNextLine(std::ifstream& ifs, string& row) {
    string tmp;
    char t_c;
    while(ifs.peek() != EOF) {
        ifs.get(t_c);
        if (t_c == '\n') {
            row.swap(curr_row_);
            // clean curr row.
            curr_row_.swap(tmp);
            return Status::OK();
        }
        curr_row_.push_back(t_c);
    }
    return Status::EndOfFile("End of file");
}

kudu::Status TextFile::GetNextLine(string* line) {
	/*if (ifs_.peek() == EOF) {
		return Status::EndOfFile("Met end of the file");
	}*/
    Status s = doGetNextLine(ifs_, *line);
    if (!s.ok()) {
        if (s.IsEndOfFile()){
            // after met end of file , seekg is needed as pointer of ifs will be messed up.
            ifs_.clear();
            ifs_.seekg(0, ios::end);
            return s;
        } else {
            return Status::IllegalState(s.message());
        }
    }
    return Status::OK();
}

size_t TextFile::getSize() {
    ifs_.seekg(0,ifs_.end);
    size_t length = ifs_.tellg();
    ifs_.seekg(seek_,ifs_.beg);
    return length;
}

void TextFile::ResetFileIndicator() {
	ifs_.clear();
    ifs_.seekg(seek_, ios::beg);
}

void TextFileIncrementalIterator::next_line() {
	 
	Status s = tf_->GetNextLine(&row);
	while (s.IsEndOfFile()) {
		//tf_->ResetFileIndicator();
		sleep(wait_internal_);
		s = tf_->GetNextLine(&row);
        //cout << "xx" << s.message().ToString()<< "pos:" << tf_->seek() << " total: "<< tf_->getSize() <<  endl;
	}
	if (!s.ok()) {
		cout << "read file error" << s.message().ToString()<< endl;
		std::exit(1);
	}
	return;
}

} // monitor
} // log
