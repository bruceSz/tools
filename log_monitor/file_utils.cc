/*
 * Author: zhangsong5@jd.com
 * Date: 2016/12/26
 *
 * */

#include <cstdlib>
#include <iostream>
#include "file_utils.h"

using std::string;
using std::cout;
using std::endl;
namespace log {
namespace monitor {

void TextFile::Init() {
    ifs_.open(file_name_, ios::in);
    if (!ifs_) {
        return Status::IOError("Error open file:" + file_name_);
    }
    return Status::OK();
}

kudu::Status TextFile::GetNextLine(string* line) {
	if (ifs_.peek() == EOF) {
		return Status::EndOfFile("Met end of the file");
	}
	getline(ifs_, *line);
	seek_ = ifs_.tellg();
	return Status::OK();
}

void TextFile::ResetFileIndicator() {
	ifs_.clear();
    ifs_.seekg(seek_, ios::beg);
}

void TextFileIcrementalIterator::next_line() {
	 
	Status s = tf_->GetNextLine(&row);
	while (s.IsEndOfFile()) {
		tf_->ResetFileIndicator();
		sleep(wait_internal_);
		s = tf_->GetNextLine(&row);
	}
	if (!s.ok()) {
		cout << "read file error" << endl;
		std::exit(1);
	}
	return;
}

} // monitor
} // log