#include "text_file.h"

struct FileReadException: std::exception {
    const char* what() const noexcept {return "Error getline against ifstream ";}
};


kudu::Status TextFile::GetNext(std::vector<std::string>* vals) {
    std::string line;
    if (std::getline(infile_, line)) {
        RETURN_NOT_OK(parse(line, vals));
        return kudu::Status::OK();
    } else {
        return kudu::Status::EndOfFile("get end of the file");
    }
}


void TextFileIterator::read() {
    kudu::Status s = tf_->GetNext(&vals);
    if (s.ok()) {
        is_valid = true;
    } else if (s.IsEndOfFile()) {
        is_valid = false;
        std::vector<std::string> tmp;
        tf_ = nullptr;
        vals.swap(tmp);
    } else {
        throw FileReadException();
    }
}
