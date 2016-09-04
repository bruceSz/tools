

struct FileReadException: std::exception {
    const char* what() const noexcept {return "Error getline against ifstream ";}
};


Status TextFile::GetNext(vector<string>* vals) {
    string line;
    if (std::getline(infile_, line)) {
        RETURN_NOT_OK(parse(line, vals));
        return Status::OK();
    } else {
        return Status::EndOfFile("get end of the file");
    }
}


void TextFileIterator::read() {
    Status s = tf_->GetNext(&vals);
    if (s.ok()) {
        is_valid = true;
    } else if (s.IsEndOfFile()) {
        is_valid = false;
        vector<string> tmp;
        tf_ = nullptr;
        vals.swap(tmp);
    } else {
        throw FileReadException();
    }
}
