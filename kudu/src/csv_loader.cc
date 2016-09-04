#include <iostream>
#include <string>
#include <stdlib.h>
#include <fstream>


#include "kudu/client/client.h"
#include "kudu/common/partial_row.h"
#include "kudu/util/status.h"

#include "gflags/gflags.h"

#include <boost/tokenizer.hpp>

#include "util.h"


using namespace std;
using kudu::Status;
using kudu::KuduPartialRow;
using kudu::client::KuduSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSession;

#define RETURN_NOT_OK(s) do {\
    Status _s = (s);\
    if (!_s.ok()) return _s;\
    } while (0);\
    

DEFINE_int32(kudu_session_timeout, 60000,
            "must set session timeout ,or kudu client will crash.");
DEFINE_int32(kudu_session_flush_buffer_size, 1000,
            "session auto background flush buffer size");
DEFINE_string(master_addresses, "localhost",
              "Comma-separated list of Kudu Master server addresses");

void Usage(string prog_name) {
    cout << prog_name << ": -master_addresses [host:port]" <<
                 " table_name csv_file" << endl;
    exit(1);
}

struct FileReadException: std::exception {
    const char* what() const noexcept {return "Error getline against ifstream ";}
};
class TextFileIterator;
class TextFile {
  public:
    TextFile():num_line_read_(0),file_name_(),infile_(file_name_) {}      
    TextFile(std::string fname):num_line_read_(0),
            file_name_(std::move(fname)),infile_(file_name_) {}
    virtual ~TextFile() = default;


    Status GetNext(vector<string>* vals) ; 
    
    TextFile(const TextFile&) = delete;
    TextFile& operator=(const TextFile& ) = delete;
    virtual TextFileIterator begin() = 0;
    virtual TextFileIterator end() = 0;

  protected:
    // parse given line and do string manipulation 
    virtual Status parse(string line,vector<string>* vals) const = 0;

  private:
    size_t num_line_read_ ;
    std::string file_name_;
    std::ifstream infile_;
};

/*
 * Limitation:
 *  This iterator is only used for TextFile , one time a iterator.
 *  two iterator exist same time will affect each other.
 *  Also caller is in charge of existence of tf_.
 *
 * */
class TextFileIterator {
    TextFile* tf_;
    vector<string> vals;
    bool is_valid;
    void read();
  public:
    typedef input_iterator_tag iterator_category;
    typedef vector<string> value_type;
    typedef ptrdiff_t difference_type;
    typedef const vector<string>* pointer;
    typedef const vector<string>& reference;
    
    reference operator*() {
        return vals;
    }
    pointer operator->() {
        return &vals;
    }
    TextFileIterator():tf_(nullptr),is_valid(false),vals({}){}
    TextFileIterator(TextFile* t_file):tf_(t_file) {
        read();
    }
    TextFileIterator operator++() {
        read();
        return *this;
    }
    TextFileIterator operator++(int) {
        TextFileIterator tmp = *this;
        read();
        return tmp;
    }
    bool operator==(const TextFileIterator& rhs) {
        if (tf_ ==  rhs.tf_ && vals == rhs.vals && is_valid == rhs.is_valid)
            return true;
        if (is_valid == false && rhs.is_valid == false)
            return true;
        return false;
    }
    bool operator!=(const TextFileIterator& rhs) {
        return !(*this==rhs);
    }
};

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


Status TextFile::GetNext(vector<string>* vals) {
    string line;
    if (std::getline(infile_, line)) {
        RETURN_NOT_OK(parse(line, vals));
        return Status::OK();
    } else {
        return Status::EndOfFile("get end of the file");
    }
}

class RawFile: public TextFile {
  public :
    RawFile(std::string fname):TextFile(fname) {}
    virtual Status parse(string line, vector<string>* vals) const override {
        vals->clear();
        vals->push_back(line);
        return Status::OK();
    }
    virtual TextFileIterator begin()  override {
        return  TextFileIterator(this);
    }
    virtual TextFileIterator end() override {
        return TextFileIterator();
    }
    
};

class CSVFile: public TextFile {
  public :
    CSVFile(std::string fname):TextFile(fname) {}
    virtual Status parse(string line, vector<string>* vals) const override {
        vals->clear();
        RETURN_NOT_OK(do_parse(line, vals));
        return Status::OK();
    }
    virtual TextFileIterator begin() override {
        return  TextFileIterator(this);
    }
    virtual TextFileIterator end() override {
        return TextFileIterator();
    }
  private:
    typedef boost::tokenizer<boost::char_separator<char>> Tokenizer;
    Status do_parse(string line, vector<string>* vals) const {
        vector<string> vec;
        boost::char_separator<char> sep{","};
        Tokenizer tok{line, sep};
        vec.assign(tok.begin(), tok.end());
        vals->swap(vec);
        return Status::OK();
    }
};

class CSVLoader {
  public:
    CSVLoader(string master_addr, string csv_file):csv_file_(csv_file),
                master_addr_(master_addr){}
    Status load(string tab_name); 
    CSVLoader(const CSVLoader&) = delete;
    CSVLoader& operator=(const CSVLoader& ) = delete;
  private:
    CSVFile csv_file_;
    string master_addr_;
};

Status doInsert(KuduPartialRow* row, KuduSchema* schema, int idx, string val) {
    KuduColumnSchema c_schema = schema->Column(idx);
    switch (c_schema.type()) {
        case KuduColumnSchema::INT32:
        {
            KUDU_RETURN_NOT_OK(row->SetInt32(idx, stringToInt32(val)));
            break;
        }
        case KuduColumnSchema::INT64:
        {
            KUDU_RETURN_NOT_OK(row->SetInt64(idx, stringToInt64(val)));
            break;
        }
        case KuduColumnSchema::STRING:
        {
            KUDU_RETURN_NOT_OK(row->SetStringCopy(idx, val));
            break;
        }
        case KuduColumnSchema::TIMESTAMP:
        {
            KUDU_RETURN_NOT_OK(row->SetInt64(idx, stringToInt64(val)));
            break;
        }
    }
    return Status::OK();
}

Status CSVLoader::load(string table_name) {
    auto client = create_client(master_addr_);
    auto table = openTable(client, table_name);
    KuduSchema schema;
    KUDU_RETURN_NOT_OK(client->GetTableSchema(table_name, &schema));

    std::tr1::shared_ptr<KuduSession> session = client->NewSession();
    session->SetTimeoutMillis(FLAGS_kudu_session_timeout);
    session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND);
    //KUDU_RETURN_NOT_OK(session->SetMutationBufferSpace(FLAGS_kudu_session_flush_buffer_size));
    
    
    for(TextFileIterator it = csv_file_.begin();it != csv_file_.end(); it++) {
        auto col_vals = *it;
        unique_ptr<KuduInsert> insert(table->NewInsert());
        KuduPartialRow * row = insert->mutable_row();
        for (int i = 0; i< col_vals.size(); i++) {
            doInsert(row, &schema, i, col_vals[i]);
        }
        KUDU_RETURN_NOT_OK(session->Apply(insert.release()));
    }
    KUDU_RETURN_NOT_OK(session->Flush());
    return Status::OK();
}

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (argc < 3) {
        Usage(argv[0]);
    } 
    string table_name(argv[1]);
    string csv_file(argv[2]);
    cout << "table_name: " << table_name << endl;
    cout << "csv file:" << csv_file << endl;
    CSVLoader f(FLAGS_master_addresses, csv_file);
    KUDU_EXIT_NOT_OK(f.load(table_name), "error load csv file into kudu table");

    /*for(TextFileIterator it = f.begin();it != f.end(); it++) {
        auto ret = *it;
        for (auto c : ret) {
             cout << c << ":";
        }
        cout << endl;
    }*/
}
