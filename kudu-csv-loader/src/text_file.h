#include <string.h>
#include <vector>
#include <fstream>
//#include <iterator>

#include <boost/tokenizer.hpp>

#include "kudu/util/status.h"

#define RETURN_NOT_OK(s) do {\
    kudu::Status _s = (s);\
    if (!_s.ok()) return _s;\
    } while (0);\

class TextFileIterator;
class TextFile {
  public:
    TextFile():num_line_read_(0),file_name_(),infile_(file_name_) {}      
    TextFile(std::string fname):num_line_read_(0),
            file_name_(std::move(fname)),infile_(file_name_) {}
    virtual ~TextFile() = default;


    kudu::Status GetNext(std::vector<std::string>* vals) ; 
    
    TextFile(const TextFile&) = delete;
    TextFile& operator=(const TextFile& ) = delete;
    virtual TextFileIterator begin() = 0;
    virtual TextFileIterator end() = 0;

  protected:
    // parse given line and do string manipulation 
    virtual kudu::Status parse(std::string line,std::vector<std::string>* vals) const = 0;

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
    std::vector<std::string> vals;
    bool is_valid;
    void read();
  public:
    typedef std::input_iterator_tag iterator_category;
    typedef std::vector<std::string> value_type;
    typedef ptrdiff_t difference_type;
    typedef const std::vector<std::string>* pointer;
    typedef const std::vector<std::string>& reference;
    
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

class RawFile: public TextFile {
  public :
    RawFile(std::string fname):TextFile(fname) {}
    virtual kudu::Status parse(std::string line, std::vector<std::string>* vals) const override {
        vals->clear();
        vals->push_back(line);
        return kudu::Status::OK();
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
    virtual kudu::Status parse(std::string line, std::vector<std::string>* vals) const override {
        vals->clear();
        RETURN_NOT_OK(do_parse(line, vals));
        return kudu::Status::OK();
    }
    virtual TextFileIterator begin() override {
        return  TextFileIterator(this);
    }
    virtual TextFileIterator end() override {
        return TextFileIterator();
    }
  private:
    typedef boost::tokenizer<boost::char_separator<char>> Tokenizer;
    kudu::Status do_parse(std::string line, std::vector<std::string>* vals) const {
        std::vector<std::string> vec;
        boost::char_separator<char> sep{","};
        Tokenizer tok{line, sep};
        vec.assign(tok.begin(), tok.end());
        vals->swap(vec);
        return kudu::Status::OK();
    }
};

