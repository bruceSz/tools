#include <string>
#include <iostream>
#include <fstream>

#include <boost/tokenizer.hpp>

#include "util.h"

typedef boost::tokenizer<boost::char_separator<char>> Tokenizer;

void TestStringConversion(){
    std::string file_n = "test.txt";
    std::fstream f(file_n);
    std::string line;
    std::vector<std::string> vec;
    boost::char_separator<char> sep{","};

    while (std::getline(f, line)) {
        std::cout << "dealing with" << line << std::endl;
        Tokenizer tok{line, sep};
        vec.assign(tok.begin(), tok.end());
        for (int i = 0; i< vec.size();i++) {
            std::cout << "sperated:" << vec[i] << std::endl;
            std::cout << stringToInt64(vec[i]) << std::endl;
        }
    }
    
}

int main() {
    TestStringConversion();
}
