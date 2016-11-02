#include <string>
#include "text_file.h"

using namespace std;

void TestReadFile() {
    string file_name = "test.txt";
    CSVFile csv_f(file_name);
    for(TextFileIterator it=csv_f.begin(); it != csv_f.end(); it++) {
        auto vals = *it;
        for(auto v: vals) {
            cout << "[" << v << "]" << endl;
        }
    }
}

int main() {
    TestReadFile();
}
