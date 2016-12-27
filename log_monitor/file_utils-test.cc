/*
 * Author: zhangsong5@jd.com
 * Date: 2016/12/26
 *
 * 
 */

#include "file_utils.h"

int main() {
	if (argc != 2){
        cout<<argv[0]<<" [in_file] "<<endl;
        return 1;
    }
    string file_name = argv[1];
    TextFile tf(file_name);
    TextFileIncrementalIterator iter = tf.begin();
    while (iter != tf.end()) {
    	cout << *iter << endl;
    }
}
