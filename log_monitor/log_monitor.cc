/*
 * Author: zhangsong5@jd.com
 * Date: 2016/12/27
 *
 * 
 */

#include <iostream>
#include "kudu/common/partial_row.h"
#include "file_utils.h"
#include "common.h"

using namespace std;
using namespace log::monitor;
using kudu::client::KuduClient;
using kudu::client::KuduTable;
using kudu::client::KuduSession;
using kudu::client::KuduSchema;
using kudu::client::KuduInsert;
using kudu::KuduPartialRow;

string MASTER_ADDR = "172.22.191.42:7051";
string TAB_NAME = "impala::test.his_impala_sql_fragments";
bool DEBUG=false;


bool extract_target_field(string& row, const string& pattern, char split_symbol, int32_t field_num, string* field_val) {
    std::size_t found = row.find(pattern);
    if (found != std::string::npos) {
        //cout << row << endl;
        vector<string> tokens;
        split(row, split_symbol, tokens);
            if (field_num<0 || field_num>=tokens.size())
                return false;
            else {
                *field_val = tokens[field_num];
                return true;
            }
    }
    return false;
}

void record_cache_sql(const string& file_name, const string& pattern, char split_symbol, int32_t field_num) {
    std::tr1::shared_ptr<KuduClient> client;
    std::tr1::shared_ptr<KuduTable> table;
    std::tr1::shared_ptr<KuduSession> session;
    KuduSchema schema;
    
    create_kudu_client(MASTER_ADDR,&client);
    open_kudu_table(client, TAB_NAME, &table);

    session = client->NewSession();
    session->SetTimeoutMillis(60000);
    session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC);
    string local_host = getLocalAddr();
    string row;
    string field_val;
    TextFile tf(file_name);
    tf.Init();
    int idx = 0;
    TextFileIncrementalIterator iter = tf.begin();
    while (iter != tf.end()) {
        row = *iter;
        if (!extract_target_field(row, pattern, split_symbol, field_num, &field_val)) {
            //continue;
        } else {
            if (DEBUG) {
                cout << "field val: " << field_val << endl;
            } else {
                unique_ptr<KuduInsert> insert(table->NewInsert());
                KuduPartialRow * row = insert->mutable_row();
                row->SetStringCopy("host", local_host);
                string curr_time = getTimeStr();
                row->SetStringCopy("time", curr_time);
                string sql_str = field_val;
                row->SetStringCopy("sql_str", sql_str);
                session->Apply(insert.release());
            }
            idx++;
            
        }
        /*unique_ptr<KuduInsert> insert(table->NewInsert());
        KuduPartialRow * row = insert->mutable_row();
        row->SetStringCopy("host", local_host);
        string curr_time = getTimeStr();
        row->SetStringCopy("time", curr_time);
        string sql_str = field_val;
        row->SetStringCopy("sql_str", sql_str);
        session->Apply(insert.release());*/

        //cout << *iter << endl;
        iter++;
    }

    session->Flush();


}

int main(int argc, char** argv) {
	if (argc != 5){
        cout<<argv[0]<<" [in_file] [match_pattern] [split_symbol] [field_num:0 is all fields]"<<endl;
        return 1;
    }
    
    string file_name = argv[1];
    string pattern = argv[2];
    char split_symbol = argv[3][0];
    uint32_t field_num = atoi(argv[4]);
    record_cache_sql(file_name, pattern, split_symbol, field_num);
    
}
