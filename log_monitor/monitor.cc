/*
 * Author: zhangsong5@jd.com
 * Date: 2016/12/26
 *
 * */

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <ctime>
#include <memory>
#include <vector>
#include <kudu/client/client.h>
#include "kudu/common/partial_row.h"
#include <kudu/util/status.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <iomanip>
//#include <cstdlib>

using namespace std;
using kudu::client::KuduClient;
using kudu::client::KuduTable;
using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduSchema;
using kudu::client::KuduClientBuilder;
using kudu::KuduPartialRow;
using kudu::Status;
using std::tr1::shared_ptr;
using std::unique_ptr;

bool DEBUG=true;

string MASTER_ADDR = "172.22.191.42:7051";
string TAB_NAME = "impala::test.his_impala_sql_fragments";
string g_str;
/*
 * `host` STRING,
 * `time` STRING,
 * `sql_str` STRING
 * */

string getLocalAddr() {
    static string ret = "";
    char name[100];   
    gethostname(name, 100);   
    hostent * ht = gethostbyname(name);   
    if (ht == NULL){     
        std::cerr << "can't get host by name: " << name << std::endl;     
        exit(1);  
    }   
    struct in_addr ** addr_list = (struct in_addr**) ht->h_addr_list;   
    for (int i=0; addr_list[i] != NULL; i++){     
        std::string ip(inet_ntoa(*addr_list[i]));     
        if (ip.find("172")==0){       
            ret = ip;     
        }   
    }
    return ret;
}


string getTimeStr() {
    time_t t = time(0);   // get time now
    struct tm * now = localtime( & t );
    stringstream ss;
    ss  <<(now->tm_year + 1900) 
        << '-'
        << setfill('0') << setw(2) << (now->tm_mon + 1)
        << '-'
        << setfill('0') << setw(2) <<  now->tm_mday
        <<  " "
        << setfill('0') << setw(2) << now->tm_hour
        << ":"
        << setfill('0') << setw(2) << (now->tm_min-20)
        << ":"
        << setfill('0') << setw(2) << (now->tm_sec);
    return ss.str() ;
}


Status GetNextLine(ifstream& ifs, string &row ) {
    string tmp;
    char t_c;
    while(ifs.peek()!= EOF) {
        ifs.get(t_c);
        if (t_c == '\n') {
            row.swap(g_str);
            g_str.swap(tmp);
            return Status::OK();
        }
        g_str.push_back(t_c);

    }
    //ifs.seekg(0, ios::end);
    if (DEBUG) {
        size_t length = ifs.tellg();
        cout << "seek val when peek == EOF" << length << endl;
    }
    return Status::EndOfFile("End of File");
}

Status create_kudu_client(string& master_addr, std::tr1::shared_ptr<KuduClient>* client) {
    std::tr1::shared_ptr<KuduClient> c;
    Status s = KuduClientBuilder().add_master_server_addr(master_addr).Build(&c);
    if (!s.ok()) {
        cout << "create kudu client error:"<< s.message() << endl;
        return s;
    }
    client->swap(c);
    return Status::OK();
}

Status open_kudu_table(std::tr1::shared_ptr<KuduClient> client, string& table_name, std::tr1::shared_ptr<KuduTable>* table) {
    std::tr1::shared_ptr<KuduTable> t;
    Status s = client->OpenTable(table_name, &t);
    if (!s.ok()) {
        cout << "open kudu table error:"<< s.message() << endl;
        return s;
    }
    table->swap(t);
    return Status::OK();
}

void split(const string& s, char delim, vector<string>& ret) {
    stringstream ss;
    ss.str(s);
    string item;
    while(getline(ss, item, delim)) {
        ret.push_back(move(item));
    }
    return;
}



void tailf(const string& file_name, const string& pattern, char split_symbol, int32_t field_num) {
    std::tr1::shared_ptr<KuduClient> client;
    std::tr1::shared_ptr<KuduTable> table;
    std::tr1::shared_ptr<KuduSession> session;
    KuduSchema schema;
    
    create_kudu_client(MASTER_ADDR,&client);
    open_kudu_table(client, TAB_NAME, &table);

    session = client->NewSession();
    session->SetTimeoutMillis(60000);
    session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC);
    

    ifstream ifs;
    ifs.open(file_name, ios::in);
    if (!ifs) {
        cout<<"open erro"<<endl;
        return ;
    }
    string local_host = getLocalAddr();
    string row;
    ifs.seekg(0, ios::end);
    size_t seek = ifs.tellg();
    uint32_t idx=0;
    do{
        /*if (ifs.peek() == EOF) {
            ifs.clear();
            ifs.seekg(seek, ios::beg);
            continue;
        }*/
        Status s_getline = GetNextLine(ifs, row);
        // TODO: there maybe a gap where this prog leek some character. 
        if (!s_getline.ok()) {
            if (s_getline.IsEndOfFile()) {
                ifs.clear();
                ifs.seekg(0, ios::end);
                //seek = ifs.tellg();
                sleep(3);
                continue;
            } else {
                break;
            }
        }
        std::size_t found = row.find(pattern);
        if (found != std::string::npos) {
            vector<string> tokens;
            split(row, split_symbol, tokens);
            if (DEBUG) {
                if (field_num==0 || field_num>tokens.size())
                    cout << row << endl;
                else {
                    cout<<tokens[field_num-1]<<endl;
                }
                    
            } else {
                if (field_num >0 && field_num <=tokens.size()){
                    unique_ptr<KuduInsert> insert(table->NewInsert());
                    KuduPartialRow * row = insert->mutable_row();
                    row->SetStringCopy("host", local_host);
                    string curr_time = getTimeStr();
                    row->SetStringCopy("time", curr_time);
                    string sql_str = tokens[field_num-1];
                    row->SetStringCopy("sql_str", sql_str);
                    session->Apply(insert.release());


                } else {
                    cout << row << endl;
                }
            }
        }
        seek = ifs.tellg();
            idx++;
            if (idx%100000==0)
                cout << "#########################after tellg" << seek << endl;
            //   break;
   }while(1);
   ifs.close();
   session->Flush();
   return;

}

int main(int argc, char* argv[]) {
    if (argc != 5){
        cout<<argv[0]<<" [in_file] [match_pattern] [split_symbol] [field_num:0 is all fields]"<<endl;
        return 1;
    }
    string file_name = argv[1];
    string pattern = argv[2];
    char split_symbol = argv[3][0];
    uint32_t field_num = atoi(argv[4]);
    tailf(file_name, pattern, split_symbol, field_num);
    return 0;
}
