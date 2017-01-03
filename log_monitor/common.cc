/*
 * Author: zhangsong5@jd.com
 * Date: 2016/12/26
 *
 * */

#include <iostream>
#include <sstream>
#include <unistd.h>
#include <iomanip>
#include <netdb.h>
#include <arpa/inet.h>
#include "common.h"

using kudu::Status;
using std::string;
using std::cout;
using std::endl;
using std::setfill;
using std::setw;
using std::vector;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduTable;
using std::stringstream;

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
        << setfill('0') << setw(2) << (now->tm_min)
        << ":"
        << setfill('0') << setw(2) << (now->tm_sec);
    return ss.str() ;
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
