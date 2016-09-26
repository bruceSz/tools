#include <iostream>
#include <string>
#include <memory>
#include <fstream>
#include <sstream>
#include <memory>

#include <stdlib.h>
#include <ctime>

#include "kudu/client/client.h"
#include "kudu/common/partial_row.h"
#include "kudu/util/status.h"
#include "kudu/util/slice.h"

#include "gflags/gflags.h"

#include "util.h"

using namespace kudu::client;
using std::string;
using std::fstream;
using std::stringstream;
using std::cerr;
using std::endl;
using std::cout;
using std::tr1::shared_ptr;
using kudu::KuduPartialRow;
using kudu::Slice;
using kudu::Status;
using std::unique_ptr;



DEFINE_int32(kudu_session_timeout, 60000,
            "must set session timeout ,or kudu client will crash.");
DEFINE_string(kudu_sample_log_dir, "/export/ldb", 
            "kudu sample log dir");
DEFINE_string(kudu_sample_log_name_suffix, "_kudu_log", 
            "kudu sample log suffix, sample log should be named as ip_kudu_log");
DEFINE_string(kudu_sample_log_table_name, "sampled_log1",
            "name of table which stores sampled_log");
DEFINE_string(master_addresses, "localhost",
              "Comma-separated list of Kudu Master server addresses");



void Usage(char** argv){
  std::cout << argv[0] << ": -master_addresses [host:port] action" << std::endl;
  std::cout << "\t" << "action:" << std::endl;
  std::cout << "\t\t" << "list_tables" << std::endl;
  std::cout << "\t\t" << "collect_log" << std::endl; 
  std::cout << "\t\t" << "test_insert: insert test into 172.22.191.41" << std::endl; 
  std::cout << "\t\t" << "print_tab_info [tab_name]" << std::endl;
  std::cout << "\t\t" << "rename_table [tab_name new_tab_name]" << std::endl;

  exit(1);
}

void listTable(std::string master_addr){
  std::tr1::shared_ptr<kudu::client::KuduClient> client;
  KuduClientBuilder().add_master_server_addr(master_addr).Build(&client);
  std::vector<KuduTabletServer*> tablet_servers;
  kudu::Status s = client->ListTabletServers(&tablet_servers);
  if (!s.ok()) {
    std::cout << "it crashed! or cannot connect to master" << std::endl;
  }
  for (auto & t_servers: tablet_servers) {
    std::cout << "hostname: " <<t_servers -> hostname() ;
    std::cout << "uuid: " << t_servers -> uuid() << std::endl;
  }
}

string getSampleLog(){
  string full_log_name = getLocalIp() + FLAGS_kudu_sample_log_name_suffix;
  string log_path = FLAGS_kudu_sample_log_dir + "/" + full_log_name;
  fstream log_file(log_path);
  stringstream ss;
  if(!log_file.is_open()){
    cerr << "can't open file :" << log_path << endl;
    exit(1);
  }
  ss << log_file.rdbuf();
  log_file.close();
  return ss.str();

}

string getTimeVal(){
  auto t = time(nullptr);
  auto* tm = std::localtime(&t);
  char buf[15];
  memset(buf,15,'\0');
  strftime(buf, 15, "%Y%m%d%H%M%S", tm);
  //long timeVal = atoi(buf);
  return string(buf);
}

unique_ptr<KuduInsert> BuildInsertRow(shared_ptr<KuduTable> table, const string& log_content){
  unique_ptr<KuduInsert> insert(table->NewInsert());
  KuduPartialRow * row = insert->mutable_row();

  //row->SetInt64(0, getTimeVal());
  row->SetStringCopy(0, Slice(getTimeVal()));
  row->SetStringCopy(1, Slice(getLocalIp()));
  row->SetStringCopy(2, Slice(log_content));
  return insert;
} 

void doCollectLog(std::string master_addr ) {
  string sample_log_content = getSampleLog(); 

  string sample_log = getSampleLog();
  if (sample_log.length()==0){
    cerr << "no sample log there." << endl;
    exit(1);

  }
  auto client = create_client(master_addr);
  auto table = openTable(client, FLAGS_kudu_sample_log_table_name);

  unique_ptr<KuduInsert> insert = BuildInsertRow(table, sample_log);
  shared_ptr<KuduSession> session = client->NewSession();
  session->SetTimeoutMillis(FLAGS_kudu_session_timeout);
  session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC);
  Status s = session->Apply(insert.release());
  if (!s.ok()) {
    cerr << "apply insert into kudu table failed."
        << sample_log << endl;
    exit(1);
  }
}

void printTabSchemaInfo(std::string master_addr, std::string table_name) {
    auto client = create_client(master_addr);
    KuduPartitionSchema kps;
    client->GetTablePartitionSchema(table_name, &kps);
    std::cout << kps.PartitionSchemaDebugString() << std::endl;
    //KUDU_EXIT_NOT_OK(client->GetTablePartitionSchema(table_name, &schema), "get table schema error");
}

void renameTable(std::string master_addr, const std::string& tab_name, const std::string& new_tab_name) {
    auto client = create_client(master_addr);
    auto table_alter = client->NewTableAlterer(tab_name);
    table_alter->RenameTo(new_tab_name);
    KUDU_EXIT_NOT_OK(table_alter->Alter(),"alter table name failed");
    
}


void dispatch_command(int argc, char** argv){
  //TODO:
  if (argc < 2){
    Usage(argv);
  }
  std::string command = argv[1];
  std::cout << "Master addr: " << FLAGS_master_addresses << std::endl;
  if (command == "list_tables") {
     listTable(FLAGS_master_addresses);
  } else if (command == "log_collect") {
    std::cout << "This action is deprecated." << std::endl;
    exit(1);
	//doCollectLog(FLAGS_master_addresses);
  } else if (command == "test_insert") {
    //doCollectLog("172.22.191.41:7051");
    std::cout << "This action is deprecated." << std::endl;
    exit(1);
    //doCollectLog(FLAGS_master_addresses);
  } else if (command == "test_scan") {
    std::cout << "This action is deprecated." << std::endl;
    exit(1);

  } else if (command == "print_tab_info") {
    if (argc < 3) {
        Usage(argv);
    }
    std::string table_name = argv[2];
    printTabSchemaInfo(FLAGS_master_addresses, table_name);
  } else if (command == "rename_table") {
    if (argc < 4) {
        Usage(argv);
    }
    std::string table_name = argv[2];
    std::string new_table_name = argv[3];
    renameTable(FLAGS_master_addresses, table_name, new_table_name);
    
  } else {
    std::cout << "unknown action." << std::endl; 
    Usage(argv);
  }
}

int main(int argc, char** argv){
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  dispatch_command(argc, argv);
}
