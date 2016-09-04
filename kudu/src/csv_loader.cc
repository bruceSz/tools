#include <iostream>
#include <string>

#include "gflags/gflags.h"

#include "kudu/common/partial_row.h"
#include "kudu/util/status.h"


#include "util.h"
#include "text_file.h"


using namespace std;
using kudu::Status;
using kudu::KuduPartialRow;
using kudu::client::KuduSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSession;

    

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



class CSVLoader {
  public:
    CSVLoader(string master_addr, string csv_file):csv_file_(csv_file),
                master_addr_(master_addr){}
    Status load(string tab_name); 
    CSVLoader(const CSVLoader&) = delete;
    CSVLoader& operator=(const CSVLoader& ) = delete;
  private:
    Status doInsert(KuduPartialRow* row, KuduSchema* schema, int idx, string val);
    CSVFile csv_file_;
    string master_addr_;
};

Status CSVLoader::doInsert(KuduPartialRow* row, KuduSchema* schema, int idx, string val) {
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

}
