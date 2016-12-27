#include <string>
#include <iostream>

#include "kudu/client/client.h"

#include "util.h"

using namespace std;

using kudu::KuduPartialRow;
using kudu::client::KuduInsert;
using kudu::client::KuduSession;

void TestInsertZero() {
    string master_addr = "172.22.191.41:7051";
    string table_name = "sku_list_test"; 
    auto client = create_client(master_addr);
    auto table = openTable(client, table_name);
    std::tr1::shared_ptr<KuduSession> session = client->NewSession();
    
    session->SetTimeoutMillis(100000);
    session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND);
    unique_ptr<KuduInsert> insert(table->NewInsert());
    KuduPartialRow * row = insert->mutable_row();
    string val = "0";
    
    row->SetInt64(0,stringToInt64(val));
    std::cout << "about to apply:" << insert->ToString() << ":endl" << std::endl; 
    KUDU_EXIT_NOT_OK(session->Apply(insert.release()),"error apply");
    session->Flush();
}

int main() {
    TestInsertZero();

}
