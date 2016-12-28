/*
 * Author: zhangsong5@jd.com
 * Date: 2016/12/26
 *
 * */


#ifndef LOG_MONITOR_COMMON_H_
#define LOG_MONITOR_COMMON_H_

#include <string>
#include <memory>
#include <vector>
#include <kudu/util/status.h>
#include <kudu/client/client.h>

kudu::Status create_kudu_client(std::string& master_addr, std::tr1::shared_ptr<kudu::client::KuduClient>* client);

kudu::Status open_kudu_table(std::tr1::shared_ptr<kudu::client::KuduClient> client, std::string& table_name,
						 std::tr1::shared_ptr<kudu::client::KuduTable>* table);

void split(const std::string& s, char delim, std::vector<std::string>& ret);
std::string getTimeStr();

#endif
