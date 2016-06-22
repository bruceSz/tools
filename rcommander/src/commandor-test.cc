/***************************************************************************
 * 
 * Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
 /**
 * @file src/commandor-test.cc
 * @author brucesz(zhangsong5@jd.com)
 * @date 2016/06/20 21:46:53
 * @version $Revision$ 
 * @brief 
 *  
 **/

/* vim: set ts=4 sw=4 sts=4 tw=100 */
#include "commandor.h"
#include <iostream>

using namespace std;

int main() {
    COMMAND::Commandor* comm = COMMAND::Commandor::getInstance();
    string ret = comm->execute("ls xxx ");
    cout << ret ;
}

