/***************************************************************************
 * 
 * Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
 /**
 * @file src/commandor.cc
 * @author brucesz(zhangsong5@jd.com)
 * @date 2016/06/20 21:11:21
 * @version $Revision$ 
 * @brief 
 *  
 **/

/* vim: set ts=4 sw=4 sts=4 tw=100 */

#include "commandor.h"
#include <stdlib.h>

namespace COMMAND{

Commandor* Commandor::getInstance(){
    static Commandor  comm;
    return &comm;
}

string execute(const string& cmd) {
    system(cmd.c_str());
    return "nn"; 
}

}
