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
#include <stdexcept>
#include <stdio.h>

namespace COMMAND{


string Commandor::execute(const string& cmd) {
    char buffer[128];
    string ret = "";
    FILE* pipe = popen(cmd.c_str(),"r");
    if (!pipe) 
        throw std::runtime_error("popen() failed");
    try {
        while(!feof(pipe)){
            if(fgets(buffer, 128, pipe) != NULL)
                ret += buffer;
        }
    } catch ( ... ) {
        pclose(pipe);
        throw;
    }
    pclose(pipe);
    return ret;
}

}
