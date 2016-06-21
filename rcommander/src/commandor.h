/***************************************************************************
 * 
 * Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
 /**
 * @file src/commandor.h
 * @author brucesz(zhangsong5@jd.com)
 * @date 2016/06/20 21:04:27
 * @version $Revision$ 
 * @brief 
 *  
 **/
#ifndef SRC_COMMANDOR_H
#define SRC_COMMANDOR_H

#endif  // SRC_COMMANDOR_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
#ifndef RCOMMANDER_COMMANDOR
#define RCOMMANDER_COMMANDOR

#include <string>
#include <memory>

using namespace std;


namespace COMMAND {
    class Commandor
    {
        private:
            Commandor(){};
        public:
            static Commandor* getInstance(){
                static Commandor comm;
                return &comm;
            }
            string execute(const string& cmd);
    };
}

#endif
