import sys
import commands
import re

def Usage():
    print sys.argv[0], " src_impala_tab target_kudu_master_addr"
    sys.exit(1)


def execute(cmd):
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        print "Execute command error:",cmd 
        print "Error Code:", status
        print "Error output:",output
        Usage()
    else:
        return output

    
    

def migrate_tab(impala_tab, bak_impala_tab ,target_kudu_master_addr, dis_key_names, tablets_num):
    cmd = 'sh impala-shell.sh  -q "use jingo_recommendation;show create table '+\
                        impala_tab+\
                        '" --print_raw 2>&1|grep CREATE'
    create_sql = execute(cmd)
    create_sql = create_sql.strip("[[").strip("]]").replace("\\n", "")\
                        .replace(impala_tab,bak_impala_tab)\
                        .replace("TBLPROPERTIES","DISTRIBUTE BY hash ("+dis_key_names+") into "+tablets_num+" buckets TBLPROPERTIES" )
    create_sql = re.sub("'kudu.master_addresses'='\S+'", "'kudu.master_addresses'='"+target_kudu_master_addr+"'", create_sql)
    cmd = "sh impala-shell.sh -q " + create_sql
    create_ret = execute(cmd)
    cmd = 'sh impala-shell.sh -q ' + '"use jingo_recommendation; insert into ' + bak_impala_tab + ' select * from ' + impala_tab+'"'
    insert_ret = execute(cmd)


if __name__ == "__main__":
    if len(sys.argv) != 6:
        Usage()
    else:
        impala_tab = sys.argv[1]
        bak_impala_tab = sys.argv[2]
        target_kudu_master_addr = sys.argv[3]
        key_names = sys.argv[4]
        tablets_num = sys.argv[5]
        migrate_tab(impala_tab, bak_impala_tab, target_kudu_master_addr, key_names, tablets_num)
