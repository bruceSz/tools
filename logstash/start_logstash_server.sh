PATH=/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/logstash/bin
unset JAVA_OPTS
JAVA_HOME=/usr/java/jdk1.8.0_91

usage(){
    echo "$0 -f config_file"
    exit 1
}
if [ $# != 2 ];then
    usage
fi
opt=$1
if [ "$opt" != "-f" ];then
    usage
fi
config_file=$2
if [ ! -f $config_file ];then
    echo "config_file $config_file does not exist."
    usage
    exit 1
fi
/opt/logstash/bin/logstash -f $config_file
