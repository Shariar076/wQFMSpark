~/.hadoop/bin/hdfs namenode -format
~/.hadoop/sbin/start-dfs.sh
~/.hadoop/bin/hdfs dfs -mkdir -p /user/$USER
~/.hadoop/bin/hdfs dfs -mkdir input
~/.hadoop/bin/hdfs dfs -mkdir output
~/.hadoop/bin/hdfs dfs -mkdir exception
~/.hadoop/bin/hdfs dfs -put ../input/* input