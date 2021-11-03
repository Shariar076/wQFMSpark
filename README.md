# wQFMSpark
hadoop namenode: http://localhost:9870/   
spark webUI: http://localhost:8080

##### Hadooop:
core-site.xml
```
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
```

hdfs-site.xml
```
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
```

hadoop-env.sh
```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```