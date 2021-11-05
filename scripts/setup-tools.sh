sudo apt install maven
sudo apt install openjdk-8-jdk

wget https://dlcdn.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz

scp /home/himel/.local/bin/triplets.soda2103 kab076@ms0923.utah.cloudlab.us:~/
sudo mv triplets.soda2103 /usr/local/bin/

mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file -Dfile=./lib/phylonet/main.jar -DgroupId=phylonet -DartifactId=phylonet -Dversion=2.4 -Dpackaging=jar -DlocalRepositoryPath=./lib
mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file -Dfile=./lib/phylonet/main.jar -DgroupId=phylonet -DartifactId=phylonet -Dversion=2.4 -Dpackaging=jar
mvn clean package
~/.spark/bin/spark-submit target/wQFMSpark-1.0-SNAPSHOT-jar-with-dependencies.jar