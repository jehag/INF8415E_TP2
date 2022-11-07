### connect ###
# ssh -i C://Users//gh0st//Documents//8415//vm1_key.pem azureuser@20.55.2.103

sudo apt update
sudo apt-get -y install default-jdk default-jre
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
tar -xvzf hadoop-3.3.1.tar.gz
rm hadoop-3.3.1.tar.gz
sudo mv hadoop-3.3.1 /usr/local/hadoop
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:/usr/local/hadoop/sbin:/usr/local/hadoop/bin

hdfs dfs -mkdir input
wget --header="Accept-encoding: *" -O pg4300.txt.gz https://www.gutenberg.org/cache/epub/4300/pg4300.txt
gzip -d pg4300.txt.gz
hdfs dfs -copyFromLocal pg4300.txt input
rm pg4300.txt

time hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.1.jar wordcount input output 
hdfs dfs -rm -r output/

# real    0m4.291s
# user    0m7.756s
# sys     0m0.390s

# real    0m5.442s
# user    0m8.571s
# sys     0m0.281s

# real    0m5.422s
# user    0m8.474s
# sys     0m0.286s
