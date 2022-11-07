### connect ###
# ssh -i C://Users//gh0st//Documents//8415//vm1_key.pem azureuser@20.55.2.103

sudo apt update
sudo apt-get -y install default-jdk default-jre
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
tar -xvzf hadoop-3.3.1.tar.gz
sudo mv hadoop-3.3.1 /usr/local/hadoop
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:/usr/local/hadoop/sbin:/usr/local/hadoop/bin