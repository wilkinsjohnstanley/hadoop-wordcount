# Hadoop WordCount (Java MapReduce)

A minimal single-node Hadoop pipeline: text file → HDFS → MapReduce word count → HDFS output.

## The Big Picture
- This is the same architecture used for real batch jobs at scale.
- Our one-line **sample.txt** on 1 datanode would be terabytes of log files split across hundreds of datanodes.
- HDFS splits big files into blocks (default 128MB each) distributed across nodes. Each mapper processes one block, in parallel, on the node that already holds that data.
- The MapReduce logic is the same whether it's 40 bytes or 40 terabytes. Hadoop splits work amongst the nodes, data is shuffled, and bingo! The job has read the entire input, processed it, and the output has been written when the program exits out.
- The combiner we use matters **quite a bit more** at scale. Without it, every single word gets shuffled across the network individually; with millions of records, that's a massive bandwidth difference.
- The Raw MapReduce we see in this project is the low-level foundation. In practice today, most people don't hand-write Mapper/Reducer classes. Instead, they use high-level tools such as Hive, Spark, and Pig, which compile down to similar distributed batch jobs but with SQL-like or more expressive APIs. Understanding MapReduce is still valuable because it's the mental model underneath those tools. 

## Environment

- Windows + Docker Desktop (WSL2 backend)
- Docker image: `apache/hadoop:3` (Hadoop 3.3.6, CentOS 7 base)
- JDK installed inside the container for compilation: `java-1.8.0-openjdk-devel`

## Setup

1. Pull the image and start a container:
   ```
   docker pull apache/hadoop:3
   docker run -it --name hadoop-wc apache/hadoop:3 /bin/bash
   ```

2. Configure pseudo-distributed HDFS. Inside the container, write to `/opt/hadoop/etc/hadoop/`:

   `core-site.xml`:
   ```xml
   <configuration>
     <property>
       <name>fs.defaultFS</name>
       <value>hdfs://localhost:9000</value>
     </property>
   </configuration>
   ```

   `hdfs-site.xml`:
   ```xml
   <configuration>
     <property>
       <name>dfs.replication</name>
       <value>1</value>
     </property>
   </configuration>
   ```

3. Format HDFS and start daemons directly (no SSH needed for a single-node container):
   ```
   hdfs namenode -format -force
   hdfs --daemon start namenode
   hdfs --daemon start datanode
   yarn --daemon start resourcemanager
   yarn --daemon start nodemanager
   ```

4. Install a JDK for compiling (the image ships JRE-only). Requires root:
   ```
   docker exec -u root -it hadoop-wc /bin/bash
   sed -i 's|mirrorlist=|#mirrorlist=|g' /etc/yum.repos.d/CentOS-*.repo
   sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*.repo
   yum install -y java-1.8.0-openjdk-devel
   ```
   (CentOS 7 is EOL, so mirrors are redirected to `vault.centos.org`.)

## Build

```
javac -classpath $(hadoop classpath) -d . WordCount.java
jar -cvf wordcount.jar -C . .
```

## Run

```
echo "hello world hello hadoop world of hadoop" > sample.txt
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -put sample.txt /user/hadoop/input
hadoop jar wordcount.jar WordCount /user/hadoop/input /user/hadoop/output
hdfs dfs -cat /user/hadoop/output/part-r-00000
```

### Output

```
hadoop  2
hello   2
of      1
world   2
```

## Notes / gotchas hit during setup

- `$HADOOP_HOME` is not set as an env var in this image — use the literal path `/opt/hadoop` (or `$HADOOP_CONF_DIR` for config, which is set) when editing config files.
- The container's default `fs.defaultFS` is `file:///`, which fails immediately for HDFS daemons — must be overridden in `core-site.xml` before formatting.
- `start-dfs.sh`/`start-yarn.sh` expect SSH; for a single container, start daemons individually with `hdfs --daemon start ...` / `yarn --daemon start ...` instead.
- Files created inside the container don't exist on the host — use `docker cp <container>:<path> <host-path>` to extract them.
