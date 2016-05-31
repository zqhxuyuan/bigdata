package com.hadoop;

/**
 * Created by zhengqh on 15/11/8.
 */
import java.io.FileNotFoundException;
import java.net.URI;

import org.apache.cassandra.hadoop.BulkOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 *
 * @author Rahul.Chauhan
 *
 * http://www.solveitinjava.com/2015/05/bulkload-to-cassandra-with-hadoop.html
 *
 * 读取CSV文件, 采用MR直接写到Cassandra集群
 *
CREATE KEYSPACE cass_word_count WITH replication = {
'class': 'SimpleStrategy',
'replication_factor': '2'
};

CREATE TABLE word_count (
word text,
count int,
PRIMARY KEY (word)
) WITH COMPACT STORAGE;
 */
public class WordCountDriver extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(WordCountDriver.class);
    String inputFilePath;

    public static void main(String[] args) {
        if(args == null || args.length == 0 || "".equals(args[0].trim())){
            LOG.info("Please provide file path as input.");
        }
        LOG.info("WordCounter uploader job started");
        try {
            new WordCountDriver().execute(args);
        }catch (Exception e){
            LOG.error("An error occurred in WordCounter Uploader Job", e);
        }
        LOG.info("WordCounter uploader job completed");
    }

    protected void execute(String[] args) throws Exception{
        Configuration conf = new Configuration();
        int ret = ToolRunner.run(conf, this, args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        inputFilePath = args[0];
        int ret = 0;
        if (isFileExist(inputFilePath, conf)) {
            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCountDriver.class);
            setJobParameters(job);
            ret = job.waitForCompletion(true) ? 0 : 1;
        }else{
            LOG.info("###############No files to process file upload####################");
        }
        return ret;
    }

    protected void setJobParameters(Job job) throws Exception {

        Configuration conf = job.getConfiguration();
        //conf.set("mapreduce.output.bulkoutputformat.streamthrottlembits", "400");
        conf.set("mapreduce.output.bulkoutputformat.buffersize", "160");

        //Cassandra本身就提供了Bulk的输出格式
        job.setOutputFormatClass(BulkOutputFormat.class);

        job.setJobName("Word Count Uploader");
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //Map的输出是(word,1) TODO: How about TTL?
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set the keyspace and column family for the output of this job
        ConfigHelper.setOutputColumnFamily(conf, "cassa_word_count", "word_count");
        ConfigHelper.setOutputRpcPort(conf, "9160");
        ConfigHelper.setOutputInitialAddress(conf, "127.0.0.1");
        ConfigHelper.setOutputPartitioner(conf, "org.apache.cassandra.dht.Murmur3Partitioner");

        FileInputFormat.setInputPaths(job, new Path(inputFilePath));
    }

    private boolean isFileExist(String path, Configuration conf) throws Exception {
        boolean exists = false;
        FileSystem fs = FileSystem.get(new URI(path), conf);
        Path dirPath = new Path(path);
        if(fs.exists(dirPath)) {
            LOG.info("Input directory exists : "+dirPath);
            try {
                FileStatus[] fileStatuses = fs.listStatus(dirPath);
                for (FileStatus fileStatus : fileStatuses) {
                    if (fileStatus.isFile()) {
                        exists = true;
                        break;
                    }
                }
            }catch (FileNotFoundException e){
                //Eating up this exception.
            }
        }else{
            LOG.info("Input directory does not exists : " + dirPath);
        }
        return exists;
    }


}