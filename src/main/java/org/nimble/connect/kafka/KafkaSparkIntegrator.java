package org.nimble.connect.kafka;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Created by neha on 3/4/2017.
 */
public class KafkaSparkIntegrator {

    public static void main (String[] args ){

        Properties props = new Properties();
        props.put("zookeeper.hosts", "localhost");
        props.put("zookeeper.port", "2181");
        props.put("kafka.topic", "test");
        props.put("kafka.consumer.id", "kafka-consumer");
        props.put("zookeeper.consumer.connection", "localhost:2181");
// Optional Properties
        props.put("consumer.fetchsizebytes", "1048576");
        props.put("consumer.fillfreqms", "1000");
        props.put("consumer.num_fetch_to_buffer", "5");

// Spark Properties
        props.put("master" , "local[*]");

        connectToKafka(props);
    }

    public static void connectToKafka(Properties props){

        SparkConf _sparkConf =  new SparkConf()
                .setAppName(KafkaSparkIntegrator.class.getName())
                .setMaster(props.getProperty("master"));
        JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, Durations.seconds(20));
        int numberOfReceivers = 1;

        JavaDStream<MessageAndMetadata<byte[]>> unionStreams = ReceiverLauncher.launch(
                jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());

        JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
                .getPartitionOffset(unionStreams, props);

        unionStreams.foreachRDD(new VoidFunction<JavaRDD<MessageAndMetadata<byte[]>>>() {
            @Override
            public void call(JavaRDD<MessageAndMetadata<byte[]>> rdd) throws Exception {
                List<MessageAndMetadata<byte[]>> rddList = rdd.collect();
                System.out.println(" Number of records in this batch " + rddList.size());
                rddList.forEach( new Consumer<MessageAndMetadata<byte[]>>(){

                    @Override
                    public void accept(MessageAndMetadata<byte[]> messageAndMetadata) {
                        try {
                            String data = new String(messageAndMetadata.getPayload(), "UTF-8");
                            System.out.println("The message is [ " +  data);
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });

        ProcessedOffsetManager.persists(partitonOffset, props);

        try {
            jsc.start();
            jsc.awaitTermination();
        }catch (Exception ex ) {
            jsc.ssc().sc().cancelAllJobs();
            jsc.stop(true, false);
            System.exit(-1);
        }
    }
}
