package cn.spark.study.streaming;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
//import org.apache.spark.streaming.kafka.KafkaUtils;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * 基于Kafka Direct方式的实时wordcount程序
 * @author Administrator
 *
 */
public class KafkaDirectWordCount {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("KafkaDirectWordCount");  
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		// 首先，要创建一份kafka参数map
//		Map<String, String> kafkaParams = new HashMap<String, String>();
//		kafkaParams.put("metadata.broker.list",
//				"node-etl-01:9092,node-etl-02:9092,node-etl-03:9092");
//		Collection<String> topics = Collections.singletonList("WordCount");

		// 然后，要创建一个set，里面放入，你要读取的topic
		// 这个，就是我们所说的，它自己给你做的很好，可以并行读取多个topic
//		Set<String> topics = new HashSet<String>();
//		topics.add("WordCount");
//
//		Iterator<String> topic = topics.iterator();
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "node-etl-01:9092,node-etl-02:9092,node-etl-03:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("topicA", "topicB");

		JavaInputDStream<ConsumerRecord<String, String>> lines =
				KafkaUtils.createDirectStream(
						jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
				);

//		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
//				jssc,
//				LocationStrategies.PreferConsistent(),
//				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//		);
				// 创建输入DStream
//		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
//				jssc,
//				String.class,
//				String.class,
//				StringDecoder.class,
//				StringDecoder.class,
//				kafkaParams,
//				topics);
		
		// 执行wordcount操作
//		JavaDStream<String> words = lines.flatMap(
//
//				new FlatMapFunction<Tuple2<String,String>, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Iterator<String> call(Tuple2<String, String> tuple)
//							throws Exception {
//						return (Iterator<String>) Arrays.asList(tuple._2.split(" "));
//					}
//
//				});
//
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<String> call(ConsumerRecord<String, String> tuple) throws Exception {

				return (Iterator<String>) Arrays.asList(tuple.value().split(" "));
			}
		});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				
				new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String word) throws Exception {
						return new Tuple2<String, Integer>(word, 1);
					}
					
				});
		
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				
				new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		wordCounts.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
	
}
