package CommonUtility;

import Configuration.kafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.File;
import java.util.*;

public class consumer {
    kafkaConfig kafkaconfig = kafkaConfig.getConfig();

    public void kafkaConsumer(){
        SparkConf conf = new SparkConf().setAppName("KafkaConsumer").setMaster("local[*]");
        conf.set("spark.driver.allowMultipleContexts","true");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(sc);
        SparkSession spark1 = sqlContext.sparkSession();

        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaConfig.getConfig().getProperty("bootstrap.servers"));
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaConfig.getConfig().getProperty("group.id"));
        kafkaParams.put("client.id", kafkaConfig.getConfig().getProperty("client.id"));
        kafkaParams.put("auto.offset.reset", kafkaConfig.getConfig().getProperty("auto.offset.reset"));
        kafkaParams.put("enable.auto.commit", kafkaConfig.getConfig().getProperty("enable.auto.commit"));
        kafkaParams.put("security.protocol", kafkaConfig.getConfig().getProperty("security.protocol"));
        kafkaParams.put("sasl.kerberos.service.name", kafkaConfig.getConfig().getProperty("sasl.kerberos.service.name"));
        kafkaParams.put("sasl.mechanism", kafkaConfig.getConfig().getProperty("sasl.mechanism"));
        kafkaParams.put("ssl.truststore.location", kafkaConfig.getConfig().getProperty("ssl.truststore.location"));
        kafkaParams.put("ssl.truststore.password", kafkaConfig.getConfig().getProperty("ssl.truststore.password"));
        kafkaParams.put("max.partition.fetch.bytes", kafkaConfig.getConfig().getProperty("max.partition.fetch.bytes"));
        kafkaParams.put("auto.commit.interval.ms", kafkaConfig.getConfig().getProperty("auto.commit.interval.ms"));


        Collection<String> topics = Arrays.asList("Hello_Topic1","Hello_Topic2");
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(40));
        ConsumerStrategy<String, String> consumerStrategy = ConsumerStrategies.Subscribe(topics, kafkaParams);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
          jssc, LocationStrategies.PreferConsistent(), consumerStrategy);
        System.out.println("Stream Count ==> "+stream.count());

        final  int[] cyclecount = {0};

        // Streaming Operation
        stream.foreachRDD( rdd-> {
            System.out.println("Going to print the rdd count");
           SQLContext sqlContext1 = SQLContext.getOrCreate(rdd.context());
           JavaRDD<String> topicValueStringsRDD = rdd.map(record -> ((String)record.value()));

           //Converting the string of rdd into row of rdd
            JavaRDD<Row> topicValueRDD = topicValueStringsRDD.map(x -> RowFactory.create(x));

            //Creating the Schema for dataFrame
            List<StructField> fields = new ArrayList<>();
            StructField field = DataTypes.createStructField("JSON", DataTypes.StringType, true);
            fields.add(field);
            StructType schema = DataTypes.createStructType(fields);

            //now Creating the DataFrame;
            cyclecount[0] = cyclecount[0] + 1;
            Dataset<Row> df = sqlContext.createDataFrame(topicValueRDD, schema);
            File f = new File("C:\\Users\\PrabhuManohari\\OneDrive\\Desktop\\SparkStreamingKafkaTestNGAutomation\\ConsumerFile\\");
            File[] listOfFiles = f.listFiles();

            if(listOfFiles.length > 0)
                df.write().mode(SaveMode.Append).option("header","true").csv("C:\\Users\\PrabhuManohari\\OneDrive\\Desktop\\SparkStreamingKafkaTestNGAutomation\\ConsumerFile\\");
            else
                df.write().mode(SaveMode.Overwrite).option("header","true").csv("C:\\Users\\PrabhuManohari\\OneDrive\\Desktop\\SparkStreamingKafkaTestNGAutomation\\ConsumerFile\\");

            if(listOfFiles.length > 0)
            {
                if(spark1.read().option("header","true").csv("C:\\Users\\PrabhuManohari\\OneDrive\\Desktop\\SparkStreamingKafkaTestNGAutomation\\ConsumerFile\\").toDF().count() > 0 && cyclecount[0] > 1) {
                sc.cancelAllJobs();;
                sc.stop();
                jssc.close();
                }
            }
        });


    }
}
