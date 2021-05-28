package SourceSpecificUtility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class jsonOperation {
    public Boolean jsonComparison(SparkSession spark1){

        ObjectMapper mapper = new ObjectMapper();
        Dataset<Row> sourceSpecificDF = spark1.read().option("header","true").csv("C:\\Users\\PrabhuManohari\\OneDrive\\Desktop\\SparkStreamingKafkaTestNGAutomation\\ConsumerFile\\").toDF();
        Dataset<Row> referenceData = spark1.read().text("C:\\Users\\PrabhuManohari\\OneDrive\\Desktop\\SparkStreamingKafkaTestNGAutomation\\src\\main\\resources\\SourceReferenceFile.txt");

        List<StructField> fields = new ArrayList<>();
        StructField field = DataTypes.createStructField("JSON", DataTypes.StringType, true);
        StructField count = DataTypes.createStructField("OCCURRENCE", DataTypes.StringType, true);
        fields.add(field);
        fields.add(count);
        StructType schema = DataTypes.createStructType(fields);

        //Comparing the existing json with reference data

        Object[] arrayOfReferenceRecords = referenceData.toJavaRDD().collect().toArray();
        Map<String, Integer> OccurrenceMap = new HashMap<>();
        JavaRDD<Row> resultRDD = sourceSpecificDF.toJavaRDD().map(x->{
            int occurrenceCount = 0;
            for(int i=0; i< arrayOfReferenceRecords.length ; i++){
                JsonNode consumedJson = mapper.readTree(x.toString());
                JsonNode referenceJson = mapper.readTree(arrayOfReferenceRecords[i].toString());
                if(consumedJson.equals(referenceJson)){
                    OccurrenceMap.put(x.toString(), occurrenceCount+1);
                }
            }

            if(OccurrenceMap.containsKey(x.toString())){
                for(Map.Entry<String, Integer> m : OccurrenceMap.entrySet()){
                    if(x.toString().equals(m.getKey()))
                        return RowFactory.create(x.toString(), String.valueOf(m.getValue()));
                }
            }
            return RowFactory.create(x.toString(), String.valueOf(0));
        });

        SQLContext sqlContext = SQLContext.getOrCreate(resultRDD.context());
        Dataset<Row> dfComparison = sqlContext.createDataFrame(resultRDD, schema);
        dfComparison.show(false);

        dfComparison.filter("OCCURRENCE = 1").show(false);
        if(dfComparison.filter("OCCURRENCE = 1").count() == referenceData.toJavaRDD().count())
            return true;
        else
            return  false;
    }
}
