package AllClientTestSuites;

import SourceSpecificUtility.jsonOperation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.testng.Assert;
import org.testng.annotations.Test;

public class sourceSpecificTS {

    jsonOperation jsonOperationObject = new jsonOperation();

    @Test(priority = 1)
    public void source1Validation(){
        System.out.println("Call to the ntisJsonValidation has been happened");

        SparkConf conf1 = new SparkConf().setAppName("Source1").setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts","true");
        SparkContext sc1 = new SparkContext(conf1);
        SQLContext sqlContext = new SQLContext(sc1);
        SparkSession spark1 = sqlContext.sparkSession();

        Boolean comparisonResult = jsonOperationObject.jsonComparison(spark1);
        Boolean expected = true;

        Assert.assertEquals(expected, comparisonResult, "Json Comparison Fails");
    }

    @Test(priority = 2, enabled = false)
    public void AfterClass(){
        System.out.println("After Class");
    }
}
