package CommonTestCases;

import CommonUtility.consumer;
import org.testng.annotations.Test;

public class kafkaConsumerMainClass {
    consumer kafkaConsumer = new consumer();

    @Test(priority = 2)
    public void kafkaConsumer(){
        System.out.println("The call has been happened successfully");
        kafkaConsumer.kafkaConsumer();
    }
}
