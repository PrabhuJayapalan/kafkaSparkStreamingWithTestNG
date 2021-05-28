package Configuration;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class kafkaConfig implements Serializable {
    protected final Properties properties = new Properties();
    private static kafkaConfig globalConfig;

    public static kafkaConfig getConfig(){
        if(globalConfig == null){
            globalConfig = new kafkaConfig();
        }
        return globalConfig;
    }

    private kafkaConfig(){
        String mainConfFile = System.getProperty("config.location");
        if(mainConfFile == null){
            throw new RuntimeException("Application config File not found");
        }

        try(FileReader input = new FileReader(mainConfFile)){
            properties.load(input);
        } catch(IOException ex){
            throw new RuntimeException("Unable to load/process config file", ex);
        }
    }

    public String getProperty(final String key){
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        String val = (String) this.properties.get(key);

        if (val == null){
            throw new RuntimeException(String.format("Property %s not found", key));
        }

        /*fill place holders in the property value with System properties */
        for(String sysPropKey : System.getProperties().stringPropertyNames()){
            String sysPropval = System.getProperty(sysPropKey);
            val = val.replaceAll("\\$\\{" + sysPropKey + "}", sysPropval);

        }

        return val;
    }
}
