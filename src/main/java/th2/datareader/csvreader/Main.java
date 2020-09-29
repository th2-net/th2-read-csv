package th2.datareader.csvreader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.logstash.logback.argument.StructuredArguments;

public class Main extends Object  {
	
	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) {
		
		try {		
   			
		    Properties props = new Properties();
		    
		    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		    
		    try (InputStream configStream = classLoader.getResourceAsStream("logback.xml"))  {
		        props.load(configStream);
		    } catch (IOException e) {
		        System.out.println("Errornot laod configuration file ");
		    }    			
	            	     
		    RabbitMqClient client = new RabbitMqClient();
	        
	        client.connect();
	        
			CsvReader reader = new CsvReader();
			
			client.setSessionAlias(reader.getFileName());
			
			while (reader.hasNextLine()) {
				String line = reader.getNextLine();
				logger.trace("csvLine",StructuredArguments.value("csvLine",line));
				client.publish(line);
			}
			
			reader.close();
			
			client.close();
		} catch (Exception e) {		
			logger.error("{}", e);
		}
	}
}
