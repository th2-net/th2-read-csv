package th2.datareader.csvreader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.logstash.logback.argument.StructuredArguments;

public class Main extends Object  {
	
	private final static Logger logger = LoggerFactory.getLogger(Main.class);

    private static final int NO_LIMIT = -1;
    private static final String BATCH_PER_SECOND_LIMIT_ENV = "BATCH_PER_SECOND_LIMIT";
	
	public static void main(String[] args) {
		
		try {		
   			
		    Properties props = new Properties();
		    
		    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		    
		    try (InputStream configStream = classLoader.getResourceAsStream("logback.xml"))  {
		        props.load(configStream);
		    } catch (IOException e) {
		        System.out.println("Error cannot load configuration file ");
		    }

            int maxBatchesPerSecond = getMaxBatchesPerSecond();
            long lastResetTime = System.currentTimeMillis();
            int batchesPublished = 0;
            boolean limited = maxBatchesPerSecond != NO_LIMIT;
            if (limited) {
                logger.info("Publication is limited to {} batch(es) per second", maxBatchesPerSecond);
            } else {
                logger.info("Publication is unlimited");
            }

            try (RabbitMqClient client = new RabbitMqClient()) {

                client.connect();

                try (CsvReader reader = new CsvReader()) {

                    client.setSessionAlias(reader.getFileName());

                    while (reader.hasNextLine()) {
                        if (limited) {
                            if (batchesPublished >= maxBatchesPerSecond) {
                                long currentTime = System.currentTimeMillis();
                                long timeSinceLastReset = Math.abs(currentTime - lastResetTime);
                                if (timeSinceLastReset < 1_000) {
                                    logger.debug("Suspend reading. Last time: {} mills, current time: {} mills, batches published: {}",
                                            lastResetTime, currentTime, batchesPublished);
                                    Thread.sleep(1_000 - timeSinceLastReset);
                                    continue;
                                }
                                lastResetTime = currentTime;
                                batchesPublished = 0;
                            }
                        }
                        String line = reader.getNextLine();
                        logger.trace("csvLine", StructuredArguments.value("csvLine", line));
                        if (client.publish(line)) {
                            batchesPublished++;
                        }
                    }
                }
            }
		} catch (Exception e) {
			logger.error("{}", e);
			System.exit(2);
		}
	}

    private static int getMaxBatchesPerSecond() {
        String limitValue = System.getenv(BATCH_PER_SECOND_LIMIT_ENV);
        return limitValue == null
                ? NO_LIMIT
                : verifyPositive(Integer.parseInt(limitValue.trim()),
                BATCH_PER_SECOND_LIMIT_ENV + " must be a positive integer but was " + limitValue);
    }

    private static int verifyPositive(int value, String message) {
        if (value <= 0) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }
}
