/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.readcsv;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.logstash.logback.argument.StructuredArguments;

import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.readcsv.cfg.ReaderConfig;


public class Main extends Object  {
	
	private final static Logger logger = LoggerFactory.getLogger(Main.class);

    private static final int NO_LIMIT = -1;
    private static final String BATCH_PER_SECOND_LIMIT_ENV = "BATCH_PER_SECOND_LIMIT";
	
	public static void main(String[] args) {
		
        CommonMetrics.setLiveness(true);
        
        CommonFactory commonFactory = CommonFactory.createFromArguments(args);
        
        ReaderConfig configuration = commonFactory.getCustomConfiguration(ReaderConfig.class);
        
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

            File csvFile = configuration.getCsvFile();
            String csvHeader = configuration.getCsvHeader();
                        
            try (PiblisherClient client = new PiblisherClient(csvFile.getName(), commonFactory.getMessageRouterRawBatch())) {
            	            	            	
                try (CsvReader reader = new CsvReader(csvFile)) {
                	
                	if (csvHeader.isBlank()) {
                		csvHeader = reader.getHeader(); 
                		logger.info("csvHeader",StructuredArguments.value("csvHeader", csvHeader));
                	}
                	
            		client.setCsvHeader(csvHeader);                	
            		CommonMetrics.setReadiness(true);

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
			logger.error("Cannot read file", e);
		}
		
        CommonMetrics.setReadiness(false);
        CommonMetrics.setLiveness(false);
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
