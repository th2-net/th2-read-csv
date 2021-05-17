/**
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.readcsv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.logstash.logback.argument.StructuredArguments;

public class CsvReader implements AutoCloseable {
	private final File fileName;
	private Scanner scanner; 
	private Logger logger = LoggerFactory.getLogger(CsvReader.class);
	
	public CsvReader(File fileName) throws FileNotFoundException {
		this.fileName = Objects.requireNonNull(fileName, "'Csv file name' parameter");
		 				
		try {
			scanner = new Scanner(fileName);
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage(), e);
            throw e;
		}
		
		logger.info("Open csv file {}", StructuredArguments.value("fileName",fileName.getAbsolutePath()));
	}

	public boolean hasNextLine() {
		return scanner.hasNextLine();
	}
	
	public String getNextLine() {
		return scanner.nextLine();
	}
	
	public String getHeader() throws IOException {
		
		String header= "";
		
		while (scanner.hasNextLine()) {
			header = scanner.nextLine();
			if (!header.isBlank()) {
				break;
			}
		}
		
		return header;
	}
	
	@Override
	public void close() {
		if (scanner != null) {
			scanner.close();
		}
		logger.info("Close csv file {}", StructuredArguments.value("fileName",fileName));
	}
}
