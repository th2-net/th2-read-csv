# Csv Reader User Manual 2.0.1

## Document Information

### Table of contents

{table_contents}

### Introduction

Csv reader read csv files, results are sending to RabbitMQ.

### Quick start

#### Configuration

##### Reader configuration

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: read-csv
spec:
  image-name: ghcr.io/th2-net/th2-read-csv
  image-version: <image version>
  type: th2-read
  custom-config:
      sourceDirectory: "dir/with/csv/"
      aliases:
        A:
          nameRegexp: "fileA.*\\.log"
#          delimiter: ","
        B:
          nameRegexp: "fileB.*\\.log"
          delimiter: ";"
          header: ['ColumnA', 'ColumnB', 'ColumnC']
      common:
        staleTimeout: "PT1S"
        maxBatchSize: 100
        maxPublicationDelay: "PT5S"
        leaveLastFileOpen: false
        fixTimestamp: false
        maxBatchesPerSecond: -1 # unlimited
        disableFileMovementTracking: true
      pullingInterval: "PT5S"
      validateContent: true
      validateOnlyExtraData: false
  pins:
    - name: read_csv_out
      connection-type: mq
      attributes: ['raw', 'publish', 'store']
  extended-settings:
    service:
      enabled: false
    envVariables:
      JAVA_TOOL_OPTIONS: "-XX:+ExitOnOutOfMemoryError"
    mounting:
      - path: "<destination path in Kubernetes pod>"
        pvcName: <Kubernetes persistent volume component name >
    resources:
      # Min system requirments ...
      limits:
        memory: 200Mi
        cpu: 200m
      requests:
        memory: 100Mi
        cpu: 50m
```
 
+ logDirectory - the directory to watch CSV files
+ aliases - the mapping between alias and files parameters that correspond to that alias. 
    + nameRegexp - regular expressions that is to match the file to read. **The files that matches the alias by the regexp will be read one by one**;
    + delimiter - the delimiter to use during CSV parsing. By default, it is `,`;
    + header - the list of headers to use for this file. If this parameters is not specified the first record from the file will be used as a header.
+ common - the common configuration for read core. Please found the description [here](https://github.com/th2-net/th2-read-file-common-core/blob/master/README.md#configuration).
  NOTE: the fields with `Duration` type should be described in the following format `PT<number><time unit>`.
  Supported time units (**H** - hours,**M** - minutes,**S** - seconds). E.g. PT5S - 5 seconds, PT5M - 5 minutes, PT0.001S - 1 millisecond
+ pullingInterval - how often the directory will be checked for updates after not updates is received;
+ validateContent - enables content validation.
  The content size (number of columns) should match the header size (either defined in the configuration or read from file).
  Otherwise, an error will be reported and reading for the stream ID which caused the error will be stopped.
  The default value is `true`;
+ validateOnlyExtraData - disables validation when the content size is less than the header size (probably some columns were not set on purpose).
  Works only with `validateContent` set to `true`. The default value is `false`

## Changes

### 1.2.1
+ Updated `bom` from 4.1.0 to 4.2.0
+ Updated `common` form 3.44.0 to 3.44.1

### 1.2.0
+ Updated `kotlin` from 1.4.32 to 1.6.21
+ Updated `common` form 3.16.1 to 3.44.0

### 1.1.0

+ Feature: Status for first and last messages

### 1.0.1

+ Fix problem with incorrect sequences

### 1.0.0

+ Migrate to common read-core