# Csv Reader User Manual 1.00

## Document Information

### Table of contents

{table_contents}

### Introduction

Csv reader read csv files, results are sending to RabbitMQ.
Csv reader produces **raw messages**. See **RawMessage** type in infra.proto.

### Quick start

#### Configuration

##### Reader configuration

Example:
```json
{
  "csv-file": "path/to/file.csv",
  "csv-header": "price,side,qty,orderid,clientid",
  "max-batches-per-second": 1000
}
```

**csv-file** - specifying path where log file is located

**max-batches-per-second** - the maximum number of batches publications per second. The default value is **-1** that means not limit.

**csv-header** - specifying the header of the csv file. If the value is empty than first line of the csv file interprets as header.


##### Pin declaration

The log reader requires a single pin with _publish_ and _raw_ attributes. The data is published in a raw format. To use it please conect the output pin with another pin that transforms raw data to parsed data. E.g. the **codec** box.

Example:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2GenericBox
metadata:
  name: csv-reader
spec:
  pins:
    - name: out_log
      connection-type: mq
      attributes: ['raw', 'publish', 'store']
```