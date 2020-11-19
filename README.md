# th2-csvreader

th2-csvreader

## Configuration

**RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY** - defines exchange name

RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY=demo_exchange (default value)

**RABBITMQ_HOST** - rabbitmq host

RABBITMQ_HOST=10.0.0.12 

**RABBITMQ_PORT** - rabbitmq port

RABBITMQ_PORT=5672

**RABBITMQ_VHOST** - rabbitmq vhost

RABBITMQ_VHOST=vhost

**RABBITMQ_USER** - rabbitmq user

RABBITMQ_USER=user

**RABBITMQ_PASS** - rabbitmq user password

RABBITMQ_PASS=password

**RABBITMQ_QUEUE_POSTFIX** - specifying postfix for generated queue names

RABBITMQ_QUEUE_POSTFIX = prod.  This will produces one queue: csvreader.first.prod

If not specified  -  will produce one queues: csvreader.first.default

**CSV_FILE_NAME** - the name of the file in CSV format to read data from.

**BATCH_PER_SECOND_LIMIT** - the max number of batches to publish per second. The default value is **-1** that means no limit.