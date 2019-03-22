Insure .env file is in the same directory as docker compose files

Attempted to implement bonus-service but could not complete it as not familiar with Kafka streams syntax

Possible solutions for bonus-service

1) https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Stream+Usage+Patterns

2) Create KTable 's' holding speed and count (KTable<Windowed<Long>, Long> count  and KTable<Windowed<Long>, Double> speed)
   and then join them to get expected results