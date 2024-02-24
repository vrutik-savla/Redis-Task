# Redis-Task
This is a producer-consumer problem, where producer function produces random numbers & strings using go routines (multi-threading), then it stores this data (as list datatype) in Redis database. After creating all producers, consumer function starts consuming the data (basically deletes data from Redis database) & it calculates the checksum of produced & consumed data. If checksum matches than producer-consumer function is said to be executed well. <br>
The code is completed written in pure go-lang.
