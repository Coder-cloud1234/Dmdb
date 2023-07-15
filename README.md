# Dmdb
## 1. Introduction
Distributed memory database implemented by C++. This program used redis 5 for reference.
The goal for developing this project is to reduce maintenance complexity and provide an
easier way for you to understand how memory database works. You can use redis-cli to 
connect to this database server. Now we have implemented the follow commands for clients:
1.AUTH  
2.MULTI/EXEC  
3.SET  
4.GET  
5.DEL  
6.EXISTS  
7.MSET  
8.EXPIRE  
9.KEYS  
10.DBSIZE  
11.PING  
12.ECHO  
13.SAVE  
14.TYPE  
15.PTTL  
16.PERSIST  
17.CLIENT  
18.BGSAVE  
19.SHUTDOWN  
20.ROLE  
21.WAIT  
Most of the commands above can be executed like being executed in redis server. Part of them
are a little different from redis, you can read the source code for the details. We had done
a performance test of this program and redis 5 by redis-benchmark in Ali cloud(clients=50,requests=100000), the result is as below:  
 | PING_BULK | SET | GET | MSET
---- / ----- / ------ / ------- / --------
Dmdb | 6317.926 | 6062.614 | 6263.438 | 6163.382
Redis | 6201.546 | 6100.722 | 6230.836 | 6298.842
According to result, we can see Dmdb has similar performance as redis 5.

## 2. Usage
Step1: Download this project  
Step2: Enter this project directory and execute "cmake" command  
Step3: Execute "make" command  
Step4: Start the server with "./DmdbServer xxx.conf" command  
Notice: Now Dmdb only supports linux platform.  

## 3. Summary and outlook
Now Dmdb has supported master-slave, cluster mode and other new features are still under development.  


