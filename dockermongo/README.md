
# Distributed NoSQL Database System Implementation

A guide to set up a MongoDB database cluster with support for replication. In this case, we are assuming one master and two slave nodes. Following these steps for a smooth set up.




## Steps
1. Ensure Docker Engine is running
2. Set up a docker network called `mongonetwork` using the following command 
```
docker network create mongonetwork
```
3. Ensure you are inside the `dockermongo` directory
4. Create the appropriate file structure by running the following commands sequentially
```
mkdir -p mongomaster/data/db
``` 

```
mkdir -p mongoslave1/data/db
```

```
mkdir -p mongoslave2/data/db
```
5. Create the master node MongoDB container using the following command for Windows based systems
````
docker run -dit --rm --name mongomaster `
-p 6001:27017 --net mongonetwork `
-v ${PWD}/mongomaster/data/db:/data/db `
mongo mongod --replSet rs0
````
For Unix/Linux based systems, use the following command
````
docker run -dit --rm --name mongomaster \
-p 6001:27017 --net mongonetwork \
-v $(pwd)/mongomaster/data/db:/data/db \
mongo mongod --replSet rs0
````
6. Now, create the slave1 MongoDB container using the following command for Windows based systems
````
docker run -dit --rm --name mongoslave1 `
-p 6002:27017 --net mongonetwork `
-v ${PWD}/mongoslave1/data/db:/data/db `
mongo mongod --replSet rs0
````
For Unix/Linux based systems, use the following command
````
docker run -dit --rm --name mongoslave1 \
-p 6002:27017 --net mongonetwork \
-v $(pwd)/mongoslave1/data/db:/data/db \
mongo mongod --replSet rs0
````
7. Now, create the slave2 MongoDB container using the following command for Windows based systems
````
docker run -dit --rm --name mongoslave2 `
-p 6003:27017 --net mongonetwork `
-v ${PWD}/mongoslave2/data/db:/data/db `
mongo mongod --replSet rs0
````
For Unix/Linux based systems, use the following command
````
docker run -dit --rm --name mongoslave2 \
-p 6003:27017 --net mongonetwork \
-v $(pwd)/mongoslave2/data/db:/data/db \
mongo mongod --replSet rs0
````
8. Next, we enter the master container using the following
```
docker exec -it mongomaster mongosh
```
9. We define the replication config by setting a variable, using the following command
````
config = {"_id": "rs0", "members": [{"_id": 0, "host": "mongomaster:27017"},{"_id": 1, "host": "mongoslave1:27017"},{"_id": 2, "host": "mongoslave2:27017"}]}
````
10. Then, we apply the config by using the following command
````
rs.initiate(config)
````
11. Next, we exit the master container and enter the slave containers and run the following commands, in their respective shells
````
mongoslave1.setSecondaryOk()
````
````
mongoslave2.setSecondaryOk()
````

The replication should now be up and running.
