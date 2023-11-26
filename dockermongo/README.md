
# Distributed NoSQL Database System Implementation

A guide to set up a MongoDB database cluster with support for replication. In this example, we are setting up two clusters each containing one master and two slave nodes. Following these steps for a smooth set up.




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

```
mkdir -p mongomaster2/data/db
``` 

```
mkdir -p mongoslave3/data/db
```

```
mkdir -p mongoslave4/data/db
```

5. Create the `master` and `master2` nodes MongoDB containers using the following commands sequentially for Windows based systems
````
docker run -dit --rm --name mongomaster `
-p 6001:27017 --net mongonetwork `
-v ${PWD}/mongomaster/data/db:/data/db `
mongo mongod --replSet rs0
````

````
docker run -dit --rm --name mongomaster2 `
-p 7001:27017 --net mongonetwork `
-v ${PWD}/mongomaster2/data/db:/data/db `
mongo mongod --replSet rs1
````
For Unix/Linux based systems, use the following commands
````
docker run -dit --rm --name mongomaster \
-p 6001:27017 --net mongonetwork \
-v $(pwd)/mongomaster/data/db:/data/db \
mongo mongod --replSet rs0
````

````
docker run -dit --rm --name mongomaster2 \
-p 7001:27017 --net mongonetwork \
-v $(pwd)/mongomaster2/data/db:/data/db \
mongo mongod --replSet rs1
````
6. Now, create the `slave1` and `slave3` MongoDB containers using the following commands sequentially for Windows based systems
````
docker run -dit --rm --name mongoslave1 `
-p 6002:27017 --net mongonetwork `
-v ${PWD}/mongoslave1/data/db:/data/db `
mongo mongod --replSet rs0
````

````
docker run -dit --rm --name mongoslave3 `
-p 7002:27017 --net mongonetwork `
-v ${PWD}/mongoslave3/data/db:/data/db `
mongo mongod --replSet rs1
````
For Unix/Linux based systems, use the following commands
````
docker run -dit --rm --name mongoslave1 \
-p 6002:27017 --net mongonetwork \
-v $(pwd)/mongoslave1/data/db:/data/db \
mongo mongod --replSet rs0
````

````
docker run -dit --rm --name mongoslave3 \
-p 7002:27017 --net mongonetwork \
-v $(pwd)/mongoslave3/data/db:/data/db \
mongo mongod --replSet rs1
````
7. Now, create the `slave2` and `slave4` MongoDB containers using the following commands sequentially for Windows based systems
````
docker run -dit --rm --name mongoslave2 `
-p 6003:27017 --net mongonetwork `
-v ${PWD}/mongoslave2/data/db:/data/db `
mongo mongod --replSet rs0
````

````
docker run -dit --rm --name mongoslave4 `
-p 7003:27017 --net mongonetwork `
-v ${PWD}/mongoslave4/data/db:/data/db `
mongo mongod --replSet rs1
````
For Unix/Linux based systems, use the following commands
````
docker run -dit --rm --name mongoslave2 \
-p 6003:27017 --net mongonetwork \
-v $(pwd)/mongoslave2/data/db:/data/db \
mongo mongod --replSet rs0
````

````
docker run -dit --rm --name mongoslave4 \
-p 7003:27017 --net mongonetwork \
-v $(pwd)/mongoslave4/data/db:/data/db \
mongo mongod --replSet rs1
````
8. Next, in different terminal windows, one pertaining to each of the `master` and `master2` containers, enter the respective master containers using the following commands
```
docker exec -it mongomaster mongosh
```

```
docker exec -it mongomaster2 mongosh
```
9. We define the replication config in `master` mongo shell by setting a variable in its terminal window, using the following command
````
config = {"_id": "rs0", "members": [{"_id": 0, "host": "mongomaster:27017", "priority": 20},{"_id": 1, "host": "mongoslave1:27017", "priority": 5},{"_id": 2, "host": "mongoslave2:27017", "priority": 5}]}
````

Similarly, in the terminal window of `master2` mongo shell, run the following command
````
config2 = {"_id": "rs1", "members": [{"_id": 0, "host": "mongomaster2:27017", "priority": 20},{"_id": 1, "host": "mongoslave3:27017", "priority": 5},{"_id": 2, "host": "mongoslave4:27017", "priority": 5}]}
````

10. Then, apply the config by using the following command in the terminal window of the `master` container
````
rs.initiate(config)
````
Similarly, apply the config by using the following command in the terminal window of the `master2` container
````
rs.initiate(config2)
````
11. Next, we exit the master containers' mongo shells and enter the slave containers' mongo shells (as described in Step 8 above) and run the following command, in their respective shells
````
db.setSecondaryOk()
````

The replication should now be up and running.
