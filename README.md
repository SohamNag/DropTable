# DropTable
Code for ASU 512 - DDS term project

## Contribution to the repo

To contribute to the project, please follow these steps:
- Clone it to your local machine `git clone git@github.com:bharat787/DropTable.git`
- Create a new branch with the contributor_feature name `git checkout -b <contributer_feature>`
- Push the changes to **your own** branch.
- Create a PR from github.

## Setup

The project is built using Python and Docker. Please follow these steps to run your local machine
- Download [Docker](https://docs.docker.com/get-docker/)
- Install psycopg2 `pip install psycopg2`
- `git clone git@github.com:bharat787/DropTable.git`
- `cd DropTable`. After this you are at the `root` directory of this project. 
  
Your directory tree currently should be looking like this

`
    ├── README.md
    ├── datasets
    ├── docker
    │   ├── master
    │   │   └── config
    │   │       ├── pg_hba.conf
    │   │       ├── pg_ident.conf
    │   │       └── postgresql.conf
    │   └── slave1
    │       └── config
    │           ├── pg_hba.conf
    │           ├── pg_ident.conf
    │           └── postgresql.conf
    └── main.py
`

`datasets` has mock data CSVs for the project. Inside the `docker` directory you has a `master` docker image config and `slave1` image config. You can setup more slaves 
by making more slave directories `mkdir -p slave<#>/config` and copying the `.conf` files from `slave1/config` to `slave<#>/config`

Now to spin up your master PostgreSQL.

Create a network on which all instances will communicate:
`docker network create postgres`

Spin up master, run the following at the `root`:

`
    docker run -it --rm --name master \ 
    --net postgres \
    -e POSTGRES_USER=postgresadmin \
    -e POSTGRES_PASSWORD=admin123 \
    -e POSTGRES_DB=masterdb \
    -e PGDATA="/data" \
    -v $(pwd)/master/pgdata:/data \
    -v $(pwd)/master/config:/config \
    -v $(pwd)/master/archive:/mnt/server/archive \
    -p 5001:5432 \
    postgres:15.0 -c 'config_file=/config/postgresql.conf'
`

Few things to note here:
- The above commands are for unix/linux based machines. To run on windows replace `\` with ``backticks(`)`` and `$(pwd)` with `${PWD}`.
- We name our postgresDB as `masterdb`, you can name it anything you want.
- We run this instance on port `5001`, instead of the usual `5432`.
- In this project we are using postgres 15.0, you may choose to upgrade to latest versions.

Now, we will have to create a replication user to replicate master for slave instances. Run the following commands.

`
    docker exec -it master bash

    createuser -U postgresadmin -P -c 5 --replication replicationUser

    exit
`

Take a base backup of master.
`
    docker run -it --rm \
    --net postgres \
    -v $(pwd)/slave1/pgdata:/data \
    --entrypoint /bin/bash postgres:15.0
`

Take the backup by loggin into `master` with our `replicationUser` and writing the backup to `\data`.

`pg_basebackup -h master -p 5432 -U replicationUser -D /data/ -Fp -Xs -R`

Now let's start the `slave1` instance.

`
    docker run -it --rm --name slave1 \ 
    --net postgres \
    -e POSTGRES_USER=postgresadmin \
    -e POSTGRES_PASSWORD=admin123 \
    -e POSTGRES_DB=postgresdb \
    -e PGDATA="/data" \
    -v $(pwd)/slave1/pgdata:/data \
    -v $(pwd)/slave1/config:/config \
    -v $(pwd)/slave1/archive:/mnt/server/archive \
    -p 5002:5432 \
    postgres:15.0 -c 'config_file=/config/postgresql.conf'
`

- Please note we start this instance on port `5002`.
- To setup more slave instance, repeat the backup steps and then spin them up on different ports.

Now you will have two instances of postgres running, with `master` as the primary postgres instance, with `slave1` as a replication server.
The `slave1` instance can be promoted to `master` in case of a failover and support read-write operations.