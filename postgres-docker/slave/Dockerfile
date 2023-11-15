# Use the official Postgres image as a base
FROM postgres:latest

# Set environment variables for the slave
ENV POSTGRES_DB=slavedb
ENV POSTGRES_USER=slaveuser
ENV POSTGRES_PASSWORD=slavepassword

# Copy the custom configuration files for the slave
COPY ./slave/postgresql.conf /var/lib/postgresql/data/postgresql.conf
COPY ./slave/pg_hba.conf /var/lib/postgresql/data/pg_hba.conf

# Expose the port Postgres is reachable on
EXPOSE 5441

# The CMD directive is used to set the container to run as an executable;
# in this case, it starts Postgres with the replication configuration.
CMD ["postgres", "-c", "config_file=/var/lib/postgresql/data/postgresql.conf"]

