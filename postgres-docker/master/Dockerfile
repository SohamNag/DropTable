# Use the official Postgres image as a base
FROM postgres:latest

# Set environment variables if you want to embed them in the image
# It's better to use Docker secrets or environment files for sensitive information
ENV POSTGRES_DB=masterdb
ENV POSTGRES_USER=masteruser
ENV POSTGRES_PASSWORD=masterpassword

# Copy the custom configuration file for Postgres
# This is where you would set the necessary configuration for replication
COPY ./master/postgresql.conf /etc/postgresql/postgresql.conf
COPY ./master/pg_hba.conf /etc/postgresql/pg_hba.conf

# Expose the port Postgres is reachable on
EXPOSE 5440

# Set the custom configuration file as the default for Postgres
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
