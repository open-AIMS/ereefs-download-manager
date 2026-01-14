# Build a Docker image for development.
# Using Ubuntu-based image instead of Alpine due to OpenSSL 3.x compatibility issues
# Alpine 3.22+ uses OpenSSL 3.5.4 which has stricter TLS requirements that break SSL handshakes
# Ubuntu 20.04 (focal) uses OpenSSL 1.1.1 which is compatible with our Java 8 + HttpClient 4.5.2 setup

FROM eclipse-temurin:8-jre-focal

# Set the work directory.
WORKDIR /opt/app/bin

# Create a non-root user with no password and no home directory.
RUN groupadd -r ereefs && useradd -r -g ereefs -M ereefs

# Add the main JAR file.
ARG JAR_NAME
COPY --chown=ereefs:ereefs target/${JAR_NAME} /opt/app/bin/

# Create an 'entrypoint.sh' script that executes the JAR file.
RUN echo "java -jar /opt/app/bin/${JAR_NAME}" > entrypoint.sh
RUN chmod +x /opt/app/bin/entrypoint.sh

# Debugging - uncomment the following 2 lines to help debugging
#RUN ls -al /opt/app/bin
#RUN cat /opt/app/bin/entrypoint.sh

# Use the new user when executing.
USER ereefs

# Use the 'entrypoint.sh' script when executing.
ENTRYPOINT "/opt/app/bin/entrypoint.sh"
