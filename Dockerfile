# Build a Docker image for development.

FROM openjdk:8-alpine

# Set the work directory.
WORKDIR /opt/app/bin

# Create a non-root user with no password and no home directory.
RUN addgroup ereefs && adduser --system ereefs --ingroup ereefs

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
