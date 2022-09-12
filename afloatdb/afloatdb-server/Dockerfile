FROM openjdk:8-jre-alpine

# fail the build if AFLOATDB_VERSION not provided
ARG AFLOATDB_VERSION
RUN test -n "$AFLOATDB_VERSION"

# we will afloatdb files under this directory
ENV AFLOATDB_HOME="/opt/afloatdb"
# we will run a fat jar that contains all dependencies
ENV AFLOATDB_JAR_FILE="afloatdb-${AFLOATDB_VERSION}-fat.jar"
# we will use the default logger config if no other config file provided
ENV AFLOATDB_LOGGER_CONFIG_FILE="log4-default.xml"
# we will print gc logs to this file
ENV AFLOATDB_GC_LOG_FILE="gc.log"
# we expect the caller to provide this ENV while starting the container
ENV AFLOATDB_CONFIG_FILE="missing-config.conf"
# source jar can be changed at build time
ARG SOURCE_JAR="target/afloatdb-server-${AFLOATDB_VERSION}-jar-with-dependencies.jar"
# default logger config file can be changed during build time
ARG DEFAULT_LOGGER_CONFIG_FILE=src/main/resources/log4j2-default.xml

WORKDIR ${AFLOATDB_HOME}

COPY ${SOURCE_JAR} ${AFLOATDB_JAR_FILE}
COPY ${DEFAULT_LOGGER_CONFIG_FILE} ${AFLOATDB_LOGGER_CONFIG_FILE}

EXPOSE 6701

ENTRYPOINT /usr/bin/java -server \
            -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:${AFLOATDB_GC_LOG_FILE} \
            -Dlog4j.configurationFile=${AFLOATDB_LOGGER_CONFIG_FILE} \
            -jar ${AFLOATDB_JAR_FILE} ${AFLOATDB_CONFIG_FILE}
