ARG CONFLUENT_VERSION=7.6.1

# Final Kafka Connect Image
FROM confluentinc/cp-kafka-connect:${CONFLUENT_VERSION}

# Install more connectors if you need
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:0.6.5 \
   && confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.10 \
   && confluent-hub install --no-prompt jcustenborder/kafka-connect-transform-common:latest \
   && confluent-hub install --no-prompt confluentinc/connect-transforms:1.4.6
