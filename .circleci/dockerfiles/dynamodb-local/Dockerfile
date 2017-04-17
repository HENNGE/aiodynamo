FROM openjdk:8-jre

RUN ["curl", "-o", "dynamodblocal.tar.gz", "https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"]
RUN ["tar", "xzvf", "dynamodblocal.tar.gz"]

EXPOSE 8000

ENTRYPOINT ["java", "-Djava.library.path=./DynamoDBLocal_lib", "-jar", "DynamoDBLocal.jar", "-inMemory"]
