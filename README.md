# Portfolio
This repository is my portfolio for data engineering.

## Requirements
- Docker Desktop
- pipenv or another virtualenv

## Build and Up the platform
Execute the following command
```
$ cd portfolio
$ docker compose up -d --build
$ docker ps -a
```
You should have the following containers up and running
<pre>
kafka-ui            <a href="http://localhost:8080">http://localhost:8080</a>
kafka-connect0      <a href="http://localhost:8083">http://localhost:8083</a>
schema-registry0    <a href="http://localhost:8085">http://localhost:8085</a>
rest-proxy          <a href="http://localhost:8082">http://localhost:8082</a>
ksqldb-server       <a href="http://localhost:8088">http://localhost:8088</a>
kafka0              <a href="http://localhost:9092">http://localhost:9092</a>
spark-master        <a href="http://localhost:18080">http://localhost:18080</a>
spark-worker-1      <a href="http://localhost:28081">http://localhost:28081</a>
minio-storage       <a href="http://localhost:9000">http://localhost:9000</a>
</pre>

## Shutdown the platform
```
$ docker compose down
```

## Start API Server
To start your API Server execute the following command
```
$ cd api
$ pipenv --python 3.11
$ pipenv shell
$ pip install -r requirements.txt
$ python server.py
```
The default value for port argument is `60000`

## Start the events JSON generator
To start your API Server execute the following command
```
$ cd generator
$ pipenv --python 3.11
$ pipenv shell
$ pip install -r requirements.txt
$ python generator.py
```
The default values for hostname and filename arguments are `localhost:60000` and `../data/events.json`
