

This is a Yamcs plugin for Archiving Yamcs parameters in Influxdb DataBase



## Steps

1- Run Influxdb on Docker

docker run -d -p 8086:8086 \
      -v $PWD/data:/var/lib/influxdb2 \
      -v $PWD/config:/etc/influxdb2 \
      -e DOCKER_INFLUXDB_INIT_MODE=setup \
      -e DOCKER_INFLUXDB_INIT_USERNAME=my-user \
      -e DOCKER_INFLUXDB_INIT_PASSWORD=my-password \
      -e DOCKER_INFLUXDB_INIT_ORG=my-org \
      -e DOCKER_INFLUXDB_INIT_BUCKET=Telemetry \
      influxdb:2.0
      
2- Add this to yamcs pom.xml file
      <dependency>
      <groupId>org.yamcs</groupId>
      <artifactId>yamcs-Influx</artifactId>
      <version>1.0.1</version>
    </dependency> 
       
3- Add the following lines to the simulator yaml file
  - class: org.yamcs.Influx.InfluxdbParameterArchive
    args:
      token: ""
      org: "my-org"
      Link: "http://localhost:8086\\"
      bucket: "Telemetry"
      
4- run ./run-example.sh simulation

5- open http://localhost:8086  in your browser and go to explore to query the parameters and visulaize it.

6-There is two rest api just for testing one for creating new buckets and other for archiving historical parameter.

## License

Affero GPLv3
