# activemq schedular with mongo

1. Place activemq-scheduled-message-broker.jar and mongo-java-driver.jar into lib directory of activemq

2. Place below code in between <broker> tag of conf/activemq.xml

```xml
<plugins>
 	<bean xmlns="http://www.springframework.org/schema/beans" class="org.apache.activemq.broker.scheduler.mongo.MongoSchedulerBroker" init-method="start" destroy-method="stop">
 		<!-- mongo host -->
 		<property name="host" value="127.0.0.1" />
 		<!-- mongo port -->
 		<property name="port" value="27017" />
 		<!-- mongo activemq database -->
 		<property name="database" value="activemq-broker" />
 		<!-- mongo connection properties -->
 		<property name="props">
    <map>
     <entry key="connectionsPerHost" value="200"></entry>
     <entry key="minConnectionsPerHost" value="10"></entry>
    </map>
   </property>
 	</bean>
 </plugins>
``` 

3. Configure mongodb host and port

4. Set property AMQ_SCHEDULED_DELAY with delay value while sending message in to activemq <br/>
example:
curl -XPOST -d "body=message" -d "AMQ_SCHEDULED_DELAY=60000" http://admin:admin@localhost:8161/api/message?destination=queue://test.input
