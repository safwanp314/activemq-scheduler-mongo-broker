# activemq schedular with mongo

1. place activemq-scheduled-message-broker.jar and mongo-java-driver.jar into lib directory of activemq

2. place below code in between <broker> tag of conf/activemq.xml
<plugins>
 	<bean xmlns="http://www.springframework.org/schema/beans" class="org.apache.activemq.broker.scheduler.mongo.MongoScheduledBroker" init-method="start" destroy-method="stop">
 		<!-- mongo host -->
 		<constructor-arg value="127.0.0.1" />
 		<!-- mongo port -->
 		<constructor-arg value="27017" />
 		<!-- mongo activemq database -->
 		<constructor-arg value="activemq-broker" />
 		<constructor-arg>
 			<map>
 				<entry key="connectionsPerHost" value="200"></entry>
 				<entry key="minConnectionsPerHost" value="10"></entry>
 			</map>
 		</constructor-arg>
 	</bean>
 </plugins>
3. replace host and port accordingly

4. set long property AMQ_SCHEDULED_DELAY with delay value while sending message

example:
curl -XPOST -d "body=message" -d "AMQ_SCHEDULED_DELAY =60000" http://admin:admin@localhost:8161/api/message?destination=queue://test.input
