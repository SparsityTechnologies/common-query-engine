/*
 * X-Ray configuration for Apache Derby >= 10.11.1.1.
 */

import static eu.coherentpaas.xray.config.AgentConfig.xray
import eu.coherentpaas.xray.graph.GraphAppender
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.ConsoleAppender

statusListener(OnConsoleStatusListener)

println("Loading XRay instrumentation for CQE.")

appender("STDOUT", ConsoleAppender) {
  encoder(PatternLayoutEncoder) {
    pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
  }
}

appender("SOCKET", SocketAppender) {
    port = 12345
    remoteHost = "localhost"
}

root(WARN, ["STDOUT"])

// JDBC Sender
xray("org.apache.derby.client.net") {
	instrument("NetConnection") {
		send("flowConnect(Ljava/lang/String;I)V", "agent_.socket_")
	}
}
xray("org.apache.derby.client.am") {
	instrument("ClientStatement") {
		send("executeQueryX(Ljava/lang/String;)Lorg/apache/derby/client/am/ClientResultSet;", "agent_.socket_")
	}
	instrument("ClientResultSet") {
		send("nextX()Z", "agent_.socket_")
	}
	instrument("ClientConnection") {
		send("closeX()V", "agent_.socket_")
	}
}

// JDBC Receiver
xray("org.apache.derby.drda") {
	instrument("MdSQLDRDAConnThread") {
		log "processCommands()V"
		receive("processCommands()V", "session.clientSocket")
	}
}

// SQL Engine
xray("org.apache.derby.iapi.sql.execute") {
	instrument("NoPutResultSet") {
		inherit = true

		log "openCore()V"
		log "getNextRowCore()Lorg/apache/derby/iapi/sql/execute/ExecRow;"
	}
}
xray("org.apache.derby.impl")
logger("xray", ALL, ["SOCKET"], false)

