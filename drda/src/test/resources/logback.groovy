/*
 * X-Ray configuration for Apache Derby >= 10.11.1.1.
 */

import static eu.coherentpaas.xray.config.AgentConfig.xray
import eu.coherentpaas.xray.graph.GraphAppender
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.ConsoleAppender
import eu.coherentpaas.xray.logback.RawSocketAppender


statusListener(OnConsoleStatusListener)

println("Loading XRay instrumentation for CQE.")

appender("STDOUT", ConsoleAppender) {
  encoder(PatternLayoutEncoder) {
    pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
  }
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
//eutropia
 xray("org.apache.hadoop.hbase.client") {
   
 instrument("SHTable") {
      inherit = true
            log "get(Lorg/apache/hadoop/hbase/client/Get;)Lorg/apache/hadoop/hbase/client/Result;"
            log "put(Lorg/apache/hadoop/hbase/client/Put;)V"
      send("get(Lorg/apache/hadoop/hbase/client/Get;)Lorg/apache/hadoop/hbase/client/Result;", "#0")
            send("put(Lorg/apache/hadoop/hbase/client/Put;)V", "#0")
    }
} 



xray("org.apache.derby.impl")


xray("eu.coherentpaas.wrapper") {
    instrument("Graph") {
        inherit = true
        log "resolveQuery(Ljava/lang/Long;Ljava/lang/String;Ljava/util/Map;)Ljava/lang/String;"
        send("resolveQuery(Ljava/lang/Long;Ljava/lang/String;Ljava/util/Map;)Ljava/lang/String;", "#0")
    }
}

// MongoDB
// MongoWrapperCQEStatement execution
xray("eu.coherentpaas.mongodb.wrapper") {
    instrument("MongoCQEStatement") {

        log "execute(Leu/coherentpaas/transactionmanager/client/TxnCtx;)Leu/coherentpaas/cqe/ResultSet;"
    }
}

// MongoDB SELECT and INSERT execution
xray("com.mongodb") {
    instrument("DBCollection") {

        log "find(Lcom/mongodb/DBObject;Lcom/mongodb/DBObject;)Lcom/mongodb/DBCursor;"
        log "insert(Lcom/mongodb/DBObject;)Lcom/mongodb/WriteResult;"
    }
}


xray("eu.coherentpaas.cqe.monetdb") {
    instrument("MonetDBStatement") {
        send("execute(Leu/coherentpaas/transactionmanager/client/TxnCtx;)Leu/coherentpaas/cqe/ResultSet;", "uuid")
    }
}

//END OF MongoDB


logger("xray",  ALL, ["STDOUT"], false)

