#javac -cp .:lib/flume-ng-core-1.5.2.jar:lib/flume-ng-configuration-1.5.2.jar:lib/flume-ng-sdk-1.5.2.jar flumepgsink/FlumePgSink.java 
javac -cp .:lib/flume-ng-core-1.5.2.jar:lib/flume-ng-configuration-1.5.2.jar:lib/flume-ng-sdk-1.5.2.jar:flumepgsink/lib/mybatis-3.2.7.jar  flumepgsink/FlumePgSink.java flumepgsink/RawLog.java  flumepgsink/ErrorType.java flumepgsink/RequestType.java
