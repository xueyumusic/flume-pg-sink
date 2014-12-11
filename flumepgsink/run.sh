
#./bin/flume-ng agent --conf conf/ --conf-file conf/flume-pg.conf --name pg -Dflume.root.logger=INFO,console --classpath flumepgsink.FlumePgSink 
./bin/flume-ng agent --conf conf/ --conf-file conf/flume-pg.conf --name pg -Dflume.root.logger=INFO,console --classpath .:./flumepgsink/lib/mybatis-3.2.7.jar:./flumepgsink/lib/postgresql-9.3-1101.jdbc4.jar
