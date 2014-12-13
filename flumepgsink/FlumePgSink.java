package flumepgsink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Date;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

public class FlumePgSink extends AbstractSink implements Configurable {
 
	private String queueSize;
	private String resultFile;
	private PrintWriter out;
        private SqlSession session;
    	
	
	
	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			Event event = ch.take();
			byte[] datas = event.getBody();
			String ds = new String(datas, "UTF-8");
			String[] dsarr = ds.split(" ");
			System.out.println(dsarr[0]);
			out.println(dsarr[0]);

                        //session.insert("flumepgsink.FlumePgMapper.insertContent", dsarr[0]);
                        RawLog curobj = new RawLog();
                        curobj.setClasstype("xueyuclasstype");
                        curobj.setTime(new Date());
                        session.insert("flumepgsink.FlumePgMapper.insertContent", curobj);
                        System.out.println("###insert obj");
                        session.commit();
			
			txn.commit();
			status = Status.READY;
		} catch (Throwable t) {
                        //System.out.println("###throw exception");
                        //t.printStackTrace();
			txn.rollback();
			status = Status.BACKOFF;
			if (t instanceof Error) {
				throw (Error)t;
			}
		} finally {
			txn.close();
		}
		return status;
	}

        protected void finalize() {
               System.out.println("#############over finalized");
               session.close();
        }

	@Override
	public void configure(Context context) {
		String queueSize = context.getString("queueSize", "10000");
		String resultFile = context.getString("resultFile", "/tmp/flume/result1.txt");
		this.queueSize = queueSize;
		this.resultFile = resultFile;
		
		try {
		  FileWriter outFile = new FileWriter(this.resultFile);
		  PrintWriter out = new PrintWriter(outFile);
		  this.out = out;
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
		  String resource = "flumepgsink/mybatis-config.xml";
		  InputStream inputmybatisstream = Resources.getResourceAsStream(resource);
		  SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputmybatisstream);
		  SqlSession session = sqlSessionFactory.openSession();
                  this.session = session;

		  //String firstContent = session.selectOne("flumepgsink.FlumePgMapper.selectFirst", 1);
		  //Map<String, String> selectmap = session.selectOne("flumepgsink.FlumePgMapper.selectFirst", 1);
	          //System.out.println("####!!!!  First mybatis:" + firstContent);
                  /*for ( String key : selectmap.keySet() ) {
                    String value = selectmap.get(key);
                    System.out.println("#####selectmap key:" + key + "  value:" + value);
                  }*/
					  
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
