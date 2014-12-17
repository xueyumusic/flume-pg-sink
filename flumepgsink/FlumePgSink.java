package flumepgsink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Date;
import java.text.SimpleDateFormat;

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
        //2014-12-12 10:06:42,002 INFO  execute - HttpProxy null 0 HttpResponse 90000318622 0 3 Successful Message:null
        //Date, log4j level, class, classic, IP, port, type, orgid, counts, forward_time, action, messages
        Status status = null;

        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
                Event event = ch.take();
                byte[] datas = event.getBody();
                String ds = new String(datas, "UTF-8");
	        System.out.println("##try:"+ds);
                String[] dsarr = ds.split("\\s+");

                /*for (int i=0; i<dsarr.length; i++) {
                  System.out.println(Integer.toString(i));
                  System.out.println(dsarr[i]);
                }*/

                if (dsarr.length >= 14 && dsarr[2].equals("INFO")) {

			//session.insert("flumepgsink.FlumePgMapper.insertContent", dsarr[0]);
			RawLog curobj = new RawLog();

			curobj.setClasstype(dsarr[3]);
			curobj.setClassic(dsarr[5]);
			curobj.setIp(dsarr[6]);
			curobj.setPort(Integer.valueOf(dsarr[7]));
			curobj.setType(dsarr[8]);
			curobj.setOrg_id(dsarr[9]);
			curobj.setCounts(Integer.valueOf(dsarr[10]));
			curobj.setForward_time(Integer.valueOf(dsarr[11]));
			curobj.setAction(dsarr[12]);

			SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
			Date time = fmt.parse(dsarr[0]+" "+dsarr[1]);
			curobj.setTime(time);

			StringBuffer sb = new StringBuffer();
			for (int i=13;i<dsarr.length;i++) {
				sb.append(dsarr[i]);
			}
			curobj.setMessage(new String(sb));
			session.insert("flumepgsink.FlumePgMapper.insertRawLog", curobj);
			System.out.println("###insert obj");
			
			if (dsarr[8].equals("HttpRequest")) {
			  RequestType aobj = new RequestType();
 			  aobj.setType(dsarr[8]);
                          aobj.setOrg_id(dsarr[9]);
                          aobj.setOrg_name("");
                          aobj.setMessage(new String(sb));
                          aobj.setTime(time);
                          session.insert("flumepgsink.FlumePgMapper.insertHttpRequest", aobj);
			}

			session.commit();

			txn.commit();
			status = Status.READY;
                } // end info
                else if (dsarr.length >= 6 && dsarr[2].equals("ERROR")) {
                  //2014-12-12 11:03:47,970 ERROR channelInactive - The channel is inactive.
                  System.out.println("###have ERROR in log");
                  ErrorType curobj = new ErrorType();
                  curobj.setType("");
                  curobj.setClass_name(dsarr[3]);
                  StringBuffer cursb = new StringBuffer();
                  for (int i = 5; i<dsarr.length; i++) {
                    cursb.append(dsarr[i]);
                  }
                  curobj.setMessage(new String(cursb));
                  SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
                  Date time = fmt.parse(dsarr[0]+" "+dsarr[1]);
                  curobj.setTime(time);

                  session.insert("flumepgsink.FlumePgMapper.insertErrorLog", curobj);
                  session.commit();
                  status = Status.READY;
                  txn.commit();
                } else {
                  if (dsarr.length >= 3) {
                    System.out.println("##ds type:"+dsarr[2]);
                    System.out.println("##ds len:"+Integer.toString(dsarr.length));
                    if (dsarr[2] == "INFO") {
                      System.out.println("#####yes info wield");
                    } else {
                      System.out.println("#####not equal info");
                    }
                  }
                  System.out.println("###other situation");
                  status = Status.BACKOFF;
                  txn.commit();
                }
        } catch (Throwable t) {
                System.out.println("###throw exception");
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
		  String resource = "flumepgsink/mybatis-config-119.xml";
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
