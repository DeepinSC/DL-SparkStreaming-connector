/**
 * Created by rick on 2017/9/29.
 */

import com.twitter.distributedlog.*;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.FutureEventListener;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.CharEncoding;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Random;


public class DLStreamLoader {
    public final static String REDIS_HOST = "localhost";
    public final static int NUM_CAMPAIGNS = 100;

    public static ArrayList<String> make_ids(int n){
        ArrayList<String> ids_list = new ArrayList<String>();
        for (int i=0;i<n;i++){
            ids_list.add(java.util.UUID.randomUUID().toString());
        }
        return ids_list;
    }

    public static ArrayList<String> gen_ads(int n){
        Jedis jedis = new Jedis(REDIS_HOST);
        jedis.flushAll();
        Set<String> campaigns = jedis.smembers("campaigns");
        int campaigns_num_cur = campaigns.size();
        ArrayList<String> ads_set = new ArrayList<String>();

        //create remaining campaigns
        if(campaigns_num_cur<NUM_CAMPAIGNS){
            System.out.println("Campaigns num not enough, start creating Campaigns");
            ArrayList<String> campaigns_remain = make_ids(NUM_CAMPAIGNS);
            for (String campaign : campaigns_remain){
                jedis.sadd("campaigns",campaign);
            }
        }
        // create ads(10 times of NUM_CAMPAIGNS)
        // new campaigns
        campaigns = jedis.smembers("campaigns");
        for (String campaign : campaigns){
            ArrayList<String> ads = make_ids(10);
            for (String ad:ads){
                jedis.set(campaign,ad);
                ads_set.add(ad);
            }

        }
        System.out.println(NUM_CAMPAIGNS*10+" ads written in redis "+REDIS_HOST);
        return ads_set;
    }



    //temp
    public static DistributedLogNamespace get_dl_namespace(String dlUriStr) throws IOException {
        URI uri = URI.create(dlUriStr);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setImmediateFlushEnabled(true);
        conf.setOutputBufferSize(0);
        conf.setPeriodicFlushFrequencyMilliSeconds(0);
        conf.setLockTimeout(100);
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .regionId(DistributedLogConstants.LOCAL_REGION_ID)
                .clientId(" DLStreamLoader")
                .build();
        return namespace;
    }


    public static String make_dl_event(String ad, String page_id, String user_id, long created_time){

        String[] ad_types = {"banner", "modal", "sponsored-search", "mail", "mobile"};
        String[] event_types  = {"view", "click", "purchase"};
        String ad_type = ad_types[new Random().nextInt(ad_types.length)];
        String event_type = event_types[new Random().nextInt(event_types.length)];

        String str = String.format("{\"user_id\": \"%s\", \"page_id\": \"%s\", \"ad_id\": \"%s\", \"ad_type\": \"%s\", \"event_type\": \"%s\", \"event_time\": \"%d\", \"ip_address\": \"1.2.3.4\"}",
                user_id,
                page_id,
                ad,
                ad_type,
                event_type,
                created_time);
        return str;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        // Thread to write records into DL
        class Create_DL_Events_Thread extends Thread {
            String dl_event;
            long sleep_time;
            DistributedLogManager dlm;
            AsyncLogWriter writer;

            Create_DL_Events_Thread(String dl_event,
                                    long sleep_time,
                                    DistributedLogManager dlm,
                                    AsyncLogWriter writer){

                this.dl_event = dl_event;
                this.sleep_time = sleep_time;
                this.dlm = dlm;
                this.writer = writer;
            }
            public void run() {
                try {
                    Thread.sleep(sleep_time);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // DL records write here
                DistributedLogManager dlm = null;
                try {
                    //namespace = get_dl_namespace("distributedlog://127.0.0.1:8000/messaging/my_namespace");
                    byte[] data = dl_event.getBytes(CharEncoding.UTF_8);
                    LogRecord record = new LogRecord(System.currentTimeMillis(), data);
                    writer.write(new LogRecord(System.currentTimeMillis(), data)).addEventListener(new FutureEventListener<DLSN>() {

                        public void onFailure(Throwable cause) {
                            System.out.println("Encountered error on writing data");
                            cause.printStackTrace(System.err);
                            Runtime.getRuntime().exit(0);
                            // executed when write failed.
                        }

                        public void onSuccess(DLSN value) {
                            System.out.println(">");
                        }
                    });

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
        String dlUriStr = args[0];
        long throughput = Long.parseLong(args[1]);

        //Init DL stuff
        final DistributedLogNamespace namespace = get_dl_namespace(dlUriStr);
        Iterator<String> streams= namespace.getLogs();
        final List<String> streamNameList  = IteratorUtils.toList(streams);
        final List<DistributedLogManager> dlms = new ArrayList<DistributedLogManager>();
        final List<AsyncLogWriter> writers = new ArrayList<AsyncLogWriter>();
        for (String stream:streamNameList){
            DistributedLogManager dlm = namespace.openLog(stream);
            AsyncLogWriter writer = FutureUtils.result(dlm.openAsyncLogWriter());
            dlms.add(dlm);
            writers.add(writer);

        }

        ArrayList<String> ads = gen_ads(100);
        ArrayList<String> page_ids = make_ids(100);
        ArrayList<String> user_ids = make_ids(100);
        long start_time_ns =  System.currentTimeMillis()* 1000000;
        long period_ns = 1000000000/throughput;
        int counter = 0;


        for (long per_time =period_ns;per_time<=1000000000;per_time+=period_ns){
            //Thread needs to sleep until next time_point
            long created_time = (start_time_ns + per_time)/1000000;
            long sleep_time = created_time - System.currentTimeMillis();
            String ad = ads.get(new Random().nextInt(ads.size()));
            String page_id = page_ids.get(new Random().nextInt(page_ids.size()));
            String user_id = user_ids.get(new Random().nextInt(user_ids.size()));
            // write records by round-robin
            int stream_id = counter%streamNameList.size();
            DistributedLogManager dlm = dlms.get(stream_id);
            AsyncLogWriter writer = writers.get(stream_id);
            //String stream = streamNameList.get(stream_id);
            String dlevent = make_dl_event(ad, page_id, user_id, created_time);
            Create_DL_Events_Thread event_thread = new Create_DL_Events_Thread(dlevent,sleep_time,dlm,writer);
            event_thread.start();
            counter++;
        }

        //close stuff

        class CloseThread implements Runnable{
            public void run() {

                for (int i =0;i<streamNameList.size();i++){
                    writers.get(i).asyncClose();
                    try {
                        dlms.get(i).close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                namespace.close();
            }
        }
        Thread close = new Thread(new CloseThread());
        //sleep 10 seconds
        close.sleep(10000);
        close.start();

        System.out.println(counter+" Threads created and finished.");
    }




}
