/**
 * Created by rick on 2017/9/29.
 */
import com.google.common.collect.Lists;
import com.twitter.distributedlog.*;
import com.twitter.distributedlog.client.DistributedLogMultiStreamWriter;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import jline.ConsoleReader;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
public class MultiStreamWriter {
    private final static String HELP = "<finagle-name> <namespace>";
    private final static String PROMPT_MESSAGE = "[dlog] > ";

    public static void main(String[] args) throws Exception {
        if (2 != args.length) {
            System.out.println(HELP);
            return;
        }

        String dlUriStr = args[0];
        String fileName = args[1];

        URI uri = URI.create(dlUriStr);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setImmediateFlushEnabled(true);
        conf.setOutputBufferSize(0);
        conf.setPeriodicFlushFrequencyMilliSeconds(0);
        //conf.setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE);
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .regionId(DistributedLogConstants.LOCAL_REGION_ID)
                .clientId(" MultiStreamWriter")
                .build();
        Iterator<String> streams= namespace.getLogs();
        final List<String> streamNameList  = IteratorUtils.toList(streams);


        FileInputStream inputStream = new FileInputStream(fileName);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));


        int length = streamNameList.size();
        System.out.println(length);

        String str;
        while((str = bufferedReader.readLine()) != null) {

            //for (int i = 0;i<length;i++){


            DistributedLogManager dlm = namespace.openLog(streamNameList.get(1));
            AsyncLogWriter writer = FutureUtils.result(dlm.openAsyncLogWriter());

            // Writer here!
            byte[] data = str.getBytes(CharEncoding.UTF_8);
            LogRecord record = new LogRecord(System.currentTimeMillis(), data);
            Future<DLSN> writeFuture = writer.write(record);
            writeFuture.addEventListener(new FutureEventListener<DLSN>() {

                public void onFailure(Throwable cause) {
                    System.out.println("Encountered error on writing data");
                    cause.printStackTrace(System.err);
                    Runtime.getRuntime().exit(0);
                    // executed when write failed.
                }


                public void onSuccess(DLSN value) {
                    //System.out.println("write completed!\n");
                    //Runtime.getRuntime().exit(0);
                    // executed when write completed.

                }
            });
            dlm.close();
            writer.asyncClose();
        //}
            System.out.println(str);

        }

        inputStream.close();
        bufferedReader.close();
        namespace.close();
    }
}
