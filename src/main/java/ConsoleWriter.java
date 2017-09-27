/**
 * Created by rick on 2017/9/27.
 */
/**
 * Created by rick on 2017/9/6.
 */

import com.twitter.distributedlog.*;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import jline.ConsoleReader;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;

public class ConsoleWriter {
    private final static String HELP = "ConsoleWriter <uri> <string>";
    private final static String PROMPT_MESSAGE = "[dlog] > ";

    public static void main(String[] args) throws Exception {

        if(args.length<2){
            System.err.println("Error: 2 parameter needed!");
            System.exit(1);
        }
        String dlUriStr = args[0];
        final String streamName = args[1];

        URI uri = URI.create(dlUriStr);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setImmediateFlushEnabled(true);
        conf.setOutputBufferSize(0);
        conf.setPeriodicFlushFrequencyMilliSeconds(0);
        conf.setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE);
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .regionId(DistributedLogConstants.LOCAL_REGION_ID)
                .clientId("console-writer")
                .build();

        // open the dlm
        System.out.println("Opening log stream " + streamName);
        DistributedLogManager dlm = namespace.openLog(streamName);
        long lasttxid = -1;
        if (dlm.getLogRecordCount()!=0)
            lasttxid = dlm.getLastTxId();
        System.out.println(dlm.getFirstTxId());
        System.out.println(lasttxid);

        try {

            AsyncLogWriter writer = null;
            try {
                writer = FutureUtils.result(dlm.openAsyncLogWriter());
                long cur_last_txid = lasttxid;
                ConsoleReader reader = new ConsoleReader();
                String line;
                while ((line = reader.readLine(PROMPT_MESSAGE)) != null) {
                    writer.write(new LogRecord(cur_last_txid+1, line.getBytes(UTF_8)))
                            .addEventListener(new FutureEventListener<DLSN>() {
                                //@Override
                                public void onFailure(Throwable cause) {
                                    System.out.println("Encountered error on writing data");
                                    cause.printStackTrace(System.err);
                                    Runtime.getRuntime().exit(0);
                                }

                                //@Override
                                public void onSuccess(DLSN value) {
                                    System.out.println("\nSuccess");

                                    // done
                                }
                            });
                    cur_last_txid+=1;
                }
            } finally {
                if (null != writer) {
                    FutureUtils.result(writer.asyncClose(), Duration.apply(5, TimeUnit.SECONDS));
                }
            }
        } finally {
            dlm.close();
            namespace.close();
        }
    }
}
