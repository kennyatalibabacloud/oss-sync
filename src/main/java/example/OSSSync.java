package example;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.Credentials;
import com.aliyun.fc.runtime.FunctionComputeLogger;
import com.aliyun.fc.runtime.StreamRequestHandler;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import net.sf.json.*;

import java.io.*;
import java.util.*;


public class OSSSync implements StreamRequestHandler {

    private static String OSS_ENDPOINT = "";
    private String SINK_BUCKET = "";
    private String ACCESS_KEY_ID = "";
    private String ACCESS_KEY_SECRET = "";
    private String SINK_ENDPOINT = "";

    private static final List<String> addEventList = Arrays.asList(
            "ObjectCreated:PutObject", "ObjectCreated:PostObject");
    private static final List<String> updateEventList = Arrays.asList(
            "ObjectCreated:AppendObject");
    private static final List<String> deleteEventList = Arrays.asList(
            "ObjectRemoved:DeleteObject", "ObjectRemoved:DeleteObjects");


    protected OSSClient getOSSClient(Context context) {
        //FunctionComputeLogger fcLogger = context.getLogger();

        Credentials creds = context.getExecutionCredentials();
        return new OSSClient(
                OSS_ENDPOINT, creds.getAccessKeyId(), creds.getAccessKeySecret(), creds.getSecurityToken());
    }

    protected OSSClient getOSSSinkClient(Context context) {
        Credentials creds = context.getExecutionCredentials();
        //return new OSSClient(SINK_ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET);
        return new OSSClient(
                SINK_ENDPOINT, creds.getAccessKeyId(), creds.getAccessKeySecret(), creds.getSecurityToken());
    }

    protected void closeQuietly(BufferedReader reader, FunctionComputeLogger fcLogger) {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (Exception ex) {
            fcLogger.error(ex.getMessage());
        }
    }

    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        FunctionComputeLogger fcLogger = context.getLogger();
        try {
            fcLogger.info("trigger called");
        } catch (Exception ex) {
            fcLogger.error(ex.getMessage());
        }

        fcLogger.info("Start handling");

        Map<String, String> env = System.getenv();
        if (env.containsKey("SINK_ENDPOINT")) {
            SINK_ENDPOINT = env.get("SINK_ENDPOINT");
            //fcLogger.info("SINK_ENDPOINT is " + SINK_ENDPOINT);
        }
        if (env.containsKey("OSS_ENDPOINT")) {
            OSS_ENDPOINT = env.get("OSS_ENDPOINT");
            //fcLogger.info("OSS_ENDPOINT is " + OSS_ENDPOINT);
        }
        if (env.containsKey("ACCESS_KEY_ID")) {
            ACCESS_KEY_ID = env.get("ACCESS_KEY_ID");
        }
        if (env.containsKey("ACCESS_KEY_SECRET")) {
            ACCESS_KEY_SECRET = env.get("ACCESS_KEY_SECRET");
        }

        if (env.containsKey("SINK_BUCKET")) {
            SINK_BUCKET = env.get("SINK_BUCKET");
        }


        JSONObject ossEvent;
        StringBuilder inputBuilder = new StringBuilder();
        BufferedReader streamReader = null;
        try {
            streamReader = new BufferedReader(new InputStreamReader(input));
            String line;
            while ((line = streamReader.readLine()) != null) {
                inputBuilder.append(line);
            }
            //fcLogger.info("Read object event success.");
        } catch (Exception ex) {
            fcLogger.error(ex.getMessage());
            return;
        } finally {
            closeQuietly(streamReader, fcLogger);
        }
        ossEvent = JSONObject.fromObject(inputBuilder.toString());

        fcLogger.info("Getting event: " + ossEvent.toString());


        JSONArray events = ossEvent.getJSONArray("events");
        for (int i = 0; i < events.size(); i++) {
            // Get event name, source, oss object.
            JSONObject event = events.getJSONObject(i);
            String eventName = event.getString("eventName");
            JSONObject oss = event.getJSONObject("oss");

            // Get bucket name and file name for file identifier.
            JSONObject bucket = oss.getJSONObject("bucket");
            String bucketName = bucket.getString("name");
            JSONObject object = oss.getJSONObject("object");
            String fileName = object.getString("key");
            //fcLogger.info("bucketName=" + bucketName);
            //fcLogger.info("fileName=" + fileName);

            if (deleteEventList.contains(eventName)) {
                fcLogger.info("prepare to delete file in sink [" + fileName +
                        "] ");
                OSSClient ossSinkClient = getOSSSinkClient(context);
                if (ossSinkClient.doesObjectExist(SINK_BUCKET, fileName)) {
                    ossSinkClient.deleteObject(SINK_BUCKET, fileName);
                } else {
                    fcLogger.info("file[" + fileName +
                            "] not not found in bucket - " + SINK_BUCKET);
                }
            } else {
                fcLogger.info("prepare to sync file to sink [" + fileName +
                        "] ");
                OSSClient ossClient = getOSSClient(context);
                if (ossClient.doesObjectExist(bucketName, fileName)) {
                    OSSObject ossObject = ossClient.getObject(bucketName, fileName);
                    OSSClient ossSinkClient = getOSSSinkClient(context);
                    ossSinkClient.putObject(SINK_BUCKET, fileName, ossObject.getObjectContent());
                    fcLogger.info("file[" + fileName +
                            "] copied to sink");
                } else {
                    fcLogger.info("file[" + fileName +
                            "] not not found in bucket - " + bucketName);
                }
            }

        }


    }
}
