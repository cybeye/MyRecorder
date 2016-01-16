package io.kickflip.sdk.util;

import android.util.Log;
import android.util.Pair;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.mobileconnectors.s3.transfermanager.TransferManager;
import com.amazonaws.mobileconnectors.s3.transfermanager.Upload;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.kickflip.sdk.av.Broadcaster;
import io.kickflip.sdk.av.SessionConfig;
import io.kickflip.sdk.event.S3UploadEvent;

/**
 * Manages a sequence of S3 uploads on behalf of
 * a single instance of {@link AWSCredentials}.
 */
public class S3BroadcastManager implements Runnable {
    private static final String TAG = "S3Manager";
    private static final boolean VERBOSE = false;

    private LinkedBlockingQueue<Pair<PutObjectRequest, Boolean>> mQueue;
    private TransferManager mTransferManager;
    private SessionConfig sessionConfig;
    private S3UploadCompleteListener uploadListener;
    private Set<WeakReference<S3RequestInterceptor>> mInterceptors;
    private boolean isRunning = true;

    private AmazonS3Client s3Client;

    public interface S3RequestInterceptor {
        public void interceptRequest(PutObjectRequest request);
    }

    public interface S3UploadCompleteListener {
        public void onS3UploadComplete(S3UploadEvent uploadEvent);
    }

    public S3BroadcastManager(SessionConfig sessionConfig, S3UploadCompleteListener listener) {

        // XXX - Need to determine what's going wrong with MD5 computation
        System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
        BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAOHTOZ74FKLHPN7GA",
                "s1Dzq3wjKmq6Ca1r7wk6ExzJSt3LuJa9+lNA3MeU");
        mTransferManager = new TransferManager(credentials);
//        setRegion("cn-north-1");
//        mTransferManager.getAmazonS3Client().setEndpoint("flash.cybeye.net.s3.cn-north-1.amazonaws.com.cn");
//        mTransferManager.getAmazonS3Client().setRegion(Region.getRegion(Regions.CN_NORTH_1));

        s3Client = new AmazonS3Client(new BasicAWSCredentials("18c00eb77b297fb8f8ef",
                "b2dd25e6d2071595a05e69d3fc678779dccf94a1"));
        s3Client.setEndpoint("oos.ctyunapi.cn");



        this.sessionConfig = sessionConfig;
        uploadListener = listener;

        mQueue = new LinkedBlockingQueue<>();
        mInterceptors = new HashSet<>();
        new Thread(this).start();

    }

    /**
     * Add an interceptor to be called on requests before they're submitted.
     * This is a good point to add request headers e.g: Cache-Control.
     * <p/>
     * WeakReferences are held on the S3RequestInterceptor so it
     * will be active as long as an external reference is held.
     */
    public void addRequestInterceptor(S3RequestInterceptor interceptor) {
        mInterceptors.add(new WeakReference<S3RequestInterceptor>(interceptor));
    }

    public void setRegion(String regionStr) {
        if (regionStr == null || regionStr.equals("")) return;
        Region region = Region.getRegion(Regions.fromName(regionStr));
        mTransferManager.getAmazonS3Client().setRegion(region);
    }


    public void queueUpload(final String bucket, final String key, final File file, boolean lastUpload) {
        if (VERBOSE) Log.i(TAG, "Queueing upload " + key);

        final PutObjectRequest por = new PutObjectRequest(bucket, key, file);
        por.setGeneralProgressListener(new ProgressListener() {
            final String url = "https://" + bucket + ".s3.amazonaws.com/" + key;
            private long uploadStartTime;

            @Override
            public void progressChanged(ProgressEvent progressEvent) {
                try {
                    if (progressEvent.getEventCode() == ProgressEvent.STARTED_EVENT_CODE) {
                        uploadStartTime = System.currentTimeMillis();
                    } else if (progressEvent.getEventCode() == ProgressEvent.COMPLETED_EVENT_CODE) {
                        long uploadDurationMillis = System.currentTimeMillis() - uploadStartTime;
                        int bytesPerSecond = (int) (file.length() / (uploadDurationMillis / 1000.0));
                        if (VERBOSE)
                            Log.i(TAG, "Uploaded " + file.length() / 1000.0 + " KB in " + (uploadDurationMillis) + "ms (" + bytesPerSecond / 1000.0 + " KBps)");
                        Log.e(TAG, "real url : " + getFileUrl(key));
                        uploadListener.onS3UploadComplete(new S3UploadEvent(file, url, bytesPerSecond));
                    } else if (progressEvent.getEventCode() == ProgressEvent.FAILED_EVENT_CODE) {
                        Log.w(TAG, "Upload failed for " + url);
                    }
                } catch (Exception excp) {
                    Log.e(TAG, "ProgressListener error");
                    excp.printStackTrace();
                }
            }
        });
        por.setCannedAcl(CannedAccessControlList.PublicRead);
        for (WeakReference<S3RequestInterceptor> ref : mInterceptors) {
            S3RequestInterceptor interceptor = ref.get();
            if (interceptor != null) {
                interceptor.interceptRequest(por);
            }
        }
        mQueue.add(new Pair<>(por, lastUpload));
    }

    private String getFileUrl(String key) {
        Date expirationDate = new Date(System.currentTimeMillis() + 60000 * 70);
        GeneratePresignedUrlRequest urlRequest = new GeneratePresignedUrlRequest("flash", key);
        urlRequest.setExpiration(expirationDate);
        URL url = mTransferManager.getAmazonS3Client().generatePresignedUrl(urlRequest);
        try {
            String urlStr = url.toURI().toString();
            return urlStr.replace("https", "http");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void run() {
        boolean lastUploadComplete = false;
        while (isRunning) {
            try {
                Pair<PutObjectRequest, Boolean> requestPair = mQueue.poll(sessionConfig.getHlsSegmentDuration() * 2, TimeUnit.SECONDS);
                if (requestPair != null) {
                    final PutObjectRequest request = requestPair.first;
//                    Upload upload = mTransferManager.upload(request);
//                    upload.waitForCompletion();
                    lastUploadComplete = requestPair.second;

                    s3Client.createBucket("cybeye");
                    s3Client.putObject(request);

                    if (!lastUploadComplete && VERBOSE)
                        Log.i(TAG, "Upload complete.");
                    else if (VERBOSE)
                        Log.i(TAG, "Last Upload complete.");
                } else {
//                    if (VERBOSE)
//                        Log.e(TAG, "Reached end of Queue before processing last segment!");
                    lastUploadComplete = true;
                }
            } catch (InterruptedException e) {
                Log.w(TAG, "InterruptedException. retrying.");
                e.printStackTrace();
            } catch (AmazonS3Exception s3e) {
                // Possible Bad Digest. Retry
                Log.w(TAG, "AmazonS3Exception. retrying.");
                s3e.printStackTrace();
            }
        }
        if (VERBOSE) Log.i(TAG, "Shutting down");
    }

    public void finish() {
        isRunning = false;
    }
}
