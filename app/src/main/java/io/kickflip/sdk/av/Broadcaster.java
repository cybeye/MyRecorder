package io.kickflip.sdk.av;

import android.content.Context;
import android.util.Log;
import android.util.Pair;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;

import io.kickflip.sdk.util.FileUtils;
import io.kickflip.sdk.event.BroadcastIsLiveEvent;
import io.kickflip.sdk.event.HlsManifestWrittenEvent;
import io.kickflip.sdk.event.HlsSegmentWrittenEvent;
import io.kickflip.sdk.event.MuxerFinishedEvent;
import io.kickflip.sdk.event.S3UploadEvent;
import io.kickflip.sdk.event.StreamLocationAddedEvent;
import io.kickflip.sdk.event.ThumbnailWrittenEvent;
import io.kickflip.sdk.util.S3BroadcastManager;

// TODO: Make HLS / RTMP Agnostic
public class Broadcaster extends AVRecorder implements S3BroadcastManager.S3UploadCompleteListener {
    private static final String TAG = "Broadcaster";
    private static final boolean VERBOSE = true;
    private static final int MIN_BITRATE = 3 * 100 * 1000;              // 300 kbps
    private final String VOD_FILENAME = "vod.m3u8";
    private Context mContext;
    private HlsFileObserver mFileObserver;
    private ArrayDeque<Pair<String, File>> mUploadQueue;
    private SessionConfig mConfig;
    private BroadcastListener mBroadcastListener;
    private EventBus mEventBus;
    private boolean mReadyToBroadcast;                                  // Kickflip user registered and endpoint ready
    private boolean mSentBroadcastLiveEvent;
    private int mVideoBitrate;
    private File mManifestSnapshotDir;                                  // Directory where manifest snapshots are stored
    private File mVodManifest;                                          // VOD HLS Manifest containing complete history
    private int mNumSegmentsWritten;
    private int mLastRealizedBandwidthBytesPerSec;                      // Bandwidth snapshot for adapting bitrate
    private boolean mDeleteAfterUploading;                              // Should recording files be deleted as they're uploaded?
    private S3BroadcastManager mS3Manager;
    private ObjectMetadata mS3ManifestMeta;


    public Broadcaster(Context context, SessionConfig config) throws IOException {
        super(config);
        init();
        mContext = context;
        mConfig = config;
        mConfig.getMuxer().setEventBus(mEventBus);
        mVideoBitrate = mConfig.getVideoBitrate();
        if (VERBOSE) Log.i(TAG, "Initial video bitrate : " + mVideoBitrate);
        mManifestSnapshotDir = new File(mConfig.getOutputPath().substring(0, mConfig.getOutputPath().lastIndexOf("/") + 1), "m3u8");
        mManifestSnapshotDir.mkdir();
        mVodManifest = new File(mManifestSnapshotDir, VOD_FILENAME);
        writeEventManifestHeader(mConfig.getHlsSegmentDuration());

        String watchDir = config.getOutputDirectory().getAbsolutePath();
        mFileObserver = new HlsFileObserver(watchDir, mEventBus);
        mFileObserver.startWatching();
        if (VERBOSE) Log.i(TAG, "Watching " + watchDir);
        mS3Manager = new S3BroadcastManager(mConfig, this);
        mS3Manager.addRequestInterceptor(mS3RequestInterceptor);
        mReadyToBroadcast = true;
    }

    private void init() {
        mDeleteAfterUploading = true;
        mLastRealizedBandwidthBytesPerSec = 0;
        mNumSegmentsWritten = 0;
        mSentBroadcastLiveEvent = false;
        mEventBus = new EventBus("Broadcaster");
        mEventBus.register(this);
    }

    public void setDeleteLocalFilesAfterUpload(boolean doDelete) {
        if (!isRecording()) {
            mDeleteAfterUploading = doDelete;
        }
    }

    public void setBroadcastListener(BroadcastListener listener) {
        mBroadcastListener = listener;
    }

    public EventBus getEventBus() {
        return mEventBus;
    }

    @Override
    public void startRecording() {
        super.startRecording();

    }

    public boolean isLive() {
        return mSentBroadcastLiveEvent;
    }

    @Override
    public void stopRecording() {
        super.stopRecording();
        mSentBroadcastLiveEvent = false;

    }

    @Subscribe
    public void onSegmentWritten(HlsSegmentWrittenEvent event) {
        try {
            File hlsSegment = event.getSegment();
            queueOrSubmitUpload(keyForFilename(hlsSegment.getName()), hlsSegment);
            if (mConfig.isAdaptiveBitrate() && isRecording()) {
                // Adjust bitrate to match expected filesize
                long actualSegmentSizeBytes = hlsSegment.length();
                long expectedSizeBytes = ((mConfig.getAudioBitrate() / 8) + (mVideoBitrate / 8)) * mConfig.getHlsSegmentDuration();
                float filesizeRatio = actualSegmentSizeBytes / (float) expectedSizeBytes;
                if (VERBOSE)
                    Log.i(TAG, "OnSegmentWritten. Segment size: " + (actualSegmentSizeBytes / 1000) + "kB. ratio: " + filesizeRatio);
                if (filesizeRatio < .7) {
                    if (mLastRealizedBandwidthBytesPerSec != 0) {
                        // Scale bitrate while not exceeding available bandwidth
                        float scaledBitrate = mVideoBitrate * (1 / filesizeRatio);
                        float bandwidthBitrate = mLastRealizedBandwidthBytesPerSec * 8;
                        mVideoBitrate = (int) Math.min(scaledBitrate, bandwidthBitrate);
                    } else {
                        // Scale bitrate to match expected fileSize
                        mVideoBitrate *= (1 / filesizeRatio);
                    }
                    if (VERBOSE) Log.i(TAG, "Scaling video bitrate to " + mVideoBitrate + " bps");
                    //// TODO: 16-1-12 根据当前上传速度调整视频的比特率
                    adjustVideoBitrate(mVideoBitrate);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void onSegmentUploaded(S3UploadEvent uploadEvent) {
        if (mDeleteAfterUploading) {
            boolean deletedFile = uploadEvent.getFile().delete();
            if (VERBOSE)
                Log.i(TAG, "Deleting uploaded segment. " + uploadEvent.getFile().getAbsolutePath() + " Succcess: " + deletedFile);
        }
        try {
            if (mConfig.isAdaptiveBitrate() && isRecording()) {
                mLastRealizedBandwidthBytesPerSec = uploadEvent.getUploadByteRate();
                // Adjust video encoder bitrate per bandwidth of just-completed upload
                if (VERBOSE) {
                    Log.i(TAG, "Bandwidth: " + (mLastRealizedBandwidthBytesPerSec / 1000.0) + " kBps. Encoder: " + ((mVideoBitrate + mConfig.getAudioBitrate()) / 8) / 1000.0 + " kBps");
                }
                if (mLastRealizedBandwidthBytesPerSec < (((mVideoBitrate + mConfig.getAudioBitrate()) / 8))) {
                    // The new bitrate is equal to the last upload bandwidth, never inferior to MIN_BITRATE, nor superior to the initial specified bitrate
                    mVideoBitrate = Math.max(Math.min(mLastRealizedBandwidthBytesPerSec * 8, mConfig.getVideoBitrate()), MIN_BITRATE);
                    if (VERBOSE) {
                        Log.i(TAG, String.format("Adjusting video bitrate to %f kBps. Bandwidth: %f kBps",
                                mVideoBitrate / (8 * 1000.0), mLastRealizedBandwidthBytesPerSec / 1000.0));
                    }
                    adjustVideoBitrate(mVideoBitrate);
                }
            }
        } catch (Exception e) {
            Log.i(TAG, "OnSegUpload excep");
            e.printStackTrace();
        }
    }

    @Subscribe
    public void onManifestUpdated(HlsManifestWrittenEvent e) {
        if (!isRecording()) {
            if (mBroadcastListener != null) {
                if (VERBOSE) Log.i(TAG, "Sending onBroadcastStop");
                mBroadcastListener.onBroadcastStop();
            }
        }
        if (VERBOSE) Log.i(TAG, "onManifestUpdated. Last segment? " + !isRecording());
        // Copy m3u8 at this moment and queue it to uploading
        // service
        final File copy = new File(mManifestSnapshotDir, e.getManifestFile().getName()
                .replace(".m3u8", "_" + mNumSegmentsWritten + ".m3u8"));
        try {
            if (VERBOSE)
                Log.i(TAG, "Copying " + e.getManifestFile().getAbsolutePath() + " to " + copy.getAbsolutePath());
            FileUtils.copy(e.getManifestFile(), copy);
            queueOrSubmitUpload(keyForFilename("index.m3u8"), copy);
            appendLastManifestEntryToEventManifest(copy, !isRecording());
        } catch (IOException e1) {
            Log.e(TAG, "Failed to copy manifest file. Upload of this manifest cannot proceed. Stream will have a discontinuity!");
            e1.printStackTrace();
        }

        mNumSegmentsWritten++;
    }

    private void onManifestUploaded(S3UploadEvent uploadEvent) {
        if (mDeleteAfterUploading) {
            if (VERBOSE) Log.i(TAG, "Deleting " + uploadEvent.getFile().getAbsolutePath());
            uploadEvent.getFile().delete();
            String uploadUrl = uploadEvent.getDestinationUrl();
            if (uploadUrl.substring(uploadUrl.lastIndexOf(File.separator) + 1).equals("vod.m3u8")) {
                if (VERBOSE) Log.i(TAG, "Deleting " + mConfig.getOutputDirectory());
                mFileObserver.stopWatching();
                FileUtils.deleteDirectory(mConfig.getOutputDirectory());
                mS3Manager.finish();
            }
        }
        if (!mSentBroadcastLiveEvent) {
            mEventBus.post(new BroadcastIsLiveEvent("http://oos.ctyunapi.cn/cybeye/index.m3u8"));
            mSentBroadcastLiveEvent = true;
            if (mBroadcastListener != null)
                mBroadcastListener.onBroadcastLive("live");
        }
    }

    /**
     * A thumbnail was written in the recording directory.
     * <p/>
     * Called on a background thread
     */
    @Subscribe
    public void onThumbnailWritten(ThumbnailWrittenEvent e) {
        try {
            queueOrSubmitUpload(keyForFilename("thumb.jpg"), e.getThumbnailFile());
        } catch (Exception ex) {
            Log.i(TAG, "Error writing thumbanil");
            ex.printStackTrace();
        }
    }

    /**
     * A thumbnail upload completed.
     * <p/>
     * Called on a background thread
     */
    private void onThumbnailUploaded(S3UploadEvent uploadEvent) {
        if (mDeleteAfterUploading) uploadEvent.getFile().delete();
    }

    @Subscribe
    public void onStreamLocationAdded(StreamLocationAddedEvent event) {
//        sendStreamMetaData();
    }

    @Subscribe
    public void onDeadEvent(DeadEvent e) {
        if (VERBOSE) Log.i(TAG, "DeadEvent ");
    }


    @Subscribe
    public void onMuxerFinished(MuxerFinishedEvent e) {
        // TODO: Broadcaster uses AVRecorder reset()

    }

    /**
     * Construct an S3 Key for a given filename
     */
    private String keyForFilename(String fileName) {
        return "" + fileName;
    }

    /**
     * Handle an upload, either submitting to the S3 client
     * or queueing for submission once credentials are ready
     *
     * @param key  destination key
     * @param file local file
     */
    private void queueOrSubmitUpload(String key, File file) {
        if (mReadyToBroadcast) {
            submitUpload(key, file);
        } else {
            if (VERBOSE) Log.i(TAG, "queueing " + key + " until S3 Credentials available");
            queueUpload(key, file);
        }
    }

    /**
     * Queue an upload for later submission to S3
     *
     * @param key  destination key
     * @param file local file
     */
    private void queueUpload(String key, File file) {
        if (mUploadQueue == null)
            mUploadQueue = new ArrayDeque<>();
        mUploadQueue.add(new Pair<>(key, file));
    }

    /**
     * Submit all queued uploads to the S3 client
     */
    private void submitQueuedUploadsToS3() {
        if (mUploadQueue == null) return;
        for (Pair<String, File> pair : mUploadQueue) {
            submitUpload(pair.first, pair.second);
        }
    }

    private void submitUpload(final String key, final File file) {
        submitUpload(key, file, false);
    }

    private void submitUpload(final String key, final File file, boolean lastUpload) {
        mS3Manager.queueUpload("cybeye", key, file, lastUpload);
    }

    /**
     * An S3 Upload completed.
     * <p/>
     * Called on a background thread
     */
    public void onS3UploadComplete(S3UploadEvent uploadEvent) {
        if (VERBOSE) Log.i(TAG, "Upload completed for " + uploadEvent.getDestinationUrl());
        if (uploadEvent.getDestinationUrl().contains(".m3u8")) {
            onManifestUploaded(uploadEvent);
        } else if (uploadEvent.getDestinationUrl().contains(".ts")) {
            onSegmentUploaded(uploadEvent);
        } else if (uploadEvent.getDestinationUrl().contains(".jpg")) {
            onThumbnailUploaded(uploadEvent);
        }
    }

    public SessionConfig getSessionConfig() {
        return mConfig;
    }

    private void writeEventManifestHeader(int targetDuration) {
        FileUtils.writeStringToFile(
                String.format("#EXTM3U\n" +
                        "#EXT-X-PLAYLIST-TYPE:VOD\n" +
                        "#EXT-X-VERSION:3\n" +
                        "#EXT-X-MEDIA-SEQUENCE:0\n" +
                        "#EXT-X-TARGETDURATION:%d\n", targetDuration + 1),
                mVodManifest, false
        );
    }

    private void appendLastManifestEntryToEventManifest(File sourceManifest, boolean lastEntry) {
        String result = FileUtils.tail2(sourceManifest, lastEntry ? 3 : 2);
        FileUtils.writeStringToFile(result, mVodManifest, true);
        if (lastEntry) {
            submitUpload(keyForFilename("vod.m3u8"), mVodManifest, true);
            if (VERBOSE) Log.i(TAG, "Queued master manifest " + mVodManifest.getAbsolutePath());
        }
    }

    S3BroadcastManager.S3RequestInterceptor mS3RequestInterceptor = new S3BroadcastManager.S3RequestInterceptor() {
        @Override
        public void interceptRequest(PutObjectRequest request) {
            if (request.getKey().contains("index.m3u8")) {
                if (mS3ManifestMeta == null) {
                    mS3ManifestMeta = new ObjectMetadata();
                    mS3ManifestMeta.setCacheControl("max-age=0");
                }
                request.setMetadata(mS3ManifestMeta);
            }
        }
    };
}
