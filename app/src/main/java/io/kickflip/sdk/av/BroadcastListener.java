package io.kickflip.sdk.av;

import io.kickflip.sdk.exception.KickflipException;

/**
 * Provides callbacks for the major lifecycle benchmarks of a Broadcast.
 */
public interface BroadcastListener {
    /**
     * The broadcast has started, and is currently buffering.
     */
    public void onBroadcastStart();

    /**
     * The broadcast is fully buffered and available. This is a good time to share the broadcast.
     *
     */
    public void onBroadcastLive(String message);

    /**
     * The broadcast has ended.
     */
    public void onBroadcastStop();

    /**
     * An error occurred.
     */
    public void onBroadcastError(KickflipException error);
}
