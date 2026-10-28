package io.ymusic.ui

import io.ymusic.download.DownloadState
import io.ymusic.dynamicengine.yt.YtFeedItem
import io.ymusic.dynamicengine.yt.YtThumb
import io.ymusic.player.PlayableItem
import io.ymusic.settings.AppSettings
import io.ymusic.settings.flow
import io.ymusic.ui.playhistory.PlayHistoryStore

/**
 * Auto-downloads a track once its cumulative watch time reaches
 * [AppSettings.autoDownloadTopPlayedMinutes] — the "most played" rule behind
 * [AppSettings.autoDownloadTopPlayed]. A background download cannot stop to ask for a folder, so it
 * runs only when a usable download location is already set; otherwise it silently skips.
 */
internal class AutoDownloader(private val graph: AppGraph) {

    suspend fun onWatchTimeRecorded(item: PlayableItem.YtMedia, history: PlayHistoryStore) {
        if (item.live) return
        val downloads = graph.downloads ?: return
        if (!graph.settings.flow(AppSettings.autoDownloadTopPlayed).value) return
        val thresholdMs = graph.settings.flow(AppSettings.autoDownloadTopPlayedMinutes).value * MINUTE_MS
        if (thresholdMs <= 0L) return
        if (history.youtubeWatchTimeMs(item.videoId) < thresholdMs) return
        if (!hasUsableDownloadLocation(graph)) return
        // A canceled task is the user saying no: it does not block a later automatic download.
        if (downloads.snapshot().any { it.videoId == item.videoId && it.state != DownloadState.CANCELED }) return
        downloads.enqueue(graph.quickDownload.value.request(item.toFeedItem(), graph.mp3SourceBitrate()))
    }

    private companion object {
        const val MINUTE_MS = 60_000L
    }
}

private fun PlayableItem.YtMedia.toFeedItem() = YtFeedItem(
    kind = YtFeedItem.KIND_VIDEO,
    id = videoId,
    title = title ?: videoId,
    channelTitle = artist,
    channelUrl = channelUrl,
    thumbs = listOfNotNull(thumbnailUrl?.let { YtThumb(it) }),
    duration = duration,
    durationMs = durationMs,
    live = live,
)
