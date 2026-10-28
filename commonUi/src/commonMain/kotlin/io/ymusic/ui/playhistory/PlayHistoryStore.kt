package io.ymusic.ui.playhistory

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlDriver
import io.ymusic.dynamicengine.yt.YtFeedItem
import io.ymusic.dynamicengine.yt.YtThumb
import io.ymusic.mediastore.model.MediaStoreItem
import io.ymusic.ui.youtubeThumbUrl
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.withContext
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

/**
 * Watch-time ranking for Library Recent play / Top tracks and Discover Top tracks.
 *
 * Local rows are keyed by [MediaStoreItem.playlistCode] so a move/rescan that keeps the file still
 * ranks. YouTube watch time is keyed by video id; display fields live in yt_video_metadata and are
 * filled only from a YouTube play (a tagged local file can rank without a card until then).
 */
class PlayHistoryStore(
    private val driver: SqlDriver,
    private val retain: Int = DEFAULT_RETAIN,
    private val metadataQuota: Int = METADATA_QUOTA,
) {
    private val io = playHistoryDispatcher()

    /** "Play again" saved queues — same database file. */
    val playAgain = PlayAgainStore(driver)

    private var historyCleaned = false
    private var metadataSaves = 0

    private val _changes = MutableSharedFlow<Unit>(extraBufferCapacity = 1)
    val changes: Flow<Unit> = _changes.asSharedFlow()

    suspend fun trackLocal(code: String, watchTimeMs: Long) {
        require(watchTimeMs >= 0)
        if (watchTimeMs == 0L || code.isEmpty()) return
        withContext(io) {
            cleanupHistoryIfNeeded()
            upsertWatchTime("local_play_history", "code", code, watchTimeMs)
        }
        _changes.tryEmit(Unit)
    }

    suspend fun trackYoutube(videoId: String, watchTimeMs: Long) {
        require(watchTimeMs >= 0)
        if (watchTimeMs == 0L || videoId.isEmpty()) return
        withContext(io) {
            cleanupHistoryIfNeeded()
            upsertWatchTime("yt_play_history", "video_id", videoId, watchTimeMs)
        }
        _changes.tryEmit(Unit)
    }

    /** The most recently played YouTube video ids, newest first — a radio excludes them to stay fresh. */
    suspend fun youtubeRecentVideoIds(limit: Int = RECENT_FOR_FRESH): List<String> = withContext(io) {
        queryIds("SELECT video_id FROM yt_play_history ORDER BY tracked_at DESC LIMIT ?", limit)
    }

    /** The artists with the most total watch time — the taste signal a radio ranks toward. */
    suspend fun topArtists(limit: Int = TOP_ARTISTS): Set<String> = withContext(io) {
        queryIds(
            """
            SELECT m.artist FROM yt_video_metadata m
            JOIN yt_play_history h ON h.video_id = m.video_id
            WHERE m.artist IS NOT NULL AND m.artist <> ''
            GROUP BY m.artist
            ORDER BY SUM(h.watch_time) DESC
            LIMIT ?
            """.trimIndent(),
            limit,
        ).toSet()
    }

    /** Cumulative watch time recorded for [videoId]; 0 when it was never played. */
    suspend fun youtubeWatchTimeMs(videoId: String): Long = withContext(io) {
        if (videoId.isEmpty()) return@withContext 0L
        driver.executeQuery(
            null,
            "SELECT watch_time FROM yt_play_history WHERE video_id = ?",
            { cursor -> QueryResult.Value(if (cursor.next().value) cursor.getLong(0) ?: 0L else 0L) },
            1,
        ) { bindString(0, videoId) }.value
    }

    /** Snapshot for the Discover chart. Live streams are skipped; a thin card only inserts if missing. */
    suspend fun saveYoutubeMetadata(
        videoId: String,
        title: String,
        artist: String? = null,
        thumbnailUrl: String? = null,
        duration: String? = null,
        durationMs: Long? = null,
        live: Boolean = false,
    ) {
        if (live || videoId.isEmpty() || title.isEmpty()) return
        withContext(io) {
            val ts = now()
            val replace = durationMs != null && durationMs > 0
            if (replace) {
                // INSERT OR IGNORE + UPDATE, not an upsert: ON CONFLICT … DO UPDATE needs SQLite 3.24 (Android 11+).
                driver.execute(
                    null,
                    """
                    INSERT OR IGNORE INTO yt_video_metadata(
                        video_id, title, artist, thumbnail_url, duration, duration_ms, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """.trimIndent(),
                    7,
                ) {
                    bindString(0, videoId)
                    bindString(1, title)
                    bindString(2, artist)
                    bindString(3, thumbnailUrl)
                    bindString(4, duration)
                    bindLong(5, durationMs)
                    bindLong(6, ts)
                }
                driver.execute(
                    null,
                    """
                    UPDATE yt_video_metadata SET
                        title = ?, artist = ?, thumbnail_url = ?, duration = ?, duration_ms = ?, last_updated = ?
                    WHERE video_id = ?
                    """.trimIndent(),
                    7,
                ) {
                    bindString(0, title)
                    bindString(1, artist)
                    bindString(2, thumbnailUrl)
                    bindString(3, duration)
                    bindLong(4, durationMs)
                    bindLong(5, ts)
                    bindString(6, videoId)
                }
            } else {
                val updated = driver.execute(
                    null,
                    "UPDATE yt_video_metadata SET last_updated = ? WHERE video_id = ?",
                    2,
                ) {
                    bindLong(0, ts)
                    bindString(1, videoId)
                }.value
                if (updated == 0L) {
                    driver.execute(
                        null,
                        """
                        INSERT INTO yt_video_metadata(
                            video_id, title, artist, thumbnail_url, duration, duration_ms, last_updated
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """.trimIndent(),
                        7,
                    ) {
                        bindString(0, videoId)
                        bindString(1, title)
                        bindString(2, artist)
                        bindString(3, thumbnailUrl)
                        bindString(4, duration)
                        bindLong(5, durationMs)
                        bindLong(6, ts)
                    }
                }
            }
            metadataSaves++
            if (metadataSaves == 1 || metadataSaves % METADATA_CLEANUP_EVERY == 0) {
                cleanupTable("yt_video_metadata", "video_id", "last_updated", metadataQuota)
            }
        }
        _changes.tryEmit(Unit)
    }

    suspend fun localTopChart(limit: Int = retain): List<String> = withContext(io) {
        queryIds(
            "SELECT code FROM local_play_history ORDER BY watch_time DESC, tracked_at DESC LIMIT ?",
            limit,
        )
    }

    suspend fun localRecent(limit: Int = retain): List<String> = withContext(io) {
        queryIds(
            "SELECT code FROM local_play_history ORDER BY tracked_at DESC LIMIT ?",
            limit,
        )
    }

    suspend fun deleteLocal(code: String) {
        if (code.isEmpty()) return
        withContext(io) {
            driver.execute(null, "DELETE FROM local_play_history WHERE code = ?", 1) {
                bindString(0, code)
            }
        }
        _changes.tryEmit(Unit)
    }

    suspend fun deleteYoutube(videoId: String) {
        if (videoId.isEmpty()) return
        withContext(io) {
            driver.execute(null, "DELETE FROM yt_play_history WHERE video_id = ?", 1) {
                bindString(0, videoId)
            }
        }
        _changes.tryEmit(Unit)
    }

    suspend fun youtubeTopChart(limit: Int = retain): List<YtFeedItem> = withContext(io) {
        val ids = queryIds(
            "SELECT video_id FROM yt_play_history ORDER BY watch_time DESC, tracked_at DESC LIMIT ?",
            limit,
        )
        if (ids.isEmpty()) return@withContext emptyList()
        val placeholders = ids.joinToString(",") { "?" }
        val byId = driver.executeQuery(
            null,
            """
            SELECT video_id, title, artist, thumbnail_url, duration, duration_ms
            FROM yt_video_metadata
            WHERE video_id IN ($placeholders)
            """.trimIndent(),
            { cursor ->
                val map = mutableMapOf<String, YtFeedItem>()
                while (cursor.next().value) {
                    val videoId = cursor.getString(0)!!
                    val thumb = cursor.getString(3)
                    map[videoId] = YtFeedItem(
                        kind = YtFeedItem.KIND_VIDEO,
                        id = videoId,
                        title = cursor.getString(1)!!,
                        subtitleLine1 = cursor.getString(2),
                        channelTitle = cursor.getString(2),
                        thumbs = listOf(YtThumb(thumb ?: youtubeThumbUrl(videoId))),
                        duration = cursor.getString(4),
                        durationMs = cursor.getLong(5),
                    )
                }
                QueryResult.Value(map)
            },
            ids.size,
        ) {
            ids.forEachIndexed { i, id -> bindString(i, id) }
        }.value
        ids.mapNotNull { byId[it] }
    }

    @OptIn(ExperimentalTime::class)
    private fun now(): Long = Clock.System.now().toEpochMilliseconds()

    private fun upsertWatchTime(table: String, idColumn: String, id: String, watchTimeMs: Long) {
        val ts = now()
        // INSERT OR IGNORE + UPDATE, not an upsert: ON CONFLICT … DO UPDATE needs SQLite 3.24 (Android 11+).
        driver.execute(null, "INSERT OR IGNORE INTO $table($idColumn, watch_time, tracked_at) VALUES (?, 0, ?)", 2) {
            bindString(0, id)
            bindLong(1, ts)
        }
        driver.execute(
            null,
            "UPDATE $table SET watch_time = watch_time + ?, tracked_at = ? WHERE $idColumn = ?",
            3,
        ) {
            bindLong(0, watchTimeMs)
            bindLong(1, ts)
            bindString(2, id)
        }
    }

    private fun queryIds(sql: String, limit: Int): List<String> =
        driver.executeQuery(
            null,
            sql,
            { cursor ->
                val out = mutableListOf<String>()
                while (cursor.next().value) {
                    out += cursor.getString(0)!!
                }
                QueryResult.Value(out)
            },
            1,
        ) { bindLong(0, limit.toLong()) }.value

    private fun cleanupHistoryIfNeeded() {
        if (historyCleaned) return
        historyCleaned = true
        cleanupTable("local_play_history", "code", "tracked_at", retain)
        cleanupTable("yt_play_history", "video_id", "tracked_at", retain)
    }

    private fun cleanupTable(table: String, idColumn: String, orderColumn: String, keep: Int) {
        driver.execute(
            null,
            """
            DELETE FROM $table
            WHERE $idColumn IN (
                SELECT $idColumn FROM $table ORDER BY $orderColumn DESC LIMIT 999 OFFSET ?
            )
            """.trimIndent(),
            1,
        ) { bindLong(0, keep.toLong()) }
    }

    companion object {
        const val DEFAULT_RETAIN = 200
        const val METADATA_QUOTA = 100
        const val RECENT_FOR_FRESH = 40
        const val TOP_ARTISTS = 15
        private const val METADATA_CLEANUP_EVERY = 10
    }
}

/** [codes] order (chart / recency) mapped onto the current library; missing files drop out. */
fun mediaItemsByPlayHistoryCodes(
    library: List<MediaStoreItem>,
    codes: List<String>,
): List<MediaStoreItem> {
    val map = library.associateBy { it.playlistCode }
    return codes.mapNotNull { map[it] }
}
