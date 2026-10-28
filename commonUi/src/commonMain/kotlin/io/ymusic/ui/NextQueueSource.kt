package io.ymusic.ui

import io.ymusic.dynamicengine.DynamicEngineManager
import io.ymusic.dynamicengine.yt.YtFeedItem
import io.ymusic.dynamicengine.yt.YtFeedPage
import io.ymusic.dynamicengine.yt.playlistQueue
import io.ymusic.dynamicengine.yt.queueContinuation
import io.ymusic.dynamicengine.yt.startRadioQueue
import io.ymusic.player.PlayableItem
import io.ymusic.player.QueueSource
import kmpcommon.log.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import projectcommon.json.int
import projectcommon.json.intOrNull
import projectcommon.json.string

/**
 * A queue growing through the engine's `yt/queue` (/next) pages: a video's radio, or a playlist page's
 * list past what the page had loaded. Videos already queued are skipped; it ends when a page brings no
 * continuation (a radio YouTube can't start ends after its related videos).
 */
internal class NextQueueSource private constructor(
    private val engines: DynamicEngineManager,
    private var step: Step?,
    totalCount: Int?,
    queuedVideoIds: Collection<String>,
    private val log: Logger?,
    /** Ids a page keeps out of the queue — the user's recent plays, so a radio does not repeat them. */
    private val exclude: (suspend () -> Set<String>)? = null,
    /** The artists the user listens to most; a radio interleaves them with YouTube's own order. */
    private val preferArtists: (suspend () -> Set<String>)? = null,
) : QueueSource {
    private sealed interface Step {
        data class Radio(val videoId: String) : Step

        /** [url] is a playlist page's engine link, round-tripped verbatim. */
        data class Playlist(val url: String, val videoId: String, val index: Int) : Step

        data class Continue(val token: String) : Step
    }

    override var totalCount: Int? = totalCount
        private set

    private val seen = HashSet(queuedVideoIds)

    override val resumeToken: String?
        get() = step?.let { current ->
            buildJsonObject {
                when (current) {
                    is Step.Radio -> {
                        put("step", STEP_RADIO)
                        put("videoId", current.videoId)
                    }
                    is Step.Playlist -> {
                        put("step", STEP_PLAYLIST)
                        put("url", current.url)
                        put("videoId", current.videoId)
                        put("index", current.index)
                    }
                    is Step.Continue -> {
                        put("step", STEP_CONTINUE)
                        put("continuation", current.token)
                    }
                }
                put("total", totalCount)
            }.toString()
        }

    override suspend fun loadMore(): QueueSource.Page {
        val current = step ?: return QueueSource.Page(emptyList(), hasMore = false)
        val page = try {
            fetch(current)
        } catch (c: CancellationException) {
            throw c
        } catch (e: Exception) {
            log?.log(Logger.Level.WARN, "queue page failed", e)
            throw e
        }
        step = page.continuation?.let(Step::Continue)
        page.pageHeader?.itemCount?.let { totalCount = it }
        val videos = page.sections.flatMap { it.items }.filter { it.kind == YtFeedItem.KIND_VIDEO }
        val skip = exclude?.invoke().orEmpty()
        // A page that is entirely recent plays still yields a queue: freshness never starves playback.
        val fresh = videos.filter { it.id !in skip }.ifEmpty { videos }
        val items = interleaveByTaste(fresh, preferArtists?.invoke().orEmpty())
            .filter { seen.add(it.id) }
            .map { it.toYtMedia() }
        return QueueSource.Page(items, hasMore = step != null)
    }

    /**
     * Alternates the user's artists with the rest of the page, keeping YouTube's order inside each
     * group. A radio stays related — it is still YouTube's mix — while not collapsing into one artist.
     */
    private fun interleaveByTaste(items: List<YtFeedItem>, artists: Set<String>): List<YtFeedItem> {
        if (artists.isEmpty()) return items
        val (liked, other) = items.partition { it.channelTitle?.let(artists::contains) == true }
        if (liked.isEmpty() || other.isEmpty()) return items
        val out = ArrayList<YtFeedItem>(items.size)
        for (i in 0 until maxOf(liked.size, other.size)) {
            liked.getOrNull(i)?.let(out::add)
            other.getOrNull(i)?.let(out::add)
        }
        return out
    }

    private suspend fun fetch(current: Step): YtFeedPage = when (current) {
        is Step.Radio -> engines.startRadioQueue(current.videoId, log)
        is Step.Playlist -> engines.playlistQueue(current.url, current.videoId, current.index, log)
        is Step.Continue -> engines.queueContinuation(current.token, log)
    }

    companion object {
        private const val STEP_RADIO = "radio"
        private const val STEP_PLAYLIST = "playlist"
        private const val STEP_CONTINUE = "next"

        /**
         * [videoId]'s radio, after the video itself (already queued); [exclude] keeps recent plays out
         * and [preferArtists] interleaves the user's own artists through YouTube's order.
         */
        fun radio(
            engines: DynamicEngineManager,
            videoId: String,
            log: Logger?,
            exclude: (suspend () -> Set<String>)? = null,
            preferArtists: (suspend () -> Set<String>)? = null,
        ): QueueSource = NextQueueSource(engines, Step.Radio(videoId), null, listOf(videoId), log, exclude, preferArtists)

        /** A playlist page's list past [loaded] (the page's videos, in order); null when nothing was loaded. */
        fun playlist(
            engines: DynamicEngineManager,
            url: String,
            loaded: List<YtFeedItem>,
            totalCount: Int?,
            log: Logger?,
        ): QueueSource? {
            val last = loaded.lastOrNull() ?: return null
            return NextQueueSource(engines, Step.Playlist(url, last.id, loaded.lastIndex), totalCount, loaded.map { it.id }, log)
        }

        /** The source a saved [QueueSource.resumeToken] describes, continuing past [queued]; null for a
         *  token this build can't read. */
        fun resume(token: String, engines: DynamicEngineManager, queued: List<PlayableItem>, log: Logger?): QueueSource? {
            val json = try {
                Json.parseToJsonElement(token) as JsonObject
            } catch (e: Exception) {
                log?.log(Logger.Level.WARN, "unreadable queue resume token", e)
                return null
            }
            val step = when (json.string("step")) {
                STEP_RADIO -> Step.Radio(json.string("videoId"))
                STEP_PLAYLIST -> Step.Playlist(json.string("url"), json.string("videoId"), json.int("index"))
                STEP_CONTINUE -> Step.Continue(json.string("continuation"))
                else -> return null
            }
            val queuedIds = queued.mapNotNull { (it as? PlayableItem.YtMedia)?.videoId }
            return NextQueueSource(engines, step, json.intOrNull("total"), queuedIds, log)
        }
    }
}

/**
 * The queue source continuing this playlist page's list past [loaded]. Always given, whatever the
 * page's own paging reports: /next answers an empty page at the list's end.
 */
internal fun Pager.State.Ready?.queueSource(
    engines: DynamicEngineManager,
    url: String,
    loaded: List<YtFeedItem>,
    log: Logger?,
): QueueSource? = NextQueueSource.playlist(engines, url, loaded, this?.pageHeader?.itemCount, log?.fork("queue-source"))
