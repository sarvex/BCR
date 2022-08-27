package com.chiller3.bcr

import android.annotation.SuppressLint
import android.content.Context
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaRecorder
import android.net.Uri
import android.os.Build
import android.os.ParcelFileDescriptor
import android.system.Os
import android.telecom.Call
import android.telecom.PhoneAccount
import android.util.Log
import androidx.documentfile.provider.DocumentFile
import com.chiller3.bcr.format.Encoder
import com.chiller3.bcr.format.Format
import com.chiller3.bcr.format.SampleRate
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatterBuilder
import java.time.format.DateTimeParseException
import java.time.format.SignStyle
import java.time.temporal.ChronoField
import kotlin.math.min
import android.os.Process as AndroidProcess

/**
 * Captures call audio and encodes it into an output file in the user's selected directory or the
 * fallback/default directory.
 *
 * @constructor Create a thread for recording a call. Note that the system only has a single
 * [MediaRecorder.AudioSource.VOICE_CALL] stream. If multiple calls are being recorded, the recorded
 * audio for each call may not be as expected.
 * @param context Used for querying shared preferences and accessing files via SAF. A reference is
 * kept in the object.
 * @param listener Used for sending completion notifications. The listener is called from this
 * thread, not the main thread.
 * @param call Used only for determining the output filename and is not saved.
 */
class RecorderThread(
    private val context: Context,
    private val listener: OnRecordingCompletedListener,
    call: Call,
) : Thread(RecorderThread::class.java.simpleName) {
    private val tag = "${RecorderThread::class.java.simpleName}/${id}"
    private val prefs = Preferences(context)
    private val isDebug = BuildConfig.DEBUG || prefs.isDebugMode

    // Thread state
    @Volatile private var isCancelled = false
    private var captureFailed = false

    // Timestamp
    private lateinit var callTimestamp: ZonedDateTime

    // Filename
    private val filenameLock = Object()
    private lateinit var filename: String
    private val redactions = HashMap<String, String>()

    // Format
    private val format: Format
    private val formatParam: UInt?
    private val sampleRate = SampleRate.fromPreferences(prefs)

    // Sources
    //private val sources = arrayOf(MediaRecorder.AudioSource.VOICE_CALL)
    private val sources = arrayOf(
        MediaRecorder.AudioSource.VOICE_UPLINK,
        MediaRecorder.AudioSource.VOICE_DOWNLINK,
    )

    init {
        Log.i(tag, "Created thread for call: $call")

        onCallDetailsChanged(call.details)

        val savedFormat = Format.fromPreferences(prefs)
        format = savedFormat.first
        formatParam = savedFormat.second
    }

    private fun redact(msg: String): String {
        synchronized(filenameLock) {
            var result = msg

            for ((source, target) in redactions) {
                result = result.replace(source, target)
            }

            return result
        }
    }

    fun redact(uri: Uri): String = redact(Uri.decode(uri.toString()))

    /**
     * Update [filename] with information from [details].
     *
     * This function holds a lock on [filenameLock] until it returns.
     */
    fun onCallDetailsChanged(details: Call.Details) {
        synchronized(filenameLock) {
            redactions.clear()

            filename = buildString {
                val instant = Instant.ofEpochMilli(details.creationTimeMillis)
                callTimestamp = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault())
                append(FORMATTER.format(callTimestamp))

                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                    when (details.callDirection) {
                        Call.Details.DIRECTION_INCOMING -> append("_in")
                        Call.Details.DIRECTION_OUTGOING -> append("_out")
                        Call.Details.DIRECTION_UNKNOWN -> {}
                    }
                }

                if (details.handle?.scheme == PhoneAccount.SCHEME_TEL) {
                    append('_')
                    append(details.handle.schemeSpecificPart)

                    redactions[details.handle.schemeSpecificPart] = "<phone number>"
                }

                val callerName = details.callerDisplayName?.trim()
                if (!callerName.isNullOrBlank()) {
                    append('_')
                    append(callerName)

                    redactions[callerName] = "<caller name>"
                }

                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                    val contactName = details.contactDisplayName?.trim()
                    if (!contactName.isNullOrBlank()) {
                        append('_')
                        append(contactName)

                        redactions[contactName] = "<contact name>"
                    }
                }
            }
            // AOSP's SAF automatically replaces invalid characters with underscores, but just
            // in case an OEM fork breaks that, do the replacement ourselves to prevent
            // directory traversal attacks.
                .replace('/', '_').trim()

            Log.i(tag, "Updated filename due to call details change: ${redact(filename)}")
        }
    }

    override fun run() {
        var success = false
        var errorMsg: String? = null
        var resultUri: Uri? = null

        try {
            Log.i(tag, "Recording thread started")

            if (isCancelled) {
                Log.i(tag, "Recording cancelled before it began")
            } else {
                val initialFilename = synchronized(filenameLock) { filename }
                val outputFile = createFileInDefaultDir(initialFilename, format.mimeTypeContainer)
                resultUri = outputFile.uri

                try {
                    openFile(outputFile).use {
                        recordUntilCancelled(it)
                    }
                } finally {
                    val finalFilename = synchronized(filenameLock) { filename }
                    if (finalFilename != initialFilename) {
                        Log.i(tag, "Renaming ${redact(initialFilename)} to ${redact(finalFilename)}")

                        if (outputFile.renameTo(finalFilename)) {
                            resultUri = outputFile.uri
                        } else {
                            Log.w(tag, "Failed to rename to final filename: ${redact(finalFilename)}")
                        }
                    }

                    tryMoveToUserDir(outputFile)?.let {
                        resultUri = it.uri
                    }

                    processRetention()
                }

                success = !captureFailed
            }
        } catch (e: Exception) {
            Log.e(tag, "Error during recording", e)
            errorMsg = e.localizedMessage
        } finally {
            Log.i(tag, "Recording thread completed")

            try {
                if (isDebug) {
                    Log.d(tag, "Dumping logcat due to debug mode")
                    dumpLogcat()
                }
            } catch (e: Exception) {
                Log.w(tag, "Failed to dump logcat", e)
            }

            if (success) {
                listener.onRecordingCompleted(this, resultUri!!)
            } else {
                listener.onRecordingFailed(this, errorMsg, resultUri)
            }
        }
    }

    /**
     * Cancel current recording. This stops capturing audio after processing the next minimum buffer
     * size, but the thread does not exit until all data encoded so far has been written to the
     * output file.
     *
     * If called before [start], the thread will not record any audio not create an output file. In
     * this scenario, [OnRecordingCompletedListener.onRecordingFailed] will be called with a null
     * [Uri].
     */
    fun cancel() {
        isCancelled = true
    }

    private fun dumpLogcat() {
        val outputFile = createFileInDefaultDir("${filename}.log", "text/plain")

        try {
            openFile(outputFile).use {
                val process = ProcessBuilder("logcat", "-d").start()
                try {
                    val data = process.inputStream.use { stream -> stream.readBytes() }
                    Os.write(it.fileDescriptor, data, 0, data.size)
                } finally {
                    process.waitFor()
                }
            }
        } finally {
            tryMoveToUserDir(outputFile)
        }
    }

    /**
     * Delete files older than the specified retention period.
     *
     * The "current time" is [callTimestamp], not the actual current time and the timestamp of past
     * recordings is based on the filename, not the file modification time. Incorrectly-named files
     * are ignored.
     */
    private fun processRetention() {
        val directory = prefs.outputDir?.let {
            // Only returns null on API <21
            DocumentFile.fromTreeUri(context, it)!!
        } ?: DocumentFile.fromFile(prefs.defaultOutputDir)

        val retention = when (val r = Retention.fromPreferences(prefs)) {
            NoRetention -> {
                Log.i(tag, "Keeping all existing files")
                return
            }
            is DaysRetention -> r.toDuration()
        }
        Log.i(tag, "Retention period is $retention")

        for (item in directory.listFiles()) {
            val filename = item.name ?: continue
            val redacted = redactTruncate(filename)

            val timestamp = timestampFromFilename(filename)
            if (timestamp == null) {
                Log.w(tag, "Ignoring unrecognized filename: $redacted")
                continue
            }

            val diff = Duration.between(timestamp, callTimestamp)

            if (diff > retention) {
                Log.i(tag, "Deleting $redacted ($timestamp)")
                if (!item.delete()) {
                    Log.w(tag, "Failed to delete: $redacted")
                }
            }
        }
    }

    /**
     * Try to move [sourceFile] to the user output directory.
     *
     * @return Whether the user output directory is set and the file was successfully moved
     */
    private fun tryMoveToUserDir(sourceFile: DocumentFile): DocumentFile? {
        val userDir = prefs.outputDir?.let {
            // Only returns null on API <21
            DocumentFile.fromTreeUri(context, it)!!
        } ?: return null

        val redactedSource = redact(sourceFile.uri)

        return try {
            val targetFile = moveFileToDir(sourceFile, userDir)
            val redactedTarget = redact(targetFile.uri)

            Log.i(tag, "Successfully moved $redactedSource to $redactedTarget")
            sourceFile.delete()

            targetFile
        } catch (e: Exception) {
            Log.e(tag, "Failed to move $redactedSource to $userDir", e)
            null
        }
    }

    /**
     * Move [sourceFile] to [targetDir].
     *
     * @return The [DocumentFile] for the newly moved file.
     */
    private fun moveFileToDir(sourceFile: DocumentFile, targetDir: DocumentFile): DocumentFile {
        val targetFile = createFileInDir(targetDir, sourceFile.name!!, sourceFile.type!!)

        try {
            openFile(sourceFile).use { sourcePfd ->
                FileInputStream(sourcePfd.fileDescriptor).use { sourceStream ->
                    openFile(targetFile).use { targetPfd ->
                        FileOutputStream(targetPfd.fileDescriptor).use { targetStream ->
                            val sourceChannel = sourceStream.channel
                            val targetChannel = targetStream.channel

                            var offset = 0L
                            var remain = sourceChannel.size()

                            while (remain > 0) {
                                val n = targetChannel.transferFrom(sourceChannel, offset, remain)
                                offset += n
                                remain -= n
                            }
                        }
                    }
                }
            }

            sourceFile.delete()
            return targetFile
        } catch (e: Exception) {
            targetFile.delete()
            throw e
        }
    }

    /**
     * Create [name] in the default output directory.
     *
     * @param name Should not contain a file extension
     * @param mimeType Determines the file extension
     *
     * @throws IOException if the file could not be created in the default directory
     */
    private fun createFileInDefaultDir(name: String, mimeType: String): DocumentFile {
        val defaultDir = DocumentFile.fromFile(prefs.defaultOutputDir)
        return createFileInDir(defaultDir, name, mimeType)
    }

    /**
     * Create a new file with name [name] inside [dir].
     *
     * @param name Should not contain a file extension
     * @param mimeType Determines the file extension
     *
     * @throws IOException if file creation fails
     */
    private fun createFileInDir(dir: DocumentFile, name: String, mimeType: String): DocumentFile {
        Log.d(tag, "Creating ${redact(name)} with MIME type $mimeType in ${dir.uri}")

        return dir.createFile(mimeType, name)
            ?: throw IOException("Failed to create file in ${dir.uri}")
    }

    /**
     * Open seekable file descriptor to [file].
     *
     * @throws IOException if [file] cannot be opened
     */
    private fun openFile(file: DocumentFile): ParcelFileDescriptor =
        context.contentResolver.openFileDescriptor(file.uri, "rw")
            ?: throw IOException("Failed to open file at ${file.uri}")

    /**
     * Record from [sources], with interleaving channels, until [cancel] is called or an audio
     * capture or encoding error occurs.
     *
     * [pfd] does not get closed by this method.
     */
    @SuppressLint("MissingPermission")
    private fun recordUntilCancelled(pfd: ParcelFileDescriptor) {
        AndroidProcess.setThreadPriority(AndroidProcess.THREAD_PRIORITY_URGENT_AUDIO)

        val minBufSize = AudioRecord.getMinBufferSize(
            sampleRate.value.toInt(), CHANNEL_CONFIG, ENCODING)
        if (minBufSize < 0) {
            throw Exception("Failure when querying minimum buffer size: $minBufSize")
        }
        Log.d(tag, "AudioRecord minimum buffer size: $minBufSize")

        val audioRecords = ArrayList<AudioRecord>(sources.size)

        try {
            sources.forEach {
                val ar = AudioRecord(
                    it,
                    sampleRate.value.toInt(),
                    CHANNEL_CONFIG,
                    ENCODING,
                    // On some devices, MediaCodec occasionally has sudden spikes in processing time, so
                    // use a larger internal buffer to reduce the chance of overrun on the recording
                    // side.
                    minBufSize * 6,
                )

                Log.d(tag, "Created AudioRecord instance:")
                Log.d(tag, "- Source: ${ar.audioSource}")
                Log.d(tag, "- Initial buffer size in frames: ${ar.bufferSizeInFrames}")
                Log.d(tag, "- Format: ${ar.format}")

                audioRecords.add(ar)
            }
        } catch (e: Exception) {
            audioRecords.forEach(AudioRecord::release)
            throw e
        }

        // Where's my RAII? :(
        try {
            audioRecords.forEach(AudioRecord::startRecording)

            try {
                val container = format.getContainer(pfd.fileDescriptor)

                try {
                    val mediaFormat = format.getMediaFormat(sources.size, sampleRate, formatParam)
                    val encoder = format.getEncoder(mediaFormat, container)

                    try {
                        encoder.start()

                        try {
                            encodeLoop(audioRecords, encoder, minBufSize)
                        } finally {
                            encoder.stop()
                        }
                    } finally {
                        encoder.release()
                    }
                } finally {
                    container.release()
                }
            } finally {
                audioRecords.forEach(AudioRecord::stop)
            }
        } finally {
            audioRecords.forEach(AudioRecord::release)
        }
    }

    /**
     * Main loop for encoding captured raw audio into an output file.
     *
     * The loop runs forever until [cancel] is called. At that point, no further data will be read
     * from any of the [audioRecords] and the remaining output data from [encoder] will be written
     * to the output file. If any of the [audioRecords] fails to capture data, the loop will behave
     * as if [cancel] was called (ie. abort, but ensuring that the output file is valid).
     *
     * The approximate amount of time to cancel reading from the audio source is the time it takes
     * to process the minimum buffer size. Additionally, additional time is needed to write out the
     * remaining encoded data to the output file.
     *
     * @param audioRecords [AudioRecord.startRecording] must have been called on every instance
     * @param encoder [Encoder.start] must have been called
     * @param bufSize Size of buffer to use for each [AudioRecord.read] operation
     *
     * @throws Exception if the audio recorder or encoder encounters an error
     */
    private fun encodeLoop(audioRecords: List<AudioRecord>, encoder: Encoder, bufSize: Int) {
        var numFrames = 0L
        val frameSize = BYTES_PER_SAMPLE * audioRecords.size

        // Use a slightly larger buffer to reduce the chance of problems under load
        val baseSize = bufSize * 2

        // Input buffers for each AudioRecord
        val buffersIn = audioRecords.map { ByteBuffer.allocateDirect(baseSize) }
        // Output buffer for interleaved channels
        val bufferOut = ByteBuffer.allocateDirect(baseSize * audioRecords.size)
        // Size of output buffer in frames
        val bufferFrames = bufferOut.capacity().toLong() / frameSize
        // Size of output buffer in nanoseconds
        val bufferNs = bufferFrames * 1_000_000_000L / sampleRate.value.toInt()

        outer@ while (!isCancelled) {
            val begin = System.nanoTime()

            for ((audioRecord, buffer) in audioRecords.zip(buffersIn)) {
                val nRead = audioRecord.read(buffer, buffer.remaining())
                // An unexpected EOF is an error too
                if (nRead <= 0) {
                    Log.e(tag, "Failed to read from source ${audioRecord.audioSource}: $nRead")
                    isCancelled = true
                    captureFailed = true
                    break@outer
                }

                buffer.limit(nRead)
            }
            val recordElapsed = System.nanoTime() - begin

            val interleaveBegin = System.nanoTime()
            val nWritten = interleaveChannels(buffersIn, bufferOut)
            val interleaveElapsed = System.nanoTime() - interleaveBegin

            val encodeBegin = System.nanoTime()
            encoder.encode(bufferOut, false)
            val encodeElapsed = System.nanoTime() - encodeBegin

            // Move unused data in the input buffers to the beginning. Clear output buffer since it
            // is guaranteed to have been fully written to the container.
            buffersIn.forEach(ByteBuffer::compact)
            bufferOut.clear()

            numFrames += nWritten / frameSize

            val totalElapsed = System.nanoTime() - begin
            if (encodeElapsed > bufferNs) {
                Log.w(tag, "${encoder.javaClass.simpleName} took too long: " +
                        "timestamp=${numFrames.toDouble() / sampleRate.value.toDouble()}s, " +
                        "buffer=${bufferNs / 1_000_000.0}ms, " +
                        "total=${totalElapsed / 1_000_000.0}ms, " +
                        "record=${recordElapsed / 1_000_000.0}ms, " +
                        "interleave=${interleaveElapsed / 1_000_000.0}ms, " +
                        "encode=${encodeElapsed / 1_000_000.0}ms")
            }
        }

        // Signal EOF with empty buffer
        Log.d(tag, "Sending EOF to encoder")
        bufferOut.limit(bufferOut.position())
        encoder.encode(bufferOut, true)

        val durationSecs = numFrames.toDouble() / sampleRate.value.toDouble()
        Log.d(tag, "Input complete after ${"%.1f".format(durationSecs)}s")
    }

    companion object {
        private const val CHANNEL_CONFIG = AudioFormat.CHANNEL_IN_MONO
        private const val ENCODING = AudioFormat.ENCODING_PCM_16BIT
        const val BYTES_PER_SAMPLE = 2

        // Eg. 20220429_180249.123-0400
        private val FORMATTER = DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendLiteral('_')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendOffset("+HHMMss", "+0000")
            .toFormatter()

        private fun timestampFromFilename(name: String): ZonedDateTime? {
            try {
                // Date is before first separator
                val first = name.indexOf('_')
                if (first < 0 || first == name.length - 1) {
                    return null
                }

                val second = name.indexOf('_', first + 1)
                if (second < 0) {
                    return null
                }

                return ZonedDateTime.parse(name.substring(0, second), FORMATTER)
            } catch (e: DateTimeParseException) {
                // Ignore
            }

            return null
        }

        private fun redactTruncate(msg: String): String = buildString {
            val n = 2

            if (msg.length > 2 * n) {
                append(msg.substring(0, n))
            }
            append("<...>")
            if (msg.length > 2 * n) {
                append(msg.substring(msg.length - n))
            }
        }

        /**
         * Read samples of size [BYTES_PER_SAMPLE] from [buffersIn] and write them to [bufferOut]
         * interleaved.
         *
         * Only complete frames will be written to [bufferOut]. For example, if [buffersIn] has
         * [2 samples remaining, 3 samples remaining], then only 2 frames are written to
         * [bufferOut]. After the copy is complete, [bufferOut] will be flipped so that it is ready
         * to be read from.
         *
         * @return The number of bytes written to [bufferOut].
         */
        private fun interleaveChannels(buffersIn: List<ByteBuffer>, bufferOut: ByteBuffer): Int {
            val data = ByteArray(BYTES_PER_SAMPLE)
            // Bytes copied per channel
            var copied = 0

            // Max bytes that can be copied per channel. Avoid unnecessary allocations with regular
            // for loop.
            var toCopy = bufferOut.remaining() / buffersIn.size
            for (buffer in buffersIn) {
                toCopy = min(toCopy, buffer.remaining())
            }

            while (toCopy - copied >= data.size) {
                buffersIn.forEach {
                    it.get(data)
                    bufferOut.put(data)
                }

                copied += data.size
            }

            bufferOut.flip()

            return copied * buffersIn.size
        }
    }

    interface OnRecordingCompletedListener {
        /**
         * Called when the recording completes successfully. [uri] is the output file.
         */
        fun onRecordingCompleted(thread: RecorderThread, uri: Uri)

        /**
         * Called when an error occurs during recording. If [uri] is not null, it points to the
         * output file containing partially recorded audio. If [uri] is null, then either the output
         * file could not be created or the thread was cancelled before it was started.
         */
        fun onRecordingFailed(thread: RecorderThread, errorMsg: String?, uri: Uri?)
    }
}