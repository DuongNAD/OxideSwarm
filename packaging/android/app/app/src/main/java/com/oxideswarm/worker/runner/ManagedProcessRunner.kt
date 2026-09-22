package com.oxideswarm.worker.runner

import android.content.Context
import android.util.Log
import kotlinx.coroutines.*
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ManagedProcessRunner(private val context: Context) : WorkerEngine {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var process: Process? = null
    private val isRunningFlag = AtomicBoolean(false)
    private var activeListener: WorkerEngineListener? = null
    private var supervisorJob: Job? = null

    override fun getEngineName(): String = "Managed Process Runner"

    override fun isRunning(): Boolean = isRunningFlag.get()

    /**
     * Resolves the executable path of the rusty-grid worker binary.
     * Searches standard Android paths:
     * 1. App nativeLibraryDir (packaged as librustygrid.so - W^X compliant)
     * 2. App private binary directory (/data/user/0/<pkg>/files/bin/rusty-grid)
     * 3. System temporary staging path (/data/local/tmp/rusty-grid)
     */
    fun resolveBinaryPath(): String? {
        // 1. Check nativeLibraryDir
        val libDir = context.applicationInfo.nativeLibraryDir
        val bundledLib = File(libDir, "librustygrid.so")
        if (bundledLib.exists() && bundledLib.canExecute()) {
            return bundledLib.absolutePath
        }

        // 2. Check internal files directory
        val internalBin = File(context.filesDir, "bin/rusty-grid")
        if (internalBin.exists()) {
            internalBin.setExecutable(true, false)
            if (internalBin.canExecute()) {
                return internalBin.absolutePath
            }
        }

        // 3. Check staging in /data/local/tmp
        val tmpBin = File("/data/local/tmp/rusty-grid")
        if (tmpBin.exists() && tmpBin.canExecute()) {
            return tmpBin.absolutePath
        }

        return null
    }

    override fun start(config: WorkerConfig, listener: WorkerEngineListener): Boolean {
        if (isRunningFlag.get()) {
            listener.onLogLine("[ManagedRunner] Worker process is already running")
            return true
        }

        val binaryPath = resolveBinaryPath()
        if (binaryPath == null) {
            val errMsg = "Worker binary not found. Deploy to /data/local/tmp/rusty-grid or app assets."
            Log.e(TAG, errMsg)
            listener.onStateChanged(WorkerState.ERROR, errMsg)
            listener.onLogLine("[ERROR] $errMsg")
            return false
        }

        activeListener = listener
        isRunningFlag.set(true)
        listener.onStateChanged(WorkerState.STARTING, "Launching $binaryPath...")

        supervisorJob = scope.launch {
            runProcessLoop(binaryPath, config)
        }
        return true
    }

    private suspend fun runProcessLoop(binaryPath: String, config: WorkerConfig) {
        var restartAttempts = 0
        val maxRestarts = 5

        while (isRunningFlag.get() && restartAttempts < maxRestarts) {
            try {
                val sandboxDir = File(context.cacheDir, "sandboxes").apply { mkdirs() }

                val cmd = mutableListOf(
                    binaryPath,
                    "worker",
                    "--master", config.masterAddress,
                    "--name", config.workerName,
                    "--cores", config.cores.toString(),
                    "--ram-mb", config.ramMb.toString(),
                    "--heartbeat-interval", config.heartbeatIntervalSecs.toString(),
                    "--sandbox-base-dir", sandboxDir.absolutePath
                )
                if (config.simulateGpu) {
                    cmd.add("--simulate-gpu")
                }

                val pb = ProcessBuilder(cmd)
                pb.environment().apply {
                    put("RUST_LOG", "info")
                    put("RUST_BACKTRACE", "1")
                    put("HOME", context.filesDir.absolutePath)
                    put("TMPDIR", context.cacheDir.absolutePath)
                }
                pb.directory(context.filesDir)

                Log.i(TAG, "Starting worker process: ${cmd.joinToString(" ")}")
                activeListener?.onLogLine("[Process] Spawning: ${cmd.joinToString(" ")}")

                val proc = pb.start()
                process = proc
                activeListener?.onStateChanged(WorkerState.RUNNING, "Worker active (External Process)")

                // Read stdout and stderr concurrently
                val stdoutJob = scope.launch { readStream(proc.inputStream, "[stdout]") }
                val stderrJob = scope.launch { readStream(proc.errorStream, "[stderr]") }

                val exitCode = withContext(Dispatchers.IO) {
                    proc.waitFor()
                }

                stdoutJob.join()
                stderrJob.join()

                Log.w(TAG, "Worker process exited with code $exitCode")
                activeListener?.onLogLine("[Process] Process exited with status $exitCode")

                if (!isRunningFlag.get()) {
                    // Normal stop
                    break
                }

                restartAttempts++
                val delayMs = (restartAttempts * 2000L).coerceAtMost(10000L)
                activeListener?.onStateChanged(
                    WorkerState.STARTING,
                    "Process died (exit $exitCode). Auto-restart attempt $restartAttempts/$maxRestarts in ${delayMs}ms..."
                )
                delay(delayMs)
            } catch (e: Exception) {
                Log.e(TAG, "Process execution failure", e)
                activeListener?.onLogLine("[ERROR] ${e.message}")
                if (!isRunningFlag.get()) break
                delay(3000L)
            }
        }

        isRunningFlag.set(false)
        activeListener?.onStateChanged(WorkerState.STOPPED, "Worker process terminated")
    }

    private suspend fun readStream(inputStream: java.io.InputStream, prefix: String) = withContext(Dispatchers.IO) {
        val reader = BufferedReader(InputStreamReader(inputStream))
        var activeTasks = 0
        var totalTasks = 0

        try {
            var line: String?
            while (reader.readLine().also { line = it } != null) {
                line?.let { l ->
                    activeListener?.onLogLine("$prefix $l")
                    
                    // Parse telemetry signals from standard worker log output
                    if (l.contains("Executing task") || l.contains("Task assigned")) {
                        activeTasks++
                        totalTasks++
                        activeListener?.onStatsUpdated(activeTasks, totalTasks)
                    } else if (l.contains("Task completed") || l.contains("Task failed")) {
                        activeTasks = (activeTasks - 1).coerceAtLeast(0)
                        activeListener?.onStatsUpdated(activeTasks, totalTasks)
                    }
                }
            }
        } catch (e: Exception) {
            // Stream closed
        }
    }

    override fun stop() {
        isRunningFlag.set(false)
        supervisorJob?.cancel()

        process?.let { p ->
            try {
                Log.i(TAG, "Terminating worker process gracefully...")
                p.destroy()
                if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.O) {
                    if (!p.waitFor(3, TimeUnit.SECONDS)) {
                        Log.w(TAG, "Worker did not exit in 3s, sending destroyForcibly()...")
                        p.destroyForcibly()
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "Error terminating process", e)
            }
        }
        process = null
        activeListener?.onStateChanged(WorkerState.STOPPED, "Worker process stopped")
    }

    companion object {
        private const val TAG = "ManagedProcessRunner"
    }
}
