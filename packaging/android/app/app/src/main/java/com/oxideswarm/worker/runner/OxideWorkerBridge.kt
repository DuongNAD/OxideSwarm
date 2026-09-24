package com.oxideswarm.worker.runner

import android.util.Log

object OxideWorkerBridge : WorkerEngine {
    private const val TAG = "OxideWorkerBridge"
    private var isLibraryAvailable = false
    private var activeListener: WorkerEngineListener? = null
    private var running = false

    init {
        try {
            System.loadLibrary("oxideworker")
            isLibraryAvailable = true
            Log.i(TAG, "Native library 'liboxideworker.so' loaded successfully via JNI")
        } catch (e: UnsatisfiedLinkError) {
            isLibraryAvailable = false
            Log.w(TAG, "Native library 'liboxideworker.so' not found in apk jniLibs. Fallback to ManagedProcessRunner is enabled.", e)
        }
    }

    fun isAvailable(): Boolean = isLibraryAvailable

    override fun getEngineName(): String = if (isLibraryAvailable) "In-Process JNI" else "JNI (Unavailable)"

    override fun isRunning(): Boolean {
        if (!isLibraryAvailable) return false
        return try {
            nativeIsRunning()
        } catch (e: Throwable) {
            running
        }
    }

    override fun start(config: WorkerConfig, listener: WorkerEngineListener): Boolean {
        if (!isLibraryAvailable) {
            listener.onStateChanged(WorkerState.ERROR, "Native JNI library not loaded")
            return false
        }
        activeListener = listener
        listener.onStateChanged(WorkerState.STARTING, "Initializing Rust Tokio runtime in-process...")

        return try {
            val started = nativeStartWorker(
                masterAddr = config.masterAddress,
                p2pTicket = config.p2pTicket ?: "",
                workerName = config.workerName,
                cores = config.cores,
                ramMb = config.ramMb,
                simulateGpu = config.simulateGpu,
                heartbeatIntervalSecs = config.heartbeatIntervalSecs
            )
            if (started) {
                running = true
                listener.onStateChanged(WorkerState.RUNNING, "Worker running in-process (PID ${android.os.Process.myPid()})")
            } else {
                listener.onStateChanged(WorkerState.ERROR, "JNI nativeStartWorker failed")
            }
            started
        } catch (e: Throwable) {
            Log.e(TAG, "Exception in nativeStartWorker", e)
            listener.onStateChanged(WorkerState.ERROR, "JNI exception: ${e.message}")
            false
        }
    }

    override fun stop() {
        if (!isLibraryAvailable || !running) return
        try {
            nativeStopWorker()
        } catch (e: Throwable) {
            Log.e(TAG, "Exception in nativeStopWorker", e)
        } finally {
            running = false
            activeListener?.onStateChanged(WorkerState.STOPPED, "Worker stopped gracefully")
            activeListener = null
        }
    }

    fun updateTelemetry(
        batteryPct: Int,
        isCharging: Boolean,
        thermalThrottled: Boolean,
        batteryTemperature: Float = -1.0f,
        networkType: String = "unknown"
    ): Boolean {
        if (!isLibraryAvailable) return false
        return try {
            nativeUpdateTelemetryDetailed(
                batteryPct,
                isCharging,
                thermalThrottled,
                batteryTemperature,
                networkType
            )
        } catch (e: Throwable) {
            try {
                nativeUpdateTelemetry(batteryPct, isCharging, thermalThrottled)
            } catch (e2: Throwable) {
                Log.w(TAG, "Exception in nativeUpdateTelemetry", e2)
                false
            }
        }
    }

    fun startMaster(bindPort: Int = 8088, dashboardPort: Int = 8080, enableP2p: Boolean = true): String {
        if (!isLibraryAvailable) return "{\"error\":\"Native library unavailable\"}"
        return try {
            nativeStartMaster(bindPort, dashboardPort, enableP2p)
        } catch (e: Throwable) {
            Log.e(TAG, "Exception in nativeStartMaster", e)
            "{\"error\":\"${e.message}\"}"
        }
    }

    fun stopMaster(): Boolean {
        if (!isLibraryAvailable) return false
        return try {
            nativeStopMaster()
        } catch (e: Throwable) {
            Log.e(TAG, "Exception in nativeStopMaster", e)
            false
        }
    }

    fun getWorkerStatus(): String {
        if (!isLibraryAvailable) return "{}"
        return try {
            nativeGetWorkerStatus()
        } catch (e: Throwable) {
            "{}"
        }
    }

    fun discoverMaster(timeoutMs: Long = 2500): String {
        if (!isLibraryAvailable) return "{}"
        return try {
            nativeDiscoverMaster(timeoutMs)
        } catch (e: Throwable) {
            Log.w(TAG, "Exception in nativeDiscoverMaster", e)
            "{}"
        }
    }

    // Callbacks invoked from native C/Rust thread via JNI
    @JvmStatic
    fun onNativeLog(level: String, message: String) {
        val logLine = "[$level] $message"
        Log.d(TAG, logLine)
        activeListener?.onLogLine(logLine)
    }

    @JvmStatic
    fun onNativeStatusUpdate(activeTasks: Int, totalTasks: Int) {
        activeListener?.onStatsUpdated(activeTasks, totalTasks)
    }

    // Native declarations matching C-ABI exports
    private external fun nativeStartWorker(
        masterAddr: String,
        p2pTicket: String,
        workerName: String,
        cores: Int,
        ramMb: Long,
        simulateGpu: Boolean,
        heartbeatIntervalSecs: Long
    ): Boolean

    private external fun nativeStopWorker(): Boolean

    private external fun nativeIsRunning(): Boolean

    private external fun nativeGetWorkerStatus(): String

    private external fun nativeDiscoverMaster(timeoutMs: Long): String

    private external fun nativeUpdateTelemetry(
        batteryPct: Int,
        isCharging: Boolean,
        thermalThrottled: Boolean
    ): Boolean

    private external fun nativeUpdateTelemetryDetailed(
        batteryPct: Int,
        isCharging: Boolean,
        thermalThrottled: Boolean,
        batteryTemperature: Float,
        networkType: String
    ): Boolean

    private external fun nativeStartMaster(
        bindPort: Int,
        dashboardPort: Int,
        enableP2p: Boolean
    ): String

    private external fun nativeStopMaster(): Boolean
}
