package com.oxideswarm.worker.service

import android.app.Service
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.ServiceInfo
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.net.wifi.WifiManager
import android.os.BatteryManager
import android.os.Binder
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import android.util.Log
import com.oxideswarm.worker.runner.*
import com.oxideswarm.worker.util.SystemInfoHelper
import java.util.concurrent.atomic.AtomicBoolean

class OxideWorkerService : Service(), WorkerEngineListener {

    private val binder = LocalBinder()
    private lateinit var notificationManager: WorkerNotificationManager
    private var cpuWakeLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null
    private var multicastLock: WifiManager.MulticastLock? = null

    private var activeEngine: WorkerEngine? = null
    private val isRunning = AtomicBoolean(false)

    // Current state caching
    private var currentConfig: WorkerConfig? = null
    private var currentState: WorkerState = WorkerState.STOPPED
    private var currentMessage: String = "Service initialized"
    private var activeTasks: Int = 0
    private var totalExecutedTasks: Int = 0

    // Mobile telemetry cache and listeners
    private var batteryReceiver: BroadcastReceiver? = null
    private var thermalListener: PowerManager.OnThermalStatusChangedListener? = null
    private var currentBatteryPct: Int = -1
    private var currentBatteryTemp: Float = -1.0f
    private var currentIsCharging: Boolean = false
    private var currentThermalThrottled: Boolean = false
    private var currentNetworkType: String = "unknown"

    // Callback listeners registered by UI (MainActivity)
    private val uiListeners = mutableListOf<WorkerServiceListener>()

    interface WorkerServiceListener {
        fun onStatusChanged(state: WorkerState, message: String, activeTasks: Int, totalTasks: Int)
        fun onLogReceived(line: String)
    }

    inner class LocalBinder : Binder() {
        fun getService(): OxideWorkerService = this@OxideWorkerService
    }

    override fun onBind(intent: Intent?): IBinder = binder

    override fun onCreate() {
        super.onCreate()
        Log.i(TAG, "OxideWorkerService onCreate - Elevating to Foreground Service")
        notificationManager = WorkerNotificationManager(this)

        // Acquire hardware locks to keep CPU and high-performance Wi-Fi alive
        acquireWakeLocks()

        // Setup real-time battery and thermal monitoring for Rust JNI injection
        setupTelemetryMonitoring()

        // Start Foreground immediately with initial notification to guarantee oom_score_adj <= 200
        startInForeground("Worker Initializing...", "Holding CPU wake-lock and high-performance Wi-Fi")
    }

    private fun getActiveNetworkType(): String {
        return try {
            val cm = getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
            val network = cm?.activeNetwork
            val caps = cm?.getNetworkCapabilities(network) ?: return "unknown"
            when {
                caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) -> "wifi"
                caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) -> "cellular"
                caps.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET) -> "ethernet"
                else -> "unknown"
            }
        } catch (e: Exception) {
            "unknown"
        }
    }

    private fun setupTelemetryMonitoring() {
        // 1. Battery State Monitoring
        batteryReceiver = object : BroadcastReceiver() {
            override fun onReceive(context: Context?, intent: Intent?) {
                if (intent == null) return
                val level = intent.getIntExtra(BatteryManager.EXTRA_LEVEL, -1)
                val scale = intent.getIntExtra(BatteryManager.EXTRA_SCALE, -1)
                val pct = if (level >= 0 && scale > 0) (level * 100) / scale else -1
                val status = intent.getIntExtra(BatteryManager.EXTRA_STATUS, -1)
                val isCharging = status == BatteryManager.BATTERY_STATUS_CHARGING || status == BatteryManager.BATTERY_STATUS_FULL
                val tempInt = intent.getIntExtra(BatteryManager.EXTRA_TEMPERATURE, -1)
                val batteryTemp = if (tempInt > 0) tempInt / 10.0f else -1.0f
                val netType = getActiveNetworkType()

                currentBatteryPct = pct
                currentIsCharging = isCharging
                currentBatteryTemp = batteryTemp
                currentNetworkType = netType
                OxideWorkerBridge.updateTelemetry(pct, isCharging, currentThermalThrottled, batteryTemp, netType)
            }
        }
        val filter = IntentFilter(Intent.ACTION_BATTERY_CHANGED)
        val stickyIntent = registerReceiver(batteryReceiver, filter)
        if (stickyIntent != null) {
            val level = stickyIntent.getIntExtra(BatteryManager.EXTRA_LEVEL, -1)
            val scale = stickyIntent.getIntExtra(BatteryManager.EXTRA_SCALE, -1)
            val pct = if (level >= 0 && scale > 0) (level * 100) / scale else -1
            val status = stickyIntent.getIntExtra(BatteryManager.EXTRA_STATUS, -1)
            val isCharging = status == BatteryManager.BATTERY_STATUS_CHARGING || status == BatteryManager.BATTERY_STATUS_FULL
            val tempInt = stickyIntent.getIntExtra(BatteryManager.EXTRA_TEMPERATURE, -1)
            val batteryTemp = if (tempInt > 0) tempInt / 10.0f else -1.0f
            val netType = getActiveNetworkType()

            currentBatteryPct = pct
            currentIsCharging = isCharging
            currentBatteryTemp = batteryTemp
            currentNetworkType = netType
            OxideWorkerBridge.updateTelemetry(pct, isCharging, currentThermalThrottled, batteryTemp, netType)
        }

        // 2. Thermal Throttling Monitoring (API 29+)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            try {
                val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
                thermalListener = PowerManager.OnThermalStatusChangedListener { status ->
                    val isThrottled = status >= PowerManager.THERMAL_STATUS_MODERATE
                    currentThermalThrottled = isThrottled
                    OxideWorkerBridge.updateTelemetry(
                        currentBatteryPct,
                        currentIsCharging,
                        isThrottled,
                        currentBatteryTemp,
                        currentNetworkType
                    )
                }
                pm.addThermalStatusListener(thermalListener!!)
            } catch (e: Exception) {
                Log.w(TAG, "Failed to register thermal status listener", e)
            }
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val action = intent?.action ?: ACTION_START

        when (action) {
            ACTION_STOP -> {
                Log.i(TAG, "Received ACTION_STOP - terminating worker")
                stopWorkerService()
                stopSelf()
                return START_NOT_STICKY
            }
            ACTION_START -> {
                val master = intent?.getStringExtra(EXTRA_MASTER) ?: "127.0.0.1:8080"
                val p2pTicket = intent?.getStringExtra(EXTRA_P2P_TICKET)
                val name = intent?.getStringExtra(EXTRA_NAME) ?: SystemInfoHelper.getDefaultWorkerName()
                val cores = intent?.getIntExtra(EXTRA_CORES, SystemInfoHelper.getAvailableCores())
                    ?: SystemInfoHelper.getAvailableCores()
                val ramMb = intent?.getLongExtra(EXTRA_RAM_MB, SystemInfoHelper.getTotalRamMb(this))
                    ?: SystemInfoHelper.getTotalRamMb(this)
                val simulateGpu = intent?.getBooleanExtra(EXTRA_SIMULATE_GPU, false) ?: false

                val config = WorkerConfig(
                    masterAddress = master,
                    p2pTicket = p2pTicket,
                    workerName = name,
                    cores = cores,
                    ramMb = ramMb,
                    simulateGpu = simulateGpu
                )
                startWorkerEngine(config)
            }
        }

        // START_STICKY tells OS to resurrect the service if it ever terminates under RAM pressure
        return START_STICKY
    }

    private fun acquireWakeLocks() {
        try {
            val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
            cpuWakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "OxideSwarm::CpuWakeLock").apply {
                setReferenceCounted(false)
                acquire(24 * 60 * 60 * 1000L) // Safety timeout 24h
            }
            Log.i(TAG, "Acquired PARTIAL_WAKE_LOCK (OxideSwarm::CpuWakeLock)")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to acquire CPU wake lock", e)
        }

        try {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
            var lock: WifiManager.WifiLock? = null
            if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.Q) {
                try {
                    lock = wm.createWifiLock(WifiManager.WIFI_MODE_FULL_LOW_LATENCY, "OxideSwarm::WifiLock").apply {
                        setReferenceCounted(false)
                        acquire()
                    }
                    Log.i(TAG, "Acquired WifiLock (WIFI_MODE_FULL_LOW_LATENCY)")
                } catch (e: Exception) {
                    Log.w(TAG, "Failed to acquire WIFI_MODE_FULL_LOW_LATENCY, falling back to WIFI_MODE_FULL_HIGH_PERF", e)
                }
            }
            if (lock == null) {
                @Suppress("DEPRECATION")
                lock = wm.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "OxideSwarm::WifiLock").apply {
                    setReferenceCounted(false)
                    acquire()
                }
                Log.i(TAG, "Acquired WifiLock (WIFI_MODE_FULL_HIGH_PERF)")
            }
            wifiLock = lock
        } catch (e: Exception) {
            Log.e(TAG, "Failed to acquire Wi-Fi lock", e)
        }

        try {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
            multicastLock = wm.createMulticastLock("OxideSwarm::ServiceMulticastLock").apply {
                setReferenceCounted(false)
                acquire()
            }
            Log.i(TAG, "Acquired MulticastLock for background UDP discovery")
        } catch (e: Exception) {
            Log.w(TAG, "Failed to acquire MulticastLock", e)
        }
    }

    private fun releaseWakeLocks() {
        try {
            cpuWakeLock?.let {
                if (it.isHeld) {
                    it.release()
                    Log.i(TAG, "Released CPU wake lock")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error releasing CPU wake lock", e)
        }

        try {
            wifiLock?.let {
                if (it.isHeld) {
                    it.release()
                    Log.i(TAG, "Released Wi-Fi lock")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error releasing Wi-Fi lock", e)
        }

        try {
            multicastLock?.let {
                if (it.isHeld) {
                    it.release()
                    Log.i(TAG, "Released MulticastLock")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error releasing MulticastLock", e)
        }
    }

    private fun startInForeground(title: String, content: String) {
        val notification = notificationManager.buildNotification(title, content)

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) { // Android 14+
            startForeground(
                WorkerNotificationManager.NOTIFICATION_ID,
                notification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE
            )
        } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) { // Android 10+
            startForeground(
                WorkerNotificationManager.NOTIFICATION_ID,
                notification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
            )
        } else {
            startForeground(WorkerNotificationManager.NOTIFICATION_ID, notification)
        }
    }

    private fun startWorkerEngine(config: WorkerConfig) {
        if (isRunning.get()) {
            Log.i(TAG, "Worker engine is already active")
            return
        }

        currentConfig = config
        isRunning.set(true)

        // Choose optimal engine: In-Process JNI if available (immune to PPK), or ManagedProcessRunner
        val engine: WorkerEngine = if (OxideWorkerBridge.isAvailable()) {
            Log.i(TAG, "Using In-Process JNI Engine (liboxideworker.so)")
            OxideWorkerBridge
        } else {
            Log.i(TAG, "Using ManagedProcessRunner (Binary Executor)")
            ManagedProcessRunner(this)
        }
        activeEngine = engine

        val statusText = "Connecting to Master: ${config.masterAddress}"
        val details = "Node: ${config.workerName} | Cores: ${config.cores} | Engine: ${engine.getEngineName()}"
        notificationManager.updateNotification(statusText, details)

        engine.start(config, this)
    }

    fun stopWorkerService() {
        if (!isRunning.get()) return
        isRunning.set(false)

        activeEngine?.stop()
        activeEngine = null

        releaseWakeLocks()
        currentState = WorkerState.STOPPED
        currentMessage = "Worker stopped"
        notifyStateChanged(currentState, currentMessage)
        stopForeground(STOP_FOREGROUND_REMOVE)
    }

    // Engine Listener Callbacks
    override fun onStateChanged(state: WorkerState, message: String) {
        currentState = state
        currentMessage = message

        val master = currentConfig?.masterAddress ?: "Unknown"
        val statusText = when (state) {
            WorkerState.RUNNING -> "Active | Connected: $master"
            WorkerState.STARTING -> "Starting | $message"
            WorkerState.STOPPED -> "Stopped"
            WorkerState.ERROR -> "Error | $message"
        }

        val details = "Active Tasks: $activeTasks | Completed: $totalExecutedTasks | PID: ${SystemInfoHelper.getCurrentPid()}"
        notificationManager.updateNotification(statusText, details)

        notifyStateChanged(state, message)
    }

    override fun onLogLine(line: String) {
        synchronized(uiListeners) {
            for (listener in uiListeners) {
                listener.onLogReceived(line)
            }
        }
    }

    override fun onStatsUpdated(activeTasks: Int, totalExecuted: Int) {
        this.activeTasks = activeTasks
        this.totalExecutedTasks = totalExecuted

        val master = currentConfig?.masterAddress ?: ""
        val statusText = "Active | Tasks: $activeTasks | Master: $master"
        val details = "Total Executed: $totalExecuted | Cores: ${currentConfig?.cores ?: 0}"
        notificationManager.updateNotification(statusText, details)

        synchronized(uiListeners) {
            for (listener in uiListeners) {
                listener.onStatusChanged(currentState, currentMessage, activeTasks, totalExecutedTasks)
            }
        }
    }

    private fun notifyStateChanged(state: WorkerState, message: String) {
        synchronized(uiListeners) {
            for (listener in uiListeners) {
                listener.onStatusChanged(state, message, activeTasks, totalExecutedTasks)
            }
        }
    }

    fun registerUiListener(listener: WorkerServiceListener) {
        synchronized(uiListeners) {
            if (!uiListeners.contains(listener)) {
                uiListeners.add(listener)
            }
            // Emit current state immediately
            listener.onStatusChanged(currentState, currentMessage, activeTasks, totalExecutedTasks)
        }
    }

    fun unregisterUiListener(listener: WorkerServiceListener) {
        synchronized(uiListeners) {
            uiListeners.remove(listener)
        }
    }

    fun isWorkerRunning(): Boolean = isRunning.get()
    fun getCurrentConfig(): WorkerConfig? = currentConfig

    override fun onDestroy() {
        Log.i(TAG, "OxideWorkerService onDestroy")
        stopWorkerService()

        batteryReceiver?.let {
            try {
                unregisterReceiver(it)
            } catch (e: Exception) {
                Log.w(TAG, "Error unregistering battery receiver", e)
            }
            batteryReceiver = null
        }

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && thermalListener != null) {
            try {
                val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
                pm.removeThermalStatusListener(thermalListener!!)
            } catch (e: Exception) {
                Log.w(TAG, "Error removing thermal listener", e)
            }
            thermalListener = null
        }

        super.onDestroy()
    }

    companion object {
        private const val TAG = "OxideWorkerService"

        const val ACTION_START = "com.oxideswarm.worker.action.START"
        const val ACTION_STOP = "com.oxideswarm.worker.action.STOP"

        const val EXTRA_MASTER = "EXTRA_MASTER"
        const val EXTRA_P2P_TICKET = "EXTRA_P2P_TICKET"
        const val EXTRA_NAME = "EXTRA_NAME"
        const val EXTRA_CORES = "EXTRA_CORES"
        const val EXTRA_RAM_MB = "EXTRA_RAM_MB"
        const val EXTRA_SIMULATE_GPU = "EXTRA_SIMULATE_GPU"

        fun startService(context: Context, config: WorkerConfig) {
            val intent = Intent(context, OxideWorkerService::class.java).apply {
                action = ACTION_START
                putExtra(EXTRA_MASTER, config.masterAddress)
                putExtra(EXTRA_P2P_TICKET, config.p2pTicket)
                putExtra(EXTRA_NAME, config.workerName)
                putExtra(EXTRA_CORES, config.cores)
                putExtra(EXTRA_RAM_MB, config.ramMb)
                putExtra(EXTRA_SIMULATE_GPU, config.simulateGpu)
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
        }

        fun stopService(context: Context) {
            val intent = Intent(context, OxideWorkerService::class.java).apply {
                action = ACTION_STOP
            }
            context.startService(intent)
        }
    }
}
