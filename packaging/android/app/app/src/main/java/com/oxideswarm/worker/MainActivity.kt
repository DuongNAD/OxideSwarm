package com.oxideswarm.worker

import android.Manifest
import android.content.*
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import android.widget.*
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import com.google.android.material.button.MaterialButton
import com.google.android.material.switchmaterial.SwitchMaterial
import com.google.android.material.textfield.TextInputEditText
import com.oxideswarm.worker.receiver.BootCompletedReceiver
import com.oxideswarm.worker.runner.OxideWorkerBridge
import com.oxideswarm.worker.runner.WorkerConfig
import com.oxideswarm.worker.runner.WorkerState
import com.oxideswarm.worker.service.OxideWorkerService
import com.oxideswarm.worker.util.BatteryOptimizationHelper
import com.oxideswarm.worker.util.SystemInfoHelper

class MainActivity : AppCompatActivity(), OxideWorkerService.WorkerServiceListener {

    private var workerService: OxideWorkerService? = null
    private var isBound = false

    // UI elements
    private lateinit var tvStatusTitle: TextView
    private lateinit var tvStatusDetails: TextView
    private lateinit var tvPidOom: TextView
    private lateinit var viewStatusDot: android.view.View
    private lateinit var btnStart: MaterialButton
    private lateinit var btnStop: MaterialButton
    private lateinit var tvBatteryExemptStatus: TextView
    private lateinit var btnRequestBatteryExemption: MaterialButton
    private lateinit var btnOemGuide: MaterialButton
    private lateinit var btnOpenDashboard: MaterialButton
    private lateinit var btnScanMaster: MaterialButton
    private lateinit var etMasterAddress: TextInputEditText
    private lateinit var etWorkerName: TextInputEditText
    private lateinit var etCores: TextInputEditText
    private lateinit var etRamMb: TextInputEditText
    private lateinit var switchSimulateGpu: SwitchMaterial
    private lateinit var switchAutoStart: SwitchMaterial
    private lateinit var tvLogs: TextView
    private lateinit var scrollLogs: ScrollView
    private lateinit var btnClearLogs: TextView
    private lateinit var btnCopyLogs: TextView
    private lateinit var tvEngineBadge: TextView

    private val logBuffer = StringBuilder()
    private var lastDiscoveredWebUi: String? = null

    private val serviceConnection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
            val binder = service as? OxideWorkerService.LocalBinder
            workerService = binder?.getService()
            isBound = true
            workerService?.registerUiListener(this@MainActivity)
            updateOomDisplay()
        }

        override fun onServiceDisconnected(name: ComponentName?) {
            workerService?.unregisterUiListener(this@MainActivity)
            workerService = null
            isBound = false
            updateUiState(WorkerState.STOPPED, "Service disconnected")
        }
    }

    private val requestNotificationPermission =
        registerForActivityResult(ActivityResultContracts.RequestPermission()) { isGranted ->
            if (!isGranted) {
                appendLog("[WARN] Notification permission denied. Foreground status might not be visible.")
            }
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        // Keep phone screen awake while OxideSwarm app is open
        window.addFlags(android.view.WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        setContentView(R.layout.activity_main)

        initViews()
        loadPreferences()
        checkPermissions()
        refreshBatteryOptimizationStatus()

        // If master is loopback or empty, attempt auto-scan in background
        val currentMaster = etMasterAddress.text?.toString().orEmpty()
        if (currentMaster.contains("127.0.0.1") || currentMaster.isEmpty() || currentMaster.equals("auto", ignoreCase = true)) {
            scanForMaster(showToast = false)
        }

        // Bind to existing running service if already started
        val serviceIntent = Intent(this, OxideWorkerService::class.java)
        bindService(serviceIntent, serviceConnection, 0)
    }

    private fun initViews() {
        tvStatusTitle = findViewById(R.id.tvStatusTitle)
        tvStatusDetails = findViewById(R.id.tvStatusDetails)
        tvPidOom = findViewById(R.id.tvPidOom)
        viewStatusDot = findViewById(R.id.viewStatusDot)
        btnStart = findViewById(R.id.btnStartWorker)
        btnStop = findViewById(R.id.btnStopWorker)
        tvBatteryExemptStatus = findViewById(R.id.tvBatteryExemptStatus)
        btnRequestBatteryExemption = findViewById(R.id.btnRequestBatteryExemption)
        btnOemGuide = findViewById(R.id.btnOemGuide)
        btnOpenDashboard = findViewById(R.id.btnOpenDashboard)
        btnScanMaster = findViewById(R.id.btnScanMaster)
        etMasterAddress = findViewById(R.id.etMasterAddress)
        etWorkerName = findViewById(R.id.etWorkerName)
        etCores = findViewById(R.id.etCores)
        etRamMb = findViewById(R.id.etRamMb)
        switchSimulateGpu = findViewById(R.id.switchSimulateGpu)
        switchAutoStart = findViewById(R.id.switchAutoStart)
        tvLogs = findViewById(R.id.tvLogs)
        scrollLogs = findViewById(R.id.scrollLogs)
        btnClearLogs = findViewById(R.id.btnClearLogs)
        btnCopyLogs = findViewById(R.id.btnCopyLogs)
        tvEngineBadge = findViewById(R.id.tvEngineBadge)

        tvEngineBadge.text = if (OxideWorkerBridge.isAvailable()) "In-Process JNI" else "Managed Process"

        btnStart.setOnClickListener { handleStartWorker() }
        btnStop.setOnClickListener { handleStopWorker() }
        btnScanMaster.setOnClickListener { scanForMaster(showToast = true) }
        
        btnOpenDashboard.setOnClickListener {
            val targetUrl = lastDiscoveredWebUi ?: run {
                val master = etMasterAddress.text?.toString()?.trim().orEmpty()
                val host = if (master.contains(":")) master.split(":")[0] else master
                val ip = if (host.isEmpty() || host.equals("auto", ignoreCase = true)) "192.168.1.144" else host
                "http://$ip:8080"
            }
            val intent = Intent(Intent.ACTION_VIEW, android.net.Uri.parse(targetUrl))
            try {
                startActivity(intent)
            } catch (e: Exception) {
                Toast.makeText(this, "Cannot open browser", Toast.LENGTH_SHORT).show()
            }
        }

        btnRequestBatteryExemption.setOnClickListener {
            val intent = BatteryOptimizationHelper.createRequestIgnoreBatteryOptimizationsIntent(this)
            try {
                startActivity(intent)
            } catch (e: Exception) {
                Toast.makeText(this, "Unable to launch battery settings", Toast.LENGTH_SHORT).show()
            }
        }

        btnOemGuide.setOnClickListener {
            val opened = BatteryOptimizationHelper.openOemBackgroundSettings(this)
            if (!opened) {
                Toast.makeText(this, "Could not identify OEM settings; please check battery settings.", Toast.LENGTH_LONG).show()
            }
        }

        btnClearLogs.setOnClickListener {
            logBuffer.clear()
            tvLogs.text = ""
        }

        btnCopyLogs.setOnClickListener {
            val clipboard = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
            val clip = ClipData.newPlainText("OxideSwarm Logs", logBuffer.toString())
            clipboard.setPrimaryClip(clip)
            Toast.makeText(this, "Logs copied to clipboard", Toast.LENGTH_SHORT).show()
        }
    }

    private fun loadPreferences() {
        val storageContext = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) createDeviceProtectedStorageContext() else this
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            storageContext.moveSharedPreferencesFrom(this, BootCompletedReceiver.PREFS_NAME)
        }
        val prefs = storageContext.getSharedPreferences(BootCompletedReceiver.PREFS_NAME, Context.MODE_PRIVATE)

        val defaultMaster = prefs.getString(BootCompletedReceiver.KEY_MASTER, "127.0.0.1:8080")
        val defaultName = prefs.getString(BootCompletedReceiver.KEY_WORKER_NAME, SystemInfoHelper.getDefaultWorkerName())
        val defaultCores = prefs.getInt(BootCompletedReceiver.KEY_CORES, SystemInfoHelper.getAvailableCores())
        val defaultRam = prefs.getLong(BootCompletedReceiver.KEY_RAM_MB, SystemInfoHelper.getTotalRamMb(this))
        val defaultSimulateGpu = prefs.getBoolean(BootCompletedReceiver.KEY_SIMULATE_GPU, false)
        val defaultAutoStart = prefs.getBoolean(BootCompletedReceiver.KEY_AUTO_START, true)

        etMasterAddress.setText(defaultMaster)
        etWorkerName.setText(defaultName)
        etCores.setText(defaultCores.toString())
        etRamMb.setText(defaultRam.toString())
        switchSimulateGpu.isChecked = defaultSimulateGpu
        switchAutoStart.isChecked = defaultAutoStart
    }

    private fun savePreferences(config: WorkerConfig, autoStart: Boolean) {
        val storageContext = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) createDeviceProtectedStorageContext() else this
        val prefs = storageContext.getSharedPreferences(BootCompletedReceiver.PREFS_NAME, Context.MODE_PRIVATE)
        prefs.edit()
            .putString(BootCompletedReceiver.KEY_MASTER, config.masterAddress)
            .putString(BootCompletedReceiver.KEY_WORKER_NAME, config.workerName)
            .putInt(BootCompletedReceiver.KEY_CORES, config.cores)
            .putLong(BootCompletedReceiver.KEY_RAM_MB, config.ramMb)
            .putBoolean(BootCompletedReceiver.KEY_SIMULATE_GPU, config.simulateGpu)
            .putBoolean(BootCompletedReceiver.KEY_AUTO_START, autoStart)
            .apply()
    }

    private fun checkPermissions() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS)
                != PackageManager.PERMISSION_GRANTED) {
                requestNotificationPermission.launch(Manifest.permission.POST_NOTIFICATIONS)
            }
        }
    }

    private fun refreshBatteryOptimizationStatus() {
        val isExempt = BatteryOptimizationHelper.isIgnoringBatteryOptimizations(this)
        if (isExempt) {
            tvBatteryExemptStatus.text = getString(R.string.battery_exempt_yes)
            tvBatteryExemptStatus.setTextColor(ContextCompat.getColor(this, R.color.status_green))
            btnRequestBatteryExemption.isEnabled = false
            btnRequestBatteryExemption.text = "Exemption Granted"
        } else {
            tvBatteryExemptStatus.text = getString(R.string.battery_exempt_no)
            tvBatteryExemptStatus.setTextColor(ContextCompat.getColor(this, R.color.status_amber))
            btnRequestBatteryExemption.isEnabled = true
            btnRequestBatteryExemption.text = getString(R.string.btn_request_battery_exemption)
        }
    }

    private fun handleStartWorker() {
        val master = etMasterAddress.text?.toString()?.trim().orEmpty()
        val name = etWorkerName.text?.toString()?.trim().orEmpty()
        val cores = etCores.text?.toString()?.toIntOrNull() ?: SystemInfoHelper.getAvailableCores()
        val ramMb = etRamMb.text?.toString()?.toLongOrNull() ?: SystemInfoHelper.getTotalRamMb(this)
        val simulateGpu = switchSimulateGpu.isChecked
        val autoStart = switchAutoStart.isChecked

        if (master.isEmpty()) {
            etMasterAddress.error = "Master address required (e.g. 192.168.1.100:8080)"
            return
        }

        val config = WorkerConfig(
            masterAddress = master,
            workerName = if (name.isEmpty()) SystemInfoHelper.getDefaultWorkerName() else name,
            cores = cores,
            ramMb = ramMb,
            simulateGpu = simulateGpu
        )
        savePreferences(config, autoStart)

        appendLog("[UI] Requesting Worker Service Start -> $master...")
        OxideWorkerService.startService(this, config)

        val serviceIntent = Intent(this, OxideWorkerService::class.java)
        bindService(serviceIntent, serviceConnection, Context.BIND_AUTO_CREATE)
    }

    private fun handleStopWorker() {
        appendLog("[UI] Stopping Worker Service...")
        workerService?.stopWorkerService()
        if (isBound) {
            workerService?.unregisterUiListener(this)
            unbindService(serviceConnection)
            isBound = false
        }
        OxideWorkerService.stopService(this)
        updateUiState(WorkerState.STOPPED, "Worker stopped by user")
    }

    private fun scanForMaster(showToast: Boolean = false) {
        if (showToast) {
            Toast.makeText(this, "Đang quét tìm Master trong mạng LAN...", Toast.LENGTH_SHORT).show()
        }
        appendLog("[DISCOVERY] Bắt đầu quét tìm Master trong mạng LAN...")

        Thread {
            var discoveredMasterCluster: String? = null
            var discoveredHostName: String? = null
            var discoveredWebUi: String? = null

            // Method 1: JNI UDP Beacon Discovery (with Android MulticastLock)
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            val multicastLock = wm?.createMulticastLock("OxideSwarm::MainScanMulticastLock")?.apply {
                setReferenceCounted(false)
            }
            try {
                multicastLock?.acquire()
                if (OxideWorkerBridge.isAvailable()) {
                    try {
                        val jsonStr = OxideWorkerBridge.discoverMaster(1800)
                        val json = org.json.JSONObject(jsonStr)
                        if (json.optBoolean("found", false)) {
                            discoveredMasterCluster = json.optString("cluster_addr")
                            discoveredHostName = json.optString("hostname")
                            discoveredWebUi = json.optString("web_ui_url")
                        }
                    } catch (e: Exception) {
                        android.util.Log.w("MainActivity", "JNI discovery exception", e)
                    }
                }
            } finally {
                try {
                    if (multicastLock?.isHeld == true) multicastLock.release()
                } catch (_: Exception) {}
            }

            // Method 2: Fast Parallel HTTP Fallback to Cluster Candidate Nodes
            if (discoveredMasterCluster == null) {
                val candidateIps = listOf("192.168.1.144", "192.168.1.123")
                val executor = java.util.concurrent.Executors.newFixedThreadPool(candidateIps.size)
                val completionService = java.util.concurrent.ExecutorCompletionService<org.json.JSONObject?>(executor)

                for (ip in candidateIps) {
                    completionService.submit {
                        try {
                            val url = java.net.URL("http://$ip:8080/api/status")
                            val conn = url.openConnection() as java.net.HttpURLConnection
                            conn.connectTimeout = 700
                            conn.readTimeout = 700
                            conn.requestMethod = "GET"
                            if (conn.responseCode == 200) {
                                val resp = conn.inputStream.bufferedReader().use { it.readText() }
                                val json = org.json.JSONObject(resp)
                                json.put("_probed_ip", ip)
                                json
                            } else {
                                null
                            }
                        } catch (_: Exception) {
                            null
                        }
                    }
                }

                var remaining = candidateIps.size
                while (remaining > 0 && discoveredMasterCluster == null) {
                    try {
                        val future = completionService.poll(800, java.util.concurrent.TimeUnit.MILLISECONDS)
                        remaining--
                        val json = future?.get()
                        if (json != null) {
                            val probedIp = json.optString("_probed_ip", "192.168.1.144")
                            if (json.has("master") && json.getJSONObject("master").optString("role") == "MASTER") {
                                discoveredMasterCluster = "$probedIp:8088"
                                discoveredHostName = json.getJSONObject("master").optString("host", "OxideMaster")
                                discoveredWebUi = "http://$probedIp:8080"
                                break
                            } else if (json.optString("role") == "WORKER_PORTAL") {
                                val redirectUrl = json.optString("redirect_url")
                                val masterCluster = json.optString("master_cluster_addr")
                                if (masterCluster.isNotEmpty() && !masterCluster.equals("auto", ignoreCase = true)) {
                                    discoveredMasterCluster = masterCluster
                                    discoveredWebUi = json.optString("master_web_ui", redirectUrl)
                                    discoveredHostName = "PortalRedirect"
                                    break
                                } else if (redirectUrl.isNotEmpty() && !redirectUrl.contains("127.0.0.1")) {
                                    val clean = redirectUrl.removePrefix("http://").removePrefix("https://")
                                    val redirectHost = clean.split(":")[0].split("/")[0]
                                    discoveredMasterCluster = "$redirectHost:8088"
                                    discoveredWebUi = redirectUrl
                                    discoveredHostName = "PortalRedirect"
                                    break
                                }
                            }
                        }
                    } catch (_: Exception) {}
                }
                executor.shutdownNow()
            }

            runOnUiThread {
                if (discoveredMasterCluster != null) {
                    lastDiscoveredWebUi = discoveredWebUi
                    etMasterAddress.setText(discoveredMasterCluster)
                    val hostDisplay = discoveredHostName ?: "OxideMaster"
                    appendLog("[DISCOVERY] ✅ Đã tìm thấy Master: $hostDisplay ($discoveredMasterCluster)")
                    tvStatusDetails.text = "Master: $hostDisplay ($discoveredMasterCluster)"
                    btnOpenDashboard.text = "🌐 Mở Dashboard ($hostDisplay)"
                    Toast.makeText(this, "Đã tìm thấy Master: $hostDisplay ($discoveredMasterCluster)", Toast.LENGTH_LONG).show()
                } else {
                    appendLog("[DISCOVERY] ⚠️ Không nhận được phản hồi từ Master nào trong LAN.")
                    if (showToast) {
                        Toast.makeText(this, "Không tìm thấy Master nào đang chạy trong LAN", Toast.LENGTH_SHORT).show()
                    }
                }
            }
        }.start()
    }

    private fun updateOomDisplay() {
        val pid = SystemInfoHelper.getCurrentPid()
        val oom = SystemInfoHelper.readCurrentOomScoreAdj()
        tvPidOom.text = "PID: $pid | oom_adj: $oom"
    }

    private fun updateUiState(state: WorkerState, message: String) {
        runOnUiThread {
            when (state) {
                WorkerState.RUNNING -> {
                    tvStatusTitle.text = getString(R.string.status_running)
                    tvStatusTitle.setTextColor(ContextCompat.getColor(this, R.color.status_green))
                    viewStatusDot.backgroundTintList = ContextCompat.getColorStateList(this, R.color.status_green)
                    btnStart.isEnabled = false
                    btnStop.isEnabled = true
                }
                WorkerState.STARTING -> {
                    tvStatusTitle.text = getString(R.string.status_connecting)
                    tvStatusTitle.setTextColor(ContextCompat.getColor(this, R.color.status_amber))
                    viewStatusDot.backgroundTintList = ContextCompat.getColorStateList(this, R.color.status_amber)
                    btnStart.isEnabled = false
                    btnStop.isEnabled = true
                }
                WorkerState.STOPPED -> {
                    tvStatusTitle.text = getString(R.string.status_stopped)
                    tvStatusTitle.setTextColor(ContextCompat.getColor(this, R.color.status_red))
                    viewStatusDot.backgroundTintList = ContextCompat.getColorStateList(this, R.color.status_red)
                    btnStart.isEnabled = true
                    btnStop.isEnabled = false
                }
                WorkerState.ERROR -> {
                    tvStatusTitle.text = "ERROR"
                    tvStatusTitle.setTextColor(ContextCompat.getColor(this, R.color.status_red))
                    viewStatusDot.backgroundTintList = ContextCompat.getColorStateList(this, R.color.status_red)
                    btnStart.isEnabled = true
                    btnStop.isEnabled = false
                }
            }
            tvStatusDetails.text = message
            updateOomDisplay()
        }
    }

    override fun onStatusChanged(state: WorkerState, message: String, activeTasks: Int, totalTasks: Int) {
        val details = if (state == WorkerState.RUNNING) {
            "Connected | Tasks Active: $activeTasks | Total Executed: $totalTasks"
        } else {
            message
        }
        updateUiState(state, details)
    }

    override fun onLogReceived(line: String) {
        runOnUiThread {
            appendLog(line)
        }
    }

    private fun appendLog(line: String) {
        if (logBuffer.length > 50000) {
            logBuffer.delete(0, 10000)
        }
        logBuffer.append(line).append("\n")
        tvLogs.text = logBuffer.toString()
        scrollLogs.post { scrollLogs.fullScroll(ScrollView.FOCUS_DOWN) }
    }

    override fun onResume() {
        super.onResume()
        refreshBatteryOptimizationStatus()
        updateOomDisplay()
    }

    override fun onDestroy() {
        if (isBound) {
            workerService?.unregisterUiListener(this)
            unbindService(serviceConnection)
            isBound = false
        }
        super.onDestroy()
    }
}
