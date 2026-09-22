package com.oxideswarm.worker.receiver

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.util.Log
import com.oxideswarm.worker.runner.WorkerConfig
import com.oxideswarm.worker.service.OxideWorkerService
import com.oxideswarm.worker.util.SystemInfoHelper

class BootCompletedReceiver : BroadcastReceiver() {

    override fun onReceive(context: Context, intent: Intent?) {
        val action = intent?.action
        Log.i(TAG, "BootCompletedReceiver received action: $action")

        if (action == Intent.ACTION_BOOT_COMPLETED ||
            action == Intent.ACTION_LOCKED_BOOT_COMPLETED ||
            action == Intent.ACTION_MY_PACKAGE_REPLACED ||
            action == "android.intent.action.QUICKBOOT_POWERON" ||
            action == "com.htc.intent.action.QUICKBOOT_POWERON") {

            // When in Direct Boot (before user unlock), use Device Protected Storage
            val targetContext = if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.N) {
                context.createDeviceProtectedStorageContext()
            } else {
                context
            }
            val prefs = targetContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            val autoStartEnabled = prefs.getBoolean(KEY_AUTO_START, true)

            if (!autoStartEnabled) {
                Log.i(TAG, "Auto-start on boot is disabled in preferences; skipping service start")
                return
            }

            val master = prefs.getString(KEY_MASTER, "127.0.0.1:8080") ?: "127.0.0.1:8080"
            val name = prefs.getString(KEY_WORKER_NAME, SystemInfoHelper.getDefaultWorkerName())
                ?: SystemInfoHelper.getDefaultWorkerName()
            val cores = prefs.getInt(KEY_CORES, SystemInfoHelper.getAvailableCores())
            val ramMb = prefs.getLong(KEY_RAM_MB, SystemInfoHelper.getTotalRamMb(context))
            val simulateGpu = prefs.getBoolean(KEY_SIMULATE_GPU, false)

            val config = WorkerConfig(
                masterAddress = master,
                workerName = name,
                cores = cores,
                ramMb = ramMb,
                simulateGpu = simulateGpu
            )

            Log.i(TAG, "Auto-starting OxideWorkerService on boot with master: $master")
            try {
                OxideWorkerService.startService(context, config)
            } catch (e: Exception) {
                Log.e(TAG, "Failed to start OxideWorkerService on boot", e)
            }
        }
    }

    companion object {
        private const val TAG = "BootCompletedReceiver"
        const val PREFS_NAME = "oxide_worker_prefs"
        const val KEY_AUTO_START = "key_auto_start_boot"
        const val KEY_MASTER = "key_master_address"
        const val KEY_WORKER_NAME = "key_worker_name"
        const val KEY_CORES = "key_cpu_cores"
        const val KEY_RAM_MB = "key_ram_mb"
        const val KEY_SIMULATE_GPU = "key_simulate_gpu"
    }
}
