package com.oxideswarm.worker.util

import android.app.ActivityManager
import android.content.Context
import android.os.Build
import android.os.Process
import java.io.File

object SystemInfoHelper {

    fun getAvailableCores(): Int {
        return Runtime.getRuntime().availableProcessors().coerceAtLeast(1)
    }

    fun getTotalRamMb(context: Context): Long {
        val actManager = context.getSystemService(Context.ACTIVITY_SERVICE) as? ActivityManager
        val memInfo = ActivityManager.MemoryInfo()
        actManager?.getMemoryInfo(memInfo)
        return (memInfo.totalMem / (1024 * 1024)).coerceAtLeast(1024L)
    }

    fun getAvailableRamMb(context: Context): Long {
        val actManager = context.getSystemService(Context.ACTIVITY_SERVICE) as? ActivityManager
        val memInfo = ActivityManager.MemoryInfo()
        actManager?.getMemoryInfo(memInfo)
        return (memInfo.availMem / (1024 * 1024)).coerceAtLeast(512L)
    }

    fun getDefaultWorkerName(): String {
        val model = Build.MODEL.replace(" ", "_").replace("[^a-zA-Z0-9_-]".toRegex(), "")
        return "android-$model"
    }

    fun getCurrentPid(): Int {
        return Process.myPid()
    }

    fun readCurrentOomScoreAdj(): String {
        return try {
            val file = File("/proc/self/oom_score_adj")
            if (file.exists() && file.canRead()) {
                file.readText().trim()
            } else {
                "N/A"
            }
        } catch (e: Exception) {
            "err"
        }
    }
}
