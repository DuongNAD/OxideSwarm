package com.oxideswarm.worker.runner

enum class WorkerState {
    STOPPED,
    STARTING,
    RUNNING,
    ERROR
}

data class WorkerConfig(
    val masterAddress: String,
    val p2pTicket: String? = null,
    val workerName: String,
    val cores: Int,
    val ramMb: Long,
    val simulateGpu: Boolean,
    val heartbeatIntervalSecs: Long = 3L
)

interface WorkerEngineListener {
    fun onStateChanged(state: WorkerState, message: String)
    fun onLogLine(line: String)
    fun onStatsUpdated(activeTasks: Int, totalExecuted: Int)
}

interface WorkerEngine {
    fun start(config: WorkerConfig, listener: WorkerEngineListener): Boolean
    fun stop()
    fun isRunning(): Boolean
    fun getEngineName(): String
}
