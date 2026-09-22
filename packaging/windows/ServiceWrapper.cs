// ==============================================================================
// ServiceWrapper.cs — Zero-Dependency Windows ServiceBase Wrapper for OxideSwarm
//
// Designed to be compiled on any modern Windows system using the built-in
// .NET Framework C# compiler (csc.exe) located at:
//   %WINDIR%\Microsoft.NET\Framework64\v4.0.30319\csc.exe
//
// Features:
//   - Win32 Service Control Manager (SCM) compliant (avoids Error 1053)
//   - Headless Session 0 execution (no interactive desktop/console window)
//   - Asynchronous stdout/stderr log redirection with timestamping & auto-rotation
//   - Child process supervision with SCM failure recovery triggering
//   - Graceful shutdown with child process tree termination
//   - Interactive console/debug mode for testing (--console / --debug)
// ==============================================================================

using System;
using System.Diagnostics;
using System.IO;
using System.ServiceProcess;
using System.Text;
using System.Threading;

namespace OxideSwarm.Packaging.Windows
{
    /// <summary>
    /// Windows Service dispatcher managing the execution of the OxideSwarm worker binary (rusty-grid.exe).
    /// </summary>
    public class OxideSwarmService : ServiceBase
    {
        public const string DefaultServiceName = "OxideSwarmWorker";
        public const string DefaultDisplayName = "OxideSwarm Distributed Grid Worker";
        private const long MaxLogSizeBytes = 10 * 1024 * 1024; // 10 MB per log segment

        private Process _childProcess;
        private volatile bool _isStopping = false;
        private readonly object _logLock = new object();

        private string _resolvedBinaryPath;
        private string _resolvedConfigPath;
        private string _resolvedWorkingDir;
        private string _resolvedLogDir;

        private string _stdoutLogPath;
        private string _stderrLogPath;

        public OxideSwarmService()
        {
            ServiceName = DefaultServiceName;
            CanStop = true;
            CanShutdown = true;
            CanPauseAndContinue = false;
            AutoLog = true;
        }

        #region ServiceBase Lifecycle Callbacks

        protected override void OnStart(string[] args)
        {
            _isStopping = false;

            try
            {
                ResolvePaths();
                EnsureDirectories();
                LogInfo("OxideSwarm Service is starting...");
                LogInfo(string.Format("Binary: {0}", _resolvedBinaryPath));
                LogInfo(string.Format("Config: {0}", _resolvedConfigPath));
                LogInfo(string.Format("Working Dir: {0}", _resolvedWorkingDir));
                LogInfo(string.Format("Log Dir: {0}", _resolvedLogDir));

                if (!File.Exists(_resolvedBinaryPath))
                {
                    string err = string.Format("Worker executable not found at: {0}", _resolvedBinaryPath);
                    LogError(err);
                    WriteEventLog(err, EventLogEntryType.Error);
                    ExitCode = 2; // ERROR_FILE_NOT_FOUND
                    Stop();
                    return;
                }

                StartWorkerProcess();
                LogInfo(string.Format("Worker process successfully spawned with PID {0}.", _childProcess.Id));
            }
            catch (Exception ex)
            {
                string msg = string.Format("Fatal exception during OnStart: {0}", ex);
                LogError(msg);
                WriteEventLog(msg, EventLogEntryType.Error);
                ExitCode = 1;
                Stop();
            }
        }

        protected override void OnStop()
        {
            _isStopping = true;
            LogInfo("OxideSwarm Service is stopping...");
            StopWorkerProcess();
            LogInfo("OxideSwarm Service stopped successfully.");
        }

        protected override void OnShutdown()
        {
            _isStopping = true;
            LogInfo("System shutdown detected. Terminating OxideSwarm worker...");
            StopWorkerProcess();
            base.OnShutdown();
        }

        #endregion

        #region Process Supervision & Management

        private void StartWorkerProcess()
        {
            string arguments = string.Format("worker --config \"{0}\"", _resolvedConfigPath);

            ProcessStartInfo psi = new ProcessStartInfo
            {
                FileName = _resolvedBinaryPath,
                Arguments = arguments,
                WorkingDirectory = _resolvedWorkingDir,
                UseShellExecute = false,
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                StandardOutputEncoding = Encoding.UTF8,
                StandardErrorEncoding = Encoding.UTF8
            };

            // Propagate environment variables
            psi.EnvironmentVariables["RUST_LOG"] = Environment.GetEnvironmentVariable("RUST_LOG") ?? "info";

            _childProcess = new Process
            {
                StartInfo = psi,
                EnableRaisingEvents = true
            };

            _childProcess.OutputDataReceived += OnOutputDataReceived;
            _childProcess.ErrorDataReceived += OnErrorDataReceived;
            _childProcess.Exited += OnProcessExited;

            bool started = _childProcess.Start();
            if (!started)
            {
                throw new InvalidOperationException("Failed to launch worker process.");
            }

            _childProcess.BeginOutputReadLine();
            _childProcess.BeginErrorReadLine();
        }

        private void StopWorkerProcess()
        {
            if (_childProcess == null)
            {
                return;
            }

            try
            {
                if (!_childProcess.HasExited)
                {
                    int pid = _childProcess.Id;
                    LogInfo(string.Format("Attempting graceful termination of worker process PID {0} and its children...", pid));

                    // Terminate process tree via taskkill to ensure sub-tasks/compilations are cleaned up
                    KillProcessTree(pid);

                    if (!_childProcess.WaitForExit(5000))
                    {
                        LogWarning(string.Format("Worker PID {0} did not exit within 5000ms. Forcing immediate termination.", pid));
                        try
                        {
                            _childProcess.Kill();
                        }
                        catch { }
                    }
                }
            }
            catch (Exception ex)
            {
                LogWarning(string.Format("Exception occurred while stopping worker process: {0}", ex.Message));
            }
            finally
            {
                try { _childProcess.Dispose(); } catch { }
                _childProcess = null;
            }
        }

        private void OnProcessExited(object sender, EventArgs e)
        {
            if (_isStopping)
            {
                LogInfo("Worker process exited normally as part of service stop.");
                return;
            }

            int exitCode = -1;
            try
            {
                if (_childProcess != null)
                {
                    exitCode = _childProcess.ExitCode;
                }
            }
            catch { }

            string msg = string.Format("Worker process exited unexpectedly with code {0}! Triggering service failure for SCM restart...", exitCode);
            LogError(msg);
            WriteEventLog(msg, EventLogEntryType.Warning);

            // Signal non-zero exit code so Windows SCM Failure Actions trigger auto-restart
            ExitCode = (exitCode != 0) ? exitCode : 1;
            Stop();
        }

        private static void KillProcessTree(int pid)
        {
            try
            {
                ProcessStartInfo psi = new ProcessStartInfo("taskkill.exe", string.Format("/T /F /PID {0}", pid))
                {
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WindowStyle = ProcessWindowStyle.Hidden
                };

                using (Process killProc = Process.Start(psi))
                {
                    if (killProc != null)
                    {
                        killProc.WaitForExit(4000);
                    }
                }
            }
            catch
            {
                // Fallback to direct process kill handled by caller
            }
        }

        #endregion

        #region Path Discovery & Initialization

        private void ResolvePaths()
        {
            string baseDir = AppDomain.CurrentDomain.BaseDirectory.TrimEnd('\\', '/');

            // 1. Binary Path Discovery
            string customBin = Environment.GetEnvironmentVariable("OXIDESWARM_BIN");
            if (!string.IsNullOrEmpty(customBin) && File.Exists(customBin))
            {
                _resolvedBinaryPath = Path.GetFullPath(customBin);
            }
            else
            {
                string candidate1 = Path.Combine(baseDir, "bin", "rusty-grid.exe");
                string candidate2 = Path.Combine(baseDir, "rusty-grid.exe");
                string candidate3 = Path.Combine(baseDir, "bin", "oxideswarm.exe");
                string candidate4 = Path.Combine(baseDir, "oxideswarm.exe");

                if (File.Exists(candidate1)) _resolvedBinaryPath = candidate1;
                else if (File.Exists(candidate2)) _resolvedBinaryPath = candidate2;
                else if (File.Exists(candidate3)) _resolvedBinaryPath = candidate3;
                else if (File.Exists(candidate4)) _resolvedBinaryPath = candidate4;
                else _resolvedBinaryPath = candidate1; // Default expectation
            }

            // 2. Working Directory
            _resolvedWorkingDir = baseDir;

            // 3. Configuration Path Discovery
            string customConfig = Environment.GetEnvironmentVariable("OXIDESWARM_CONFIG");
            if (!string.IsNullOrEmpty(customConfig) && File.Exists(customConfig))
            {
                _resolvedConfigPath = Path.GetFullPath(customConfig);
            }
            else
            {
                string commonAppData = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
                string programDataConfig = Path.Combine(commonAppData, "OxideSwarm", "rusty-grid.toml");
                string localConfig = Path.Combine(baseDir, "config", "rusty-grid.toml");

                if (File.Exists(programDataConfig)) _resolvedConfigPath = programDataConfig;
                else if (File.Exists(localConfig)) _resolvedConfigPath = localConfig;
                else _resolvedConfigPath = programDataConfig;
            }

            // 4. Log Directory Discovery
            string customLogDir = Environment.GetEnvironmentVariable("OXIDESWARM_LOG_DIR");
            if (!string.IsNullOrEmpty(customLogDir))
            {
                _resolvedLogDir = Path.GetFullPath(customLogDir);
            }
            else
            {
                string commonAppData = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
                string programDataLogDir = Path.Combine(commonAppData, "OxideSwarm", "logs");
                string localLogDir = Path.Combine(baseDir, "logs");

                // Prefer ProgramData for service logging to avoid Program Files ACL issues
                _resolvedLogDir = programDataLogDir;
            }

            _stdoutLogPath = Path.Combine(_resolvedLogDir, "worker.out.log");
            _stderrLogPath = Path.Combine(_resolvedLogDir, "worker.err.log");
        }

        private void EnsureDirectories()
        {
            if (!Directory.Exists(_resolvedLogDir))
            {
                Directory.CreateDirectory(_resolvedLogDir);
            }

            string configDir = Path.GetDirectoryName(_resolvedConfigPath);
            if (!string.IsNullOrEmpty(configDir) && !Directory.Exists(configDir))
            {
                Directory.CreateDirectory(configDir);
            }
        }

        #endregion

        #region Logging Infrastructure

        private void OnOutputDataReceived(object sender, DataReceivedEventArgs e)
        {
            if (e.Data != null)
            {
                AppendLog(_stdoutLogPath, "INFO", e.Data);
            }
        }

        private void OnErrorDataReceived(object sender, DataReceivedEventArgs e)
        {
            if (e.Data != null)
            {
                AppendLog(_stderrLogPath, "ERROR", e.Data);
            }
        }

        private void LogInfo(string message)
        {
            AppendLog(_stdoutLogPath, "SERVICE-INFO", message);
        }

        private void LogWarning(string message)
        {
            AppendLog(_stderrLogPath, "SERVICE-WARN", message);
        }

        private void LogError(string message)
        {
            AppendLog(_stderrLogPath, "SERVICE-ERR", message);
        }

        private void AppendLog(string filePath, string level, string message)
        {
            if (string.IsNullOrEmpty(filePath)) return;

            lock (_logLock)
            {
                try
                {
                    RotateLogIfNeeded(filePath);
                    string line = string.Format("[{0:yyyy-MM-ddTHH:mm:ss.fffZ}] [{1}] {2}{3}",
                        DateTime.UtcNow, level, message, Environment.NewLine);
                    File.AppendAllText(filePath, line, Encoding.UTF8);
                }
                catch
                {
                    // Logging failures must not crash the service dispatcher
                }
            }
        }

        private void RotateLogIfNeeded(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                {
                    FileInfo fi = new FileInfo(filePath);
                    if (fi.Length > MaxLogSizeBytes)
                    {
                        string rolled = filePath + ".1";
                        if (File.Exists(rolled))
                        {
                            File.Delete(rolled);
                        }
                        File.Move(filePath, rolled);
                    }
                }
            }
            catch { }
        }

        private void WriteEventLog(string message, EventLogEntryType entryType)
        {
            try
            {
                const string source = "OxideSwarmWorker";
                if (!EventLog.SourceExists(source))
                {
                    EventLog.CreateEventSource(source, "Application");
                }
                EventLog.WriteEntry(source, message, entryType);
            }
            catch { }
        }

        #endregion

        #region Interactive / Debug Mode Support

        public void RunInteractive()
        {
            Console.WriteLine("================================================================");
            Console.WriteLine("  OxideSwarm Windows Service Wrapper — Interactive Debug Mode");
            Console.WriteLine("================================================================");

            OnStart(new string[0]);

            Console.WriteLine("Worker running. Press Ctrl+C or Enter to stop...");
            Console.ReadLine();

            OnStop();
            Console.WriteLine("Worker stopped. Exiting debug session.");
        }

        #endregion

        #region Entry Point

        public static int Main(string[] args)
        {
            if (args.Length > 0)
            {
                string arg = args[0].ToLowerInvariant().Trim('-', '/');
                if (arg == "console" || arg == "c" || arg == "debug" || arg == "d")
                {
                    OxideSwarmService service = new OxideSwarmService();
                    service.RunInteractive();
                    return 0;
                }
                if (arg == "version" || arg == "v")
                {
                    Console.WriteLine("OxideSwarm Windows Service Wrapper v1.0.0");
                    Console.WriteLine("Compiled for .NET Framework 4.0+ (x86/x64)");
                    return 0;
                }
                if (arg == "help" || arg == "h" || arg == "?")
                {
                    Console.WriteLine("Usage: OxideSwarmService.exe [options]");
                    Console.WriteLine("Options:");
                    Console.WriteLine("  --console, -c, --debug   Run worker interactively for debugging");
                    Console.WriteLine("  --version, -v            Print wrapper version information");
                    Console.WriteLine("  --help, -h               Show this help message");
                    Console.WriteLine("When invoked by Windows Service Control Manager, runs as a background service.");
                    return 0;
                }
            }

            // Standard SCM execution
            ServiceBase.Run(new OxideSwarmService());
            return 0;
        }

        #endregion
    }
}
