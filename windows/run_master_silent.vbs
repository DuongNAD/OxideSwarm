' ==============================================================================
' OxideSwarm Silent MASTER Runner (double-click; no console window)
' The master keeps its dashboard on this PC only. Open a terminal and run
' "windows\start_master.cmd --lan-dashboard" if phones/other PCs need it.
' ==============================================================================
Set objShell = CreateObject("Wscript.Shell")
strCurrentDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)

' Wait for start_master.cmd (it returns once the master is up or has failed) and report failures.
intExit = objShell.Run(Chr(34) & strCurrentDir & "\start_master.cmd" & Chr(34), 0, True)
If intExit <> 0 Then
    MsgBox "The OxideSwarm master did not start. See " & objShell.ExpandEnvironmentStrings("%USERPROFILE%") & "\.oxideswarm\run\master.log and master.err.log", vbExclamation, "OxideSwarm"
End If
Set objShell = Nothing
