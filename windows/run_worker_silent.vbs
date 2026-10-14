' ==============================================================================
' OxideSwarm Silent Background Runner (double-click, no console window)
' Starts the worker through start_worker.cmd (master "auto" = LAN discovery) and
' tells you if it could not start. The master's cluster.secret must be present in
' %USERPROFILE%\.oxideswarm\ first (see windows\README.md).
' ==============================================================================
Set objShell = CreateObject("Wscript.Shell")
strCurrentDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)

' intWindowStyle = 0 (hidden), bWaitOnReturn = True so a failed start can be reported.
intExit = objShell.Run(Chr(34) & strCurrentDir & "\start_worker.cmd" & Chr(34), 0, True)
If intExit <> 0 Then
    MsgBox "The OxideSwarm worker did not start. Most likely the master's cluster.secret is missing from " & objShell.ExpandEnvironmentStrings("%USERPROFILE%") & "\.oxideswarm\. See run\worker.log and worker.err.log in that folder.", vbExclamation, "OxideSwarm"
End If
Set objShell = Nothing
