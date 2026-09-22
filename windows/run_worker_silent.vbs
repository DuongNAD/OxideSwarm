' ==============================================================================
' OxideSwarm Silent Background Runner (Double-Click & Forget)
' Runs the background worker with 0 visible windows (100% hidden)
' ==============================================================================

Set objShell = CreateObject("Wscript.Shell")
strCurrentDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)

' Execute start_worker.cmd completely hidden (intWindowStyle = 0, bWaitOnReturn = False)
objShell.Run Chr(34) & strCurrentDir & "\start_worker.cmd" & Chr(34), 0, False
Set objShell = Nothing
