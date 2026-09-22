Set objShell = CreateObject("Wscript.Shell")
strCurrentDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
objShell.Run Chr(34) & strCurrentDir & "\start_master.cmd" & Chr(34), 0, False
Set objShell = Nothing
