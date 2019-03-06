@echo off

powershell "Get-ChildItem %* -Recurse | ? { $_.PSIsContainer } | Select-Object mode,length,lastwritetime,fullname| ConvertTo-Csv -NoTypeInformation -Delimiter `t"
