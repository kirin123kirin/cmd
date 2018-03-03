Dim objShell
Set objShell = CreateObject( "WScript.Shell" ) 
Set lstArgs = WScript.Arguments

Dim strFileFullPath
strFileFullPath = "C:\data\sample\Database.accdb"
'strFileFullPath = lstArgs(0) '引数処理

'Access起動
objShell.Run strFileFullPath '& " /Runtime"

'Accessオブジェクトの取得
Dim AcApp
Set AcApp = GetObject(strFileFullPath)

If AcApp is Nothing Then
    Msgbox "File Not Found"
    WScript.Quit
End If
'VBAマクロ実行

AcApp.RunCommand 285

'AcApp.Quit

'Set AcApp = Nothing