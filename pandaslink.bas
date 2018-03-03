Option Explicit
Private Declare Function OpenProcess Lib "kernel32" _
    (ByVal dwDesiredAccess As Long, ByVal bInheritHandle As Long, _
     ByVal dwProcessId As Long) As Long
Private Declare Function GetExitCodeProcess Lib "kernel32" _
    (ByVal hProcess As Long, lpExitCode As Long) As Long
Private Declare Function CloseHandle Lib "kernel32" _
    (ByVal hObject As Long) As Long
Private Const PROCESS_QUERY_INFORMATION = &H400&
Private Const STILL_ACTIVE = &H103&

Function runpython(args As String)
    Dim Ret_Val
    Ret_Val = Shell("python.exe " & args)
    ShellEnd (Ret_Val)
    If Ret_Val = 0 Then
       MsgBox "Couldn't run python script!", vbOKOnly
    End If
End Function

Private Sub ShellEnd(ProcessID As Long)
    Dim hProcess As Long
    Dim EndCode As Long
    Dim EndRet   As Long
    'ハンドルを取得する
     hProcess = OpenProcess(PROCESS_QUERY_INFORMATION, 1, ProcessID)
    '終わるまで待つ
    Do
        EndRet = GetExitCodeProcess(hProcess, EndCode)
        DoEvents
    Loop While (EndCode = STILL_ACTIVE)
    'ハンドルを閉じる
     EndRet = CloseHandle(hProcess)
End Sub

Sub Build()
    Dim nrow As Long
    Dim ncol As Long

    nrow = Range("A" & Rows.Count).End(xlUp).Row
    ncol = Range("XFD2").End(xlToLeft).Column
    
    'Worksheets("links").Range(Cells(2, 2), Cells(nrow, ncol)).Copy
    Range(Cells(2, 1), Cells(nrow, ncol)).Copy
    
    'Call runpython(ThisWorkbook.Path & "\blockdiag.py")
    Call runpython(ThisWorkbook.Path & "\" & ActiveSheet.Name & ".py")
    Application.CutCopyMode = False
End Sub

Sub Preview()
    Dim dirpath As String
    Dim OwnFile As String
    dirpath = ThisWorkbook.Path & "\..\.cache\"
    OwnFile = GetFNameFromFStr(ThisWorkbook.Name) & ".html"
    Shell ("CMD /C START " & Chr(34) & Chr(34) & " " & Chr(34) & dirpath & OwnFile & Chr(34))
End Sub

Function GetFNameFromFStr(sFileName As String) As String
    Dim sFileStr As String
    Dim lFindPoint As Long
    Dim lStrLen As Long
    lFindPoint = InStrRev(sFileName, ".")
    sFileStr = Left(sFileName, lFindPoint - 1)
    GetFNameFromFStr = sFileStr
End Function

