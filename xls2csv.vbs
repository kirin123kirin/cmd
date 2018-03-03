Set lstArgs = WScript.Arguments

For I = 0 to lstArgs.Count - 1 ' Loop through each file

    FullName = lstArgs(I)

    FileName = Left(lstArgs(I), InStrRev(lstArgs(I), ".") )


' Create Excel Objects

    Set objWS = CreateObject("Excel.application")

    set objWB = objWS.Workbooks.Open(FullName)


    objWS.application.visible=false

    objWS.application.displayalerts=false

'MsgBox FileName

    objWB.SaveAs FileName & "csv", 23

    objWB.SaveAs

    objWS.Application.Quit

    objWS.Quit 

' Destroy Excel Objects

    Set objWS = Nothing

    set objWB = Nothing


Next