'--------------------------------------------------------------------------------------
'
'	Excel シート毎に保存
'			(指定したExcelファイルを１シート毎に１ブックに分割します)
'	
'	注意点	スクリプト実行前にすべてのExcelアプリケーションを終了してください。
'			スクリプト実行中はマウス、キーボードを使用しないでください。
'			このスクリプトが異常終了した場合はExcelのプロセスを手動で終了してください。
'
'--------------------------------------------------------------------------------------
'★分割対象Excel
Const SAVE_ELS = "C:\Documents and Settings\admin\デスクトップ\hoge.xls"

'★保存先フォルダ
Const SAVE_DIR = "C:\Documents and Settings\admin\デスクトップ\OUTFOLDER\"

	'Excelを開く
	Set Excel  = CreateObject("Excel.Application")
    Set wkBook = Excel.WorkBooks.Open(SAVE_ELS)
	
	'シートの数ループ
	For i = 1 To wkBook.Sheets.Count

		'シートオブジェクトを取得
		Set wkSheet = wkBook.Sheets(i)

		'新しくブックを作成し、コピー
		Set Addbook = Excel.WorkBooks.Add
		wkSheet.Copy ,Addbook.Sheets(Addbook.Sheets.Count)
		
		'不要なシートを削除
	    Excel.DisplayAlerts = False
	    Addbook.Sheets(1).Delete
	    Excel.DisplayAlerts = True

		'名前を付けて保存
		Addbook.SaveAs SAVE_DIR & wkSheet.Name & ".xls"
		
		Set wkSheet = Nothing
		Addbook.Close
		Set Addbook = Nothing
	Next

	wkBook.Close False
	Set wkBook = Nothing
	Set Excel = Nothing

	msgbox "出力を完了しました。"
