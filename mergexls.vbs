'--------------------------------------------------------------------------------------
'
'	Excel １ブックにまとめる
'			(指定したフォルダにあるExcelを1ブックに分割します)
'	
'	注意点	スクリプト実行前にすべてのExcelアプリケーションを終了してください。
'		スクリプト実行中はマウス、キーボードを使用しないでください。
'		このスクリプトが異常終了した場合はExcelのプロセスを手動で終了してください。
'
'--------------------------------------------------------------------------------------
'★マージ対象フォルダ
Const SAVE_ELS = "C:\Users\harada\Desktop\エビデンス\エビデンス"

'★保存先フォルダ
Const SAVE_DIR = "C:\output\"

'★保存先ファイル名
Const SAVE_FILE ="test" 



	'このディレクトリのファイル一覧を取得する
	Set fileSystem   = CreateObject("Scripting.FileSystemObject")
	Set targetFolder = fileSystem.getFolder(SAVE_ELS)
	Set fileList = targetFolder.Files

	'Excelオブジェクトを開く
	Set Excel  = CreateObject("Excel.Application")
	Excel.DisplayAlerts = False

	'新しくブックを作成し、コピー
	Set Addbook = Excel.WorkBooks.Add

	'ファイルの終端までループ
	For Each wkFile In fileList


        	Set wkBook = Excel.WorkBooks.Open(SAVE_ELS & "\" & wkFile.Name)

		'シートの数ループ
		For i = 1 To wkBook.Sheets.Count

			'シートオブジェクトを取得
			Set wkSheet = wkBook.Sheets(i)

			wkSheet.Copy ,Addbook.Sheets(Addbook.Sheets.Count)
	
		
			Set wkSheet = Nothing

		Next

		wkBook.Close	
		Set wkBook = Nothing
	Next


	'名前を付けて保存
	Addbook.SaveAs SAVE_DIR & SAVE_FILE  & ".xlsx"

	Addbook.Close
	Set Addbook = Nothing

	Set wkBook = Nothing
	Set Excel = Nothing

	msgbox "出力を完了しました。"