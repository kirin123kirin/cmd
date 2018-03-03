
# ドラッグ＆ドロップの実装

Add-Type -AssemblyName System.Windows.Forms

$form = New-Object System.Windows.Forms.Form
$form.Size = "300,300"
$form.StartPosition = "CenterScreen"
$form.Text = "タイトル"

# リストボックスの生成
$Listbox =  New-Object System.Windows.Forms.ListBox
$Listbox.Location = "10,10"
$Listbox.Size = "260,200"
$Listbox.AllowDrop = $True

# ドラッグエンター　イベント
$Enter = {
　　$_.Effect = "All"
}
$Listbox.Add_DragEnter($Enter)

# ドラッグドロップ　イベント
$Drop = {
    $Name = @($_.Data.GetData("FileDrop"))

    # 1つずつ取得し、リストボックスに追加
    For ( $i = 0 ; $i -lt $Name.Count ; $i++ )
    {
        [void]$Listbox.Items.Add($Name[$i])
    }
}
$Listbox.Add_DragDrop($Drop)

# 閉じるボタン
$Button = New-Object System.Windows.Forms.Button
$Button.Location = "200,220"
$Button.size = "80,30"
$Button.text  =　"閉じる"
$Button.DialogResult = [System.Windows.Forms.DialogResult]::Cancel

$form.Controls.AddRange(@($Listbox,$Button))

$Form.Showdialog()