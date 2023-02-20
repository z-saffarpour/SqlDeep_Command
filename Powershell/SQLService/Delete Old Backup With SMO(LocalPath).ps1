# sqldeep.com
# version : 2.0.0
# Author: siavash.golchoobian@gmail.com
# Author: mohammadamin.mazidi@yahoo.com
# Author: z.saffarpour@gmail.com
<#
The following script should be executed in Database DBA like SqlDeep
    CREATE TABLE [dbo].[BackupPath]
    (
        [Id] [int] NOT NULL CONSTRAINT [PK_BackupPath] PRIMARY KEY,
        [BackupPath] [nvarchar] (500)  NULL,
        [BackupType] [char] (1)  NULL,
        [RetentionDay] [int] NULL
    )
#>
#--------------------------------------------------------------Parameters.
$ServerName = $env:COMPUTERNAME
$InstanceName = "SqlDeep"
$DBADatabase = 'SqlDeep'
[ValidateSet("F", "D", "L")]$BackupType = 'D'
#--------------------------------------------------------------Load Assembly.
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") | Out-Null
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SmoExtended") | Out-Null
#--------------------------------------------------------------Functions start here.
Set-Location c:
Function Get-SqlInstance 
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][string]$ServerName
    )
    $myInstances = @()
    [array]$captions = Get-WmiObject win32_service -computerName $ServerName | Where-Object{$_.Caption -match "SQL Server*" -and $_.PathName -match "sqlservr.exe"  -and $_.State -match "Running"} | ForEach-Object{$_.Caption}
    foreach ($caption in $captions) 
    {
        If ($caption -eq "MSSQLSERVER") 
        {
            $myInstances += "MSSQLSERVER"
        }
        ELSE 
        {
            $myInstances += $caption | ForEach-Object{$_.split(" ")[-1]} | ForEach-Object{$_.trimStart("(")} | ForEach-Object{$_.trimEnd(")")}
        }
    }
    return $myInstances
}

function Get-BackupPath 
{
    param 
    (
        [Parameter(Mandatory=$true, Position=0)][Microsoft.SqlServer.Management.Smo.Server]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$Database,
        [Parameter(Mandatory=$true, Position=2)][string]$BackupType
    )
    $myDatabase = $SqlInstance.Databases["$Database"]
    $myBackupPathQuery = "SELECT BackupPath, BackupType, RetentionDay FROM [dbo].[BackupPath] WHERE BackupType = '$BackupType' ORDER BY id"
    $myDataSet = $myDatabase.ExecuteWithResults($myBackupPathQuery)    
    $myDataTable = $myDataSet.Tables[0]
    return ,$myDataTable
}

function Delete_Old_Files 
{
    param 
    (
        [Parameter(Mandatory=$true, Position=0)][string]$BackupPath,
        [Parameter(Mandatory=$true, Position=0)][int]$RetentionDay,
        [Parameter(Mandatory=$true, Position=0)][string]$Extension
    )
    $myDate = Get-Date     
    $myStartDate = $myDate.AddDays($myRetentionDay) 
    $myFiles = Get-ChildItem -Recurse $BackupPath -ErrorAction Stop | Where-Object { $_.LastWriteTime -lt $myStartDate -and $_.Extension -eq $Extension }  
    foreach($myFile in $myFiles)
    {
        [System.IO.File]::Delete($myFile.FullName.ToString())
    }
}
#--------------------------------------------------------------Main Body
$errortext = [string]::Empty
$myInstance = Get-SqlInstance -ServerName $ServerName | Where-Object {$_ -eq $InstanceName }
if($null -eq $myInstance)
{
    throw "Not Detect '$InstanceName' Instance"
    exist
}
[string]$mySqlInstanceName = [string]::Empty
if($myInstance -match "MSSQLSERVER")
{
    $mySqlInstanceName = $ServerName
}
else
{
    $mySqlInstanceName = $ServerName + "\" + $myInstance
}
$mySqlInstance = New-Object Microsoft.SqlServer.Management.Smo.Server $mySqlInstanceName
$mySqlInstance.ConnectionContext.StatementTimeout=0
$myBackupPathTable = Get-BackupPath -SqlInstance $mySqlInstance -Database $DBADatabase -BackupType $BackupType
foreach($myBackupPathRow in $myBackupPathTable.Rows )
{
    $myBackupPath = $myBackupPathRow["BackupPath"]
    $myRetentionDay = $myBackupPathRow["RetentionDay"]
    $myBackupType = $myBackupPathRow["BackupType"]
    try
    {
        if ( Test-Path -Path $myBackupPath)
        {
            if ($myBackupType -eq "F")
            {
                Delete_Old_Files -BackupPath $myBackupPath -RetentionDay $myRetentionDay -Extension ".bak"
            }
            elseif ($myBackupType -eq "D")
            {
                Delete_Old_Files -BackupPath $myBackupPath -RetentionDay $myRetentionDay -Extension ".dif"
            }
            elseif ($myBackupType -eq "L")
            {
                Delete_Old_Files -BackupPath $myBackupPath -RetentionDay $myRetentionDay -Extension ".trn"
            }
        }
    }
    catch 
    {
        $errortext+= $_ | Select-Object *  | Out-String      
    }
}
if (!([string]::IsNullOrEmpty($errortext)))
{
    throw $errortext
}