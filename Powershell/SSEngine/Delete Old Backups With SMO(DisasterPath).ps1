# sqldeep.com
# version : 3.0.0
# Author: siavash.golchoobian@gmail.com
# Author: mohammadamin.mazidi@yahoo.com
# Author: z.saffarpour@gmail.com
<#
The following script should be executed in Database DBA like SqlDeep
    CREATE TABLE [dbo].[DisasterPath]
    (
        [DisasterPathId] [int] NOT NULL CONSTRAINT [PK_dbo_DisasterPath] PRIMARY KEY CLUSTERED,
        [DestinationPathType] [varchar] (10) NOT NULL,
        [DestinationPath] [nvarchar] (500) NOT NULL,
        [RetentionDayFullBackup] [int] NOT NULL,
        [RetentionDayDiffBackup] [int] NOT NULL,
        [RetentionDayLogBackup] [int] NOT NULL,
        [CheckExtendedProperty] [bit] NOT NULL,
        [ExtendedPropertyKey] [nvarchar](10) NOT NULL,
        [ExtendedPropertyValue] [NVARCHAR](10) NOT NULL
    ) 
    #>
#--------------------------------------------------------------Parameters.
$ServerName = $env:COMPUTERNAME
$InstanceName = "SqlDeep"
$DBADatabase = 'SqlDeep'
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
Function Get-DisasterPath
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][string]$SqlServerInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$Database
    )
    $myServer = New-Object Microsoft.SqlServer.Management.Smo.Server($SqlServerInstance)
    $myDatabases = $myServer.Databases["$Database"]
    $myQuery = "SELECT [DestinationPath], [RetentionDayFullBackup], [RetentionDayDiffBackup], [RetentionDayLogBackup] FROM dbo.DisasterPath WHERE DestinationPathType='hdd'"
    $myDataSet = $myDatabases.ExecuteWithResults($myQuery)   
    $myDataTable = $myDataSet.Tables[0]
    return ,$myDataTable
}
function Delete_Old_Files_From_Disaster
{
    param 
    (
        [Parameter(Mandatory=$true, Position=0)][string]$DestinationPath,
        [Parameter(Mandatory=$true, Position=1)][int]$RetentionDay,
        [Parameter(Mandatory=$true, Position=2)][string]$Extension
    )
    $date = Get-Date     
    $myStartDate = $date.AddDays($myRetentionDay) 
    $myFiles = Get-ChildItem -Path "Microsoft.PowerShell.Core\FileSystem::$DestinationPath" -Recurse -ErrorAction Stop | Where-Object { $_.LastWriteTime -lt $myStartDate -and $_.Extension -eq $Extension } 
    foreach($myFile in $myFiles)
    {
        Remove-Item -Path "Microsoft.PowerShell.Core\FileSystem::$myFile"
    }
}
#--------------------------------------------------------------Main Body.
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
$myDisasterPathTable = Get-DisasterPath -SqlServerInstance $mySqlInstance -Database $DBADatabase
foreach($DisasterPathRow in $myDisasterPathTable)
{
    $myDestinationPath = $DisasterPathRow["DestinationPath"]
    $myRetentionDayFullBackup = $DisasterPathRow["RetentionDayFullBackup"]
    $myRetentionDayDiffBackup = $DisasterPathRow["RetentionDayDiffBackup"]
    $myRetentionDayLogBackup = $DisasterPathRow["RetentionDayLogBackup"]
    $myDestination = $myDestinationPath + "\" + $myInstance.ToString().Replace('\','_')
    try
    {
        if (Test-Path -Path $myDestinationPath)
        {
            Delete_Old_Files_From_Disaster -DestinationPath $myDestination -RetentionDay $myRetentionDayFullBackup -Extension ".bak"
            Delete_Old_Files_From_Disaster -DestinationPath $myDestination -RetentionDay $myRetentionDayDiffBackup -Extension ".dif"
            Delete_Old_Files_From_Disaster -DestinationPath $myDestination -RetentionDay $myRetentionDayLogBackup -Extension ".trn"
        } 
    }
    catch 
    {
        $errortext += $_ | Select-Object *  | Out-String      
    }
}
if (!([string]::IsNullOrEmpty($errortext)))
{
    throw $errortext
}