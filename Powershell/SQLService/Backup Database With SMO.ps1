# sqldeep.com
# version : 4.0.0
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
[ValidateSet("F", "D", "L")]$BackupType = 'F'
[ValidateSet("bak", "dif", "trn")]$BackupExtension = 'bak'
[ValidateSet("Persian", "Miladi")]$CalendarType ='Persian'
$extendedpropertykey="_SchFullBackup"
$extendedpropertyvalue="D"
$checkextendedProperty = $false
#--------------------------------------------------------------Load Assembly here.
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") | Out-Null
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SmoExtended") | Out-Null
#--------------------------------------------------------------Functions start here.
#Return list of SQL Server instances
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

Function HasPrimaryReplica
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][Microsoft.SqlServer.Management.Smo.Server]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$Database
    )
    
    $myDatabase = $SqlInstance.Databases["msdb"]
    $myQuery = "DECLARE @dbname sysname;
              SET @dbname = '$Database';
              SELECT ISNULL(sys.fn_hadr_is_primary_replica(@dbname),0) AS IsPrimary;"
    $myDataSet = $myDatabase.ExecuteWithResults($myQuery)    
    $myDataTable = $myDataSet.Tables[0]
    $myDataTable.Rows[0].IsPrimary
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
    $myBackupPathQuery = "SELECT [BackupPath] FROM [dbo].[BackupPath] WHERE BackupType = '$BackupType' ORDER BY id"
    $myDataSet = $myDatabase.ExecuteWithResults($myBackupPathQuery)    
    $myDataTable = $myDataSet.Tables[0]
    return ,$myDataTable
}

function Get-Databases 
{
    param 
    (
        [Parameter(Mandatory=$true, Position=0)][Microsoft.SqlServer.Management.Smo.Server]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$BackupType,
        [Parameter(Mandatory=$true, Position=2)][string]$CheckExtendedProperty,
        [Parameter(Mandatory=$true, Position=3)][string]$ExtendedPropertyKey,
        [Parameter(Mandatory=$true, Position=4)][string]$ExtendedPropertyValue
    )
    $myDatabaseList = New-Object Microsoft.SqlServer.Management.Smo.Database 
    $myDatabaseList = $SqlInstance.Databases 

    $myDatabaseTable = New-Object system.Data.DataTable "BackupDatabases"

    $myDatabaseNameColumn = New-Object system.Data.DataColumn DatabaseName,([string])
    $myDatabaseTable.columns.add($myDatabaseNameColumn)

    $myHasReadOnlyColumn = New-Object system.Data.DataColumn HasReadOnly,([bool])
    $myDatabaseTable.columns.add($myHasReadOnlyColumn)

    foreach ($myDatabase in $myDatabaseList) 
    { 
        $myDatabaseName = $myDatabase.Name
        if($SqlInstance.Version.Major -ge 11)
        {
            if(!([string]::IsNullOrEmpty($myDatabase.AvailabilityGroupName)))
            {
                $IsPrimary = HasPrimaryReplica -SqlInstance $SqlInstance -database $myDatabaseName
                if (!$IsPrimary)
                {
                    continue
                } 
            }
        }
        if ($myDatabaseName -eq "tempdb" -or $myDatabase.Status -ne "Normal" )
        {
            continue
        }
        if ($BackupType -eq "D" -and ($myDatabase.ReadOnly -eq $true -or "master","model","msdb" -contains $myDatabaseName))
        {
            continue
        }
        if ($BackupType -eq "L" -and $myDatabase.RecoveryModel.ToString() -ne "Full")
        {
            continue
        }
        if ($CheckExtendedProperty -eq $true)
        {
            #not exist extended property
            if (!$myDatabase.ExtendedProperties.Contains($ExtendedPropertyKey))
            {
                continue
            }
            #exist Extended property but not equal key
            if ($myDatabase.ExtendedProperties.Item($ExtendedPropertyKey).value -ne $ExtendedPropertyValue)
            {
                continue
            }
        }

        $myDatabaseRow = $myDatabaseTable.NewRow()
        $myDatabaseRow.DatabaseName = $myDatabaseName 
        $myDatabaseRow.HasReadOnly = $myDatabase.ReadOnly
        $myDatabaseTable.Rows.Add($myDatabaseRow)
    } 

    return ,$myDatabaseTable
}

function TakeBackupAndVerify
{
    param 
    (
        [Parameter(Mandatory=$true, Position=0)][Microsoft.SqlServer.Management.Smo.Server]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$BackupType,
        [Parameter(Mandatory=$true, Position=2)][string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=3)][bool]$DatabaseHasReadOnly,
        [Parameter(Mandatory=$true, Position=4)][string]$BackupTypeDescription,
        [Parameter(Mandatory=$true, Position=5)][string]$BackupExtension,
        [Parameter(Mandatory=$false, Position=6)][System.Collections.ArrayList]$BackupPathArray,
        [Parameter(Mandatory=$true, Position=7)][string]$Year,
        [Parameter(Mandatory=$true, Position=8)][string]$Month,
        [Parameter(Mandatory=$true, Position=9)][string]$Day,
        [Parameter(Mandatory=$true, Position=10)][string]$Hour,
        [Parameter(Mandatory=$true, Position=11)][string]$Minute,
        [Parameter(Mandatory=$true, Position=12)][string]$Second      
    )
    $myMediaSetName = $BackupTypeDescription +'_' + $DatabaseName + "_" + $Year + "_" + $Month + "_" + $Day + "_on_" +$Hour + "_" + $Minute + "_" + $Second
    $myBackupSetName = $myMediaSetName
    $myMediaSetDescription = $myMediaSetName
    $myBackupSetDescription = $myBackupSetName
    
    $myBackup = New-Object Microsoft.SqlServer.Management.Smo.Backup
    $myRestore = New-Object Microsoft.SqlServer.Management.Smo.Restore
    SWITCH($BackupType)
    {
        'F' 
        {
            $myBackup.Action = [Microsoft.SqlServer.Management.Smo.BackupActionType]::Database
        }
        'D' 
        {
            $myBackup.Action = [Microsoft.SqlServer.Management.Smo.BackupActionType]::Database
            $myBackup.Incremental = 1
        }
        'L' 
        {
            $myBackup.Action = [Microsoft.SqlServer.Management.Smo.BackupActionType]::Log
        }
    }
    
    $myBackup.Database = $myDatabaseName 
    $myBackup.BackupSetName = $myBackupSetName
    $myBackup.BackupSetDescription = $myBackupSetDescription
    $myBackup.MediaName = $myMediaSetName
    $myBackup.MediaDescription = $myMediaSetDescription
    $myBackup.CompressionOption = 1 
    $myBackup.Checksum = 1
    if ($DatabaseHasReadOnly -eq $true)
    {
        $myBackup.CopyOnly = $true
    }
    $myIterator = 1
    $myBackupFileCount = $BackupPathArray.Count
    foreach($myBackupPathItem in $BackupPathArray)
    {
        $myBackupPath = $myBackupPathItem + "\" + $BackupTypeDescription + '_' + $DatabaseName + "_" + $Year + "_" + $Month + "_" + $Day + "_on_" + $Hour + "_" + $Minute + "_" + $Second + '_' + $myIterator + 'of' + $myBackupFileCount + '.' + $BackupExtension
        $myBackup.Devices.AddDevice($myBackupPath, "File") 
        $myRestore.Devices.AddDevice($myBackupPath, "File") 
        if ($myBackupFileCount -ge 1)
        {
            $myIterator = $myIterator + 1
        }
    } 
    try
    {
        $myBackup.SqlBackup($SqlInstance) 
    }
    catch
    {
        throw $_
    }
    try
    {
        if (!( $myRestore.SqlVerify($SqlInstance)))
        {
            throw "backup Database'$DatabaseName' is not verfied"
        }
    }
    catch
    {
        throw $_
    }
}
#--------------------------------------------------------------Main Body
[string]$errortext = [string]::Empty
[string]$myYear = [string]::Empty
[string]$myMonth = [string]::Empty
[string]$myDay = [string]::Empty
[string]$myHour = [string]::Empty
[string]$myMinute = [string]::Empty
[string]$mySecond = [string]::Empty

$myDate = [System.DateTime]::Now 
if($CalendarType -eq "Persian")
{
	$myPersianCalendar= new-object system.Globalization.PersianCalendar 
	$myYear = $myPersianCalendar.GetYear($myDate).ToString("0000")
	$myMonth = $myPersianCalendar.GetMonth($myDate).ToString("00")
	$myDay = $myPersianCalendar.GetDayOfMonth($myDate).ToString("00")
	$myHour = $myPersianCalendar.GetHour($myDate).ToString("00")
	$myMinute = $myPersianCalendar.GetMinute($myDate).ToString("00")
	$mySecond = $myPersianCalendar.GetSecond($myDate).ToString("00")
}
else #if($CalendarType -eq "Miladi")
{
    $myYear = $myDate.Year.ToString("0000")
    $myMonth = $myDate.Month.ToString("00")
    $myDay = $myDate.Day.ToString("00")
    $myHour = $myDate.Hour.ToString("00")
    $myMinute = $myDate.Minute.ToString("00")
    $mySecond = $myDate.Second.ToString("00")
}

$myYearMonth = $myYear + "_" + $myMonth
$myBackupTypeDescription = [string]::Empty

SWITCH($BackupType)
{
    'F' 
    {
        $myBackupTypeDescription = "FULL"
    }
    'D' 
    {
        $myBackupTypeDescription = "DIFF"
    }
    'L' 
    {
        $myBackupTypeDescription = "LOG"
    }
}
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
if ( $myBackupPathTable.Rows.Count -eq 0)
{
    throw "Not Detect '$myBackupTypeDescription' Backup Path"
    exist
}
$myBackupPathArray = New-Object -TypeName System.Collections.ArrayList
foreach($myBackupPathRow in $myBackupPathTable)
{
    $myBackupPath =  $myBackupPathRow.BackupPath + "\" + $myYearMonth  + "\" + $myDay 
    if (!(Test-Path -Path "Microsoft.PowerShell.Core\FileSystem::$myBackupPath") )
    {          
        New-Item -Path "Microsoft.PowerShell.Core\FileSystem::$myBackupPath"  -ItemType Directory | Out-Null  
    }
    $myBackupPathArray.Add($myBackupPath)
}
$myDatabaseTable = Get-Databases -SqlInstance $mySqlInstance -BackupType $BackupType -CheckExtendedProperty $checkextendedProperty -ExtendedPropertyKey $extendedpropertykey -ExtendedPropertyValue $extendedpropertyvalue
foreach ($myDatabaseRow in $myDatabaseTable) 
{ 
    $myDatabaseName = $myDatabaseRow["DatabaseName"]
    $myDatabaseHasReadOnly = $myDatabaseRow["HasReadOnly"]
    try
    {
        TakeBackupAndVerify -SqlInstance $mySqlInstance -BackupType $BackupType -DatabaseName $myDatabaseName -DatabaseHasReadOnly $myDatabaseHasReadOnly -BackupTypeDescription $myBackupTypeDescription -BackupExtension $BackupExtension -BackupPathArray $myBackupPathArray -Year $myYear -Month $myMonth -Day $myDay -Hour $myHour -Minute $myMinute -Second $mySecond
    }   
    catch [Exception]
    {
        #$errortext += $_.Exception.Message  
        $errortext += $_ | Select-Object * | Out-String 
        continue
    }  
}
if ($errortext.Length -gt 0)
{
    #Write-Output $errortext
    throw $errortext
}