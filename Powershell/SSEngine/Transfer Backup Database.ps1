# sqldeep.com
# Author: siavash.golchoobian@gmail.com
# Author: mohammadamin.mazidi@yahoo.com
# Author: z.saffarpour@gmail.com
#--------------------------------------------------------------Parameters.
[string]$ServerName = $env:COMPUTERNAME 
[string]$Instance = "SqlDeep"
[string]$DBADatabase = 'SqlDeep'
[string]$BackupType = "'D','I'"  #"'D','I','L'"
[string]$TransferedSuffix = "_Transfered"
[int]$ScanLastXHours = 72
[string]$DestinationPath = "\\192.168.1.1\BackupRepo\DB"
[bool]$PrintInfo = $false
#--------------------------------------------------------------Functions start here.
Function Get-Connection {
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][string]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$DatabaseName
    )
    $sqlConnection = New-Object System.Data.SqlClient.SqlConnection
    # Connectionstring setting for local machine database with window authentication
    $sqlConnection.ConnectionString = "server=$SqlInstance;database=$DatabaseName;trusted_connection=True"
    return $sqlConnection
}

#Execute commands than output is table
Function ExecuteReader {
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][string]$SqlServerInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=2)][string]$Query
    )
    $mySqlCommand = New-Object System.Data.SqlClient.SqlCommand
    $dataTable = New-Object System.Data.DataTable
    $mySqlCommand.CommandText = $Query
    $mySqlCommand.Connection = Get-Connection -SqlServerInstance $SqlServerInstance -DatabaseName $DatabaseName
    $mySqlCommand.Connection.Open()
    $myDataReader = $mySqlCommand.ExecuteReader()
    $dataTable.Load($myDataReader)
    $mySqlCommand.Connection.Close()
    return ,$dataTable
}

#Execute commands that have no output
Function ExecuteNonQuery {
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][string]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=2)][string]$Query
    )
    $mySqlCommand = New-Object System.Data.SqlClient.SqlCommand
    $mySqlCommand.CommandText = $Query
    $mySqlCommand.Connection = Get-Connection -SqlInstance $SqlInstance -DatabaseName $DatabaseName
    $mySqlCommand.Connection.Open()
    $mySqlCommand.ExecuteNonQuery()
    $mySqlCommand.Connection.Close()
}

#Return list of backup files that have not been transferred
Function Get-BackupHistory {
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][string]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=2)][int]$DisasterPathId,
        [Parameter(Mandatory=$true, Position=3)][string]$BackupType,
        [Parameter(Mandatory=$true, Position=4)][String]$TransferedSuffix,
        [Parameter(Mandatory=$true, Position=5)][int]$ScanLastXHours
    )
    $myBackupHistoryQuery = "DECLARE @TransferedSuffix nvarchar(20) = '$TransferedSuffix' ;
                           DECLARE @ScanLastXHours INT = $ScanLastXHours; 
                           SELECT myUniqueBackupSet.backup_set_id AS BackupSetId
	                             ,myMediaSet.media_set_id AS MediaSetId
	                             ,myMediaSet.physical_device_name AS PhysicalFile
	                             ,UPPER(myUniqueBackupSet.machine_name) AS MachineName
	                             ,UPPER(myUniqueBackupSet.server_name) AS InstanceName
	                             ,myUniqueBackupSet.[database_name] AS DatabaseName
	                             ,BackupType=CASE UPPER(myUniqueBackupSet.[type])
	  				                           WHEN 'D' THEN 'Database'
	  				                           WHEN 'I' THEN 'Differential'
	  				                           WHEN 'L' THEN 'TransactionLog'
	  				                           ELSE 'Database'
	  			                           END
	                             ,RIGHT(myMediaSet.physical_device_name,CHARINDEX('\',REVERSE(myMediaSet.physical_device_name))-1) AS BackupFileName
	                             ,myUniqueBackupSet.backup_start_date as BackupStartDate
                                 ,myUniqueBackupSet.backup_finish_date as BackupFinishDate
                                 ,myUniqueBackupSet.first_lsn AS BackupFirstLSN
                                 ,myUniqueBackupSet.last_lsn AS BackupLastLSN
	                             ,CAST([myMediaSet].[family_sequence_number] AS INT) AS [family_sequence_number]
	                             ,MAX(CAST([myMediaSet].[family_sequence_number] AS INT)) OVER (PARTITION BY myMediaSet.[media_set_id]) AS [max_family_sequence_number]
                           FROM msdb.dbo.backupmediafamily as myMediaSet
                           INNER JOIN (
			                           SELECT
				                           myBackupSet.media_set_id,
				                           MAX(myBackupSet.backup_set_id) AS backup_set_id,
				                           MAX(myBackupSet.machine_name) AS machine_name,
				                           MAX(myBackupSet.server_name) AS server_name,
				                           MAX(myBackupSet.[database_name]) AS [database_name],
				                           MAX(myBackupSet.backup_start_date) AS backup_start_date,
                                           MAX(myBackupSet.backup_finish_date) AS backup_finish_date,
				                           MAX(myBackupSet.[type]) AS [type],
                                           MAX(myBackupSet.[first_lsn]) AS [first_lsn],
                                           MAX(myBackupSet.[last_lsn]) AS [last_lsn]
			                           FROM msdb.dbo.backupset as myBackupSet
			                           WHERE myBackupSet.backup_finish_date IS NOT NULL
				                           AND myBackupSet.is_copy_only = 0
				                           AND myBackupSet.backup_start_date >= DATEADD(HOUR,-1*@ScanLastXHours,getdate())
				                           AND myBackupSet.server_name = @@SERVERNAME
				                           AND (myBackupSet.[description] IS NULL OR myBackupSet.[description] NOT LIKE '%' + @TransferedSuffix + '%')
				                           AND myBackupSet.[type] IN ($BackupType)
			                           GROUP BY myBackupSet.media_set_id
			                           ) AS myUniqueBackupSet on myUniqueBackupSet.media_set_id=myMediaSet.media_set_id
                           WHERE myMediaSet.mirror=0 AND myMediaSet.physical_device_name LIKE '_:%'
                           ORDER BY myUniqueBackupSet.backup_start_date desc ,myMediaSet.media_set_id desc"
    $dataTable = New-Object System.Data.DataTable
    $dataTable = ExecuteReader -SqlInstance $SqlInstance -DatabaseName $DatabaseName -query $myBackupHistoryQuery
    return ,$dataTable
}

#Copy Backup to DisasterPath
Function Copy-Backup {
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][string]$InstanceName,
        [Parameter(Mandatory=$true, Position=1)][datetime]$BackupStartDate,
        [Parameter(Mandatory=$true, Position=2)][string]$BackupFileName,
        [Parameter(Mandatory=$true, Position=3)][string]$PhysicalFile,
        [Parameter(Mandatory=$true, Position=4)][string]$Destination
     )
    if ((Test-Path -Path $PhysicalFile)) 
    {
        $myDayOfWeek=''
        SWITCH($BackupStartDate.DayOfWeek)
        {
            'Saturday' {$myDayOfWeek = '01'}
            'Sunday'   {$myDayOfWeek = '02'}
            'Monday'   {$myDayOfWeek = '03'}
            'Tuesday'  {$myDayOfWeek = '04'}
            'Wednesday'{$myDayOfWeek = '05'}
            'Thursday' {$myDayOfWeek = '06'}
            'Friday'   {$myDayOfWeek = '07'}
        }
        $InstanceName = $InstanceName.Replace('\','_')
        $Destination = $Destination + "\" + $myDayOfWeek + "\" + $InstanceName
        $myDestinationFile = $Destination + "\" + $BackupFileName
        if (!(Test-Path -Path "Microsoft.PowerShell.Core\FileSystem::$Destination"))
        {
            New-Item -Path "Microsoft.PowerShell.Core\FileSystem::$Destination" -ItemType Directory 
        }
        try
        {
            Copy-Item -Path "Microsoft.PowerShell.Core\FileSystem::$PhysicalFile" -Destination "Microsoft.PowerShell.Core\FileSystem::$myDestinationFile" -Force
        }
        catch
        {
            $myDestinationFile = [string]::Empty
            throw $PSItem
        }
        return ,$myDestinationFile
    }
}

#
Function FeedBack-TransferBackup 
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][string]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$DBADatabase,
        [Parameter(Mandatory=$true, Position=2)][string]$MachineName,
        [Parameter(Mandatory=$true, Position=3)][string]$ServerName,
        [Parameter(Mandatory=$true, Position=4)][string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=5)][string]$BackupFileName,
        [Parameter(Mandatory=$true, Position=6)][string]$BackupType,
        [Parameter(Mandatory=$true, Position=7)][decimal]$BackupFirstLSN ,
        [Parameter(Mandatory=$true, Position=8)][decimal]$BackupLastLSN,
        [Parameter(Mandatory=$true, Position=9)][datetime]$BackupStartDate,
        [Parameter(Mandatory=$true, Position=10)][datetime]$BackupFinishDate,
        [Parameter(Mandatory=$true, Position=11)][string]$DestinationFile
    )
    $myBackupRepositoryTransferQuery = "EXECUTE dbo.dbasp_backuprepository_transfer @MachineName = '$MachineName', @ServerName = '$ServerName', @DatabaseName = '$DatabaseName', @BackupFileName = '$BackupFileName', @BackupType = '$BackupType', @BackupFirstLSN = $BackupFirstLSN, @BackupLastLSN = $BackupLastLSN, @BackupStartDate = '$BackupStartDate', @BackupFinishDate = '$BackupFinishDate', @RemotePath = '$DestinationFile'"

    ExecuteNonQuery -SqlInstance $SqlInstance -DatabaseName $DBADatabase -Query $myBackupRepositoryTransferQuery
}

#Update msdb database To LogCopyBackup
Function Update-MSDB {
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][string]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=3)][int]$BackupSetId,
        [Parameter(Mandatory=$true, Position=4)][string]$TransferedSuffix
    )
    $myBackupSetQuery = "UPDATE myBackupSet SET description = ISNULL(description,'') + '$TransferedSuffix' FROM msdb.dbo.backupset as myBackupSet WHERE myBackupSet.backup_set_id = $BackupSetId"
    ExecuteNonQuery -SqlInstance $SqlInstance -DatabaseName $DatabaseName -Query $myBackupSetQuery
}

#--------------------------------------------------------------Main Body
#Validating input parameters
If(-not($ServerName)) 
{
    $ServerName=$env:COMPUTERNAME
}
If(-not($DBADatabase)) 
{
    $DBADatabase="DBA"
}

[string]$mySqlInstance =""
if($Instance -match "MSSQLSERVER")
{
    $mySqlInstance = $ServerName
}
else
{
    $mySqlInstance = $ServerName + "\" + $Instance
}
try
{
    [string]$errortext=""
    [int]$myIterator = 0
    $myBackupHistoryDataTable = Get-BackupHistory -SqlInstance $mySqlInstance -DatabaseName $DBADatabase -DisasterPathId $DisasterPathId -BackupType $BackupType -TransferedSuffix $TransferedSuffix -ScanLastXHours $ScanLastXHours
    foreach($myBackupHistoryRow in $myBackupHistoryDataTable.Rows )
    {
        [int]$myBackupHistoryCount= $myBackupHistoryDataTable.Rows.count
        try
        {
            $myBackupSetId = $myBackupHistoryRow["BackupSetId"]
            $myPhysicalFile = $myBackupHistoryRow["PhysicalFile"]
            $myMachineName = $myBackupHistoryRow["MachineName"]
            $myInstanceName = $myBackupHistoryRow["InstanceName"]
            $myDatabaseName = $myBackupHistoryRow["DatabaseName"]
            $myBackupType = $myBackupHistoryRow["BackupType"]
            $myBackupFileName = $myBackupHistoryRow["BackupFileName"]
            $myBackupStartDate = $myBackupHistoryRow["BackupStartDate"]
            $myBackupFinishDate = $myBackupHistoryRow["BackupFinishDate"]
            $myBackupFirstLSN = $myBackupHistoryRow["BackupFirstLSN"]
            $myBackupLastLSN = $myBackupHistoryRow["BackupLastLSN"]
            $myFamilySequenceNumber = $myBackupHistoryRow["family_sequence_number"]
            $myMaxFamilySequenceNumber = $myBackupHistoryRow["max_family_sequence_number"]
            $myDestinationPath = $DestinationPath
            if($PrintInfo)
            {
                Write-Host 'physicalFile : ' $myPhysicalFile -ForegroundColor Gray
            }
            $myDestinationFile = Copy-Backup -InstanceName $myInstanceName -BackupStartDate $myBackupStartDate -BackupFileName $myBackupFileName -PhysicalFile $myPhysicalFile -Destination $myDestinationPath 
            if($PrintInfo)
            {
                Write-Host 'destinationFile : ' $myDestinationFile -ForegroundColor Gray
            }
            if(![string]::IsNullOrEmpty($myDestinationFile))
            {
                $myDestinationFile = $myDestinationFile.REPLACE($DestinationPath,"")
                FeedBack-TransferBackup -SqlInstance $mySqlInstance -DBADatabase $DBADatabase -MachineName $myMachineName -ServerName $ServerName -DatabaseName $myDatabaseName -BackupFileName $myBackupFileName -BackupType $myBackupType -BackupFirstLSN $myBackupFirstLSN -BackupLastLSN $myBackupLastLSN -BackupStartDate $myBackupStartDate -BackupFinishDate $myBackupFinishDate -DestinationFile $myDestinationFile
            }
            if($myFamilySequenceNumber -eq $myMaxFamilySequenceNumber)
            {
                Update-MSDB -SqlInstance $mySqlInstance -DatabaseName $myDatabaseName -BackupSetId $myBackupSetId -TransferedSuffix $TransferedSuffix
            }
            $myIterator = $myIterator + 1
            if($PrintInfo)
            {
                Write-Host "Transfer file {$myIterator} of {$myBackupHistoryCount}" -ForegroundColor Green
            }
        }
        catch{
            $errortext += $_ | Select-Object *  | Out-String
            #throw $PSItem
        }
    }
    if ($errortext -ne "")
    {
        throw $errortext
    }
}
catch
{
    #Write-Error $PSItem.ToString()
    throw $PSItem
}