# sqldeep.com
# version : 3.0.0
# Author: siavash.golchoobian@gmail.com
# Author: mohammadamin.mazidi@yahoo.com
# Author: z.saffarpour@gmail.com
#--------------------------------------------------------------Parameters.
[string]$ServerName = $env:COMPUTERNAME 
[string]$InstanceName = "SqlDeep"
[string]$extendedPropertyKey="_ShrinkLogFile"
[bool]$CheckExtendedProperty = $true
#--------------------------------------------------------------Functions start here.
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") | out-null
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SqlEnum") | out-null
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SmoEnum") | out-null
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

Function Get-InitSize
{
    Param
    (
        [Parameter(Mandatory=$true, Position=0)][Microsoft.SqlServer.Management.Smo.Server]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$Database,
        [Parameter(Mandatory=$true, Position=1)][string]$extendedPropertyKey
    )
    $myDatabase = $SqlInstance.Databases[$Database]
    if (!$myDatabase.ExtendedProperties.Contains($extendedpropertykey))
    {
        return 0
    }
    $myDatabase.ExtendedProperties.Item($extendedpropertykey).value
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

function Get-Databases 
{
    param 
    (
        [Parameter(Mandatory=$true, Position=0)][Microsoft.SqlServer.Management.Smo.Server]$SqlInstance,
        [Parameter(Mandatory=$true, Position=2)][string]$CheckExtendedProperty,
        [Parameter(Mandatory=$true, Position=3)][string]$ExtendedPropertyKey
    )
    $myDatabaseList = New-Object Microsoft.SqlServer.Management.Smo.Database 
    $myDatabaseList = $SqlInstance.Databases 

    $myDatabaseTable = New-Object system.Data.DataTable "ShrinkDatabases"

    $myDatabaseNameColumn = New-Object system.Data.DataColumn DatabaseName,([string])
    $myDatabaseTable.columns.add($myDatabaseNameColumn)

    $myHasReadOnlyColumn = New-Object system.Data.DataColumn HasReadOnly,([bool])
    $myDatabaseTable.columns.add($myHasReadOnlyColumn)

    $myShrinkLogSizeColumn = New-Object system.Data.DataColumn ShrinkLogSize,([string])
    $myDatabaseTable.columns.add($myShrinkLogSizeColumn)
    [string]$myShrinkLogSize = "1024"; # Default Value
    foreach ($myDatabase in $myDatabaseList) 
    { 
        $myDatabaseName = $myDatabase.Name
        if($SqlInstance.Version.Major -ge 11)
        {
            if(!([string]::IsNullOrEmpty($myDatabase.AvailabilityGroupName)))
            {
                $IsPrimary = HasPrimaryReplica -SqlInstance $SqlInstance -Database $myDatabaseName
                if (!$IsPrimary)
                {
                    continue
                } 
            }
        }
        if ($myDatabase.RecoveryModel.ToString() -ne "Full")
        {
            continue
        }
        if ($myDatabase.Status.ToString().Contains("Offline"))
        {
            continue
        }
        if ($myDatabase.ReadOnly -eq $true)
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
            #exist Extended property 
            if ($myDatabase.ExtendedProperties.Contains($ExtendedPropertyKey))
            {
                $myShrinkLogSize = $myDatabase.ExtendedProperties.Item($ExtendedPropertyKey).value 
            }
        }

        $myDatabaseRow = $myDatabaseTable.NewRow()
        $myDatabaseRow.DatabaseName = $myDatabaseName 
        $myDatabaseRow.HasReadOnly = $myDatabase.ReadOnly
        $myDatabaseRow.ShrinkLogSize = $myShrinkLogSize
        $myDatabaseTable.Rows.Add($myDatabaseRow)
    } 

    return ,$myDatabaseTable
}

function ShrinkDatabaseLogFile
{
    param 
    (
        [Parameter(Mandatory=$true, Position=0)][Microsoft.SqlServer.Management.Smo.Server]$SqlInstance,
        [Parameter(Mandatory=$true, Position=1)][string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=2)][bool]$DatabaseHasReadOnly,
        [Parameter(Mandatory=$true, Position=3)][string]$ShrinkLogSize
    )

    $myDatabase = $SqlInstance.Databases[$DatabaseName]
    if (!($DatabaseHasReadOnly))
    {
        $myLogFileCount = $myDatabase.LogFiles.Count
        for($myIterator=0; $myIterator -le $myLogFileCount-1 ; $myIterator++)
        {
            $myDatabase.LogFiles[$myIterator].Shrink($ShrinkLogSize, [Microsoft.SqlServer.Management.Smo.ShrinkMethod]::Default)
            $myDatabase.Logfiles.refresh($true)
        }
    }
}
#--------------------------------------------------------------Main Body
[string]$errortext = [string]::Empty
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
$myDatabaseTable = Get-Databases -SqlInstance $mySqlInstance -CheckExtendedProperty $checkextendedProperty -ExtendedPropertyKey $extendedpropertykey 
foreach ($myDatabaseRow in $myDatabaseTable) 
{ 
    $myDatabaseName = $myDatabaseRow["DatabaseName"]
    $myDatabaseHasReadOnly = $myDatabaseRow["HasReadOnly"]
    $myShrinkLogSize = $myDatabaseRow["ShrinkLogSize"]
    try
    {
        ShrinkDatabaseLogFile -SqlInstance $mySqlInstance -DatabaseName $myDatabaseName -DatabaseHasReadOnly $myDatabaseHasReadOnly -ShrinkLogSize $myShrinkLogSize
    }   
    catch [Exception]
    {
        #$errortext += $_.Exception.Message  
        $errortext += $_ | Select-Object * | Out-String 
        continue
    }  
} 
if (!([string]::IsNullOrEmpty($errortext)))
{
    throw $errortext
}