$InstanceList = "Server01\SqlDeep","Server02\SqlDeep"
$DestinationPath = "U:\Databases\SSAS\LocalBackup"
$PrintOnly = $true
##=================================================================================================
# load the AMO and XML assemblies into the current runspace
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices") | Out-Null
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices.Tabular") | Out-Null
[System.Reflection.Assembly]::LoadWithPartialName("System.Xml") | Out-Null
##=================================================================================================
Function ExportOlapStructure
{
    Param
        (
            [Parameter(Mandatory=$true)][string]$InstanceName,
            [Parameter(Mandatory=$true)][string]$DatabaseName,
            [Parameter(Mandatory=$true)][string]$DestinationFolderPath,
            [Parameter(Mandatory=$true)][string]$DestinationFileName
        )
    $myServer = new-Object Microsoft.AnalysisServices.Server;
    $myServer.Connect($InstanceName);
    $myDatabase = $myServer.Databases.GetByName("$DatabaseName");
    if ($myServer.ServerMode -eq [Microsoft.AnalysisServices.ServerMode]::Multidimensional) 
    {
        $myFileName="$DestinationFileName.xmla"
        $myFilePath="$DestinationFolderPath\$myFileName"
        $myXmlWriter = new-object System.Xml.XmlTextWriter("$myFilePath", [System.Text.Encoding]::UTF8)
        $myXmlWriter.Formatting = [System.Xml.Formatting]::Indented
        [Microsoft.AnalysisServices.Scripter]::WriteCreate($myXmlWriter,$myServer,$myDatabase,$true,$true)
        $myXmlWriter.Close()
    }
    elseif ($myServer.ServerMode -eq [Microsoft.AnalysisServices.ServerMode]::Tabular) 
    {
        $myFileName="$DestinationFileName.json"
        $myFilePath="$DestinationFolderPath\$myFileName"
        [Microsoft.AnalysisServices.Tabular.JsonScripter]::ScriptCreate($myDatabase,$false) | Out-File -FilePath "$myFilePath" -Encoding utf8
    }
    $myServer.Disconnect()
    $myZipFilePath="$DestinationFolderPath\"+$myFileName.Replace(".json","").Replace(".xmla","")+".zip"
    Compress-Archive -Path $myFilePath -DestinationPath $myZipFilePath -CompressionLevel Optimal -Force
    Remove-Item -Path $myFilePath
} 
##=================================================================================================
$myDate = Get-Date
foreach($myInstance in $InstanceList)
{
    # connect to the olap server
    $myServer = new-Object Microsoft.AnalysisServices.Server;
    $myServer.Connect($myInstance);
    $myServer.Disconnect()
    $myDestinationFolderPath = $DestinationPath + "\" + ([int]$myDate.DayOfWeek).ToString("00") 
    $myDestinationFolderPath = "$myDestinationFolderPath\" + $myInstance.Replace("\","_")
    if(!(Test-Path $myDestinationFolderPath))
    {
        New-Item -Type Directory -Path $myDestinationFolderPath | Out-Null
    }
    foreach($myDatabase in $myServer.Databases)
    {
        $myDatabaseName = $myDatabase.Name
        $myDestinationFileName = $myDatabaseName + "_" + $myDate.ToString("yyyy_MM_dd_on_HH_mm_ss")
        try
        {
            if ($PrintOnly) 
            {
                Write-Host "Server:$myServer" -ForegroundColor Green
                Write-Host "DatabaseName:$myDatabaseName" -ForegroundColor Green
            }
            #Exporting an OLAP database
            ExportOlapStructure -InstanceName $myInstance -DatabaseName $myDatabaseName -DestinationFolderPath $myDestinationFolderPath -DestinationFileName $myDestinationFileName
        }
        catch [Exception]
        {
            if ($PrintOnly) 
            {
                Write-Host "Error:$_" -ForegroundColor Red
            }
            else 
            {
                $_
            }
            continue
        }
        finally 
        {
            if ($PrintOnly) 
            {
                Write-Host "--======================================" -ForegroundColor Green
            }
        }
    }
}