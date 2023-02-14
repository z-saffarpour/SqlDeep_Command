$InstanceName = "Server01\SqlDeep"
$RoleName = "Role1"
$Username = "SqlDeep\User01"
##=================================================================================================
# load the AMO and XML assemblies into the current runspace
[System.reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices") | Out-Null
##=================================================================================================
# connect to the olap server
$myServer = new-Object Microsoft.AnalysisServices.Server
$myServer.Connect($InstanceName)

foreach ($myDatabase in $myServer.Databases)
{
    [Microsoft.AnalysisServices.Role]$myRole = $null
    $myRole = $myDatabase.Roles.FindByName($RoleName)
    if($null -ne $myRole)
    {
        Write-Host " $myDatabase : " -ForegroundColor Green
        foreach ($myMember in $myRole.Members)
        {
            if( $myMember.Name -contains $Username)
            {
                Write-Host "   " $myMember.Name -ForegroundColor yellow
            }
        }
    }
}
$mySourceServer.Disconnect()