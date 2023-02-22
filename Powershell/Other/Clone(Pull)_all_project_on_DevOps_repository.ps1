#Guid to making Access token :  https://chuvash.eu/2022/02/21/automate-creation-of-git-repos-using-azure-devops-api/
[Parameter(Mandatory=$true)][string]$TokenName = "TokenName"
[Parameter(Mandatory=$true)][string]$Token = "Token"
[Parameter(Mandatory=$true)][string]$DevOpsURL = "https://dev.azure.com" # my Azure DevOps URL
[Parameter(Mandatory=$true)][string]$Organization = "" # my Azure DevOps organization
[Parameter(Mandatory=$true)][string]$Project = "" # my Azure DevOps Project
[Parameter(Mandatory=$true)][string]$LocalPath = "C:\SqlDeep\Source"
#--===============================================
$myAuth = $TokenName + ":" + $Token
$myBytes = [System.Text.Encoding]::ASCII.GetBytes($myAuth)
$myToken = [System.Convert]::ToBase64String($myBytes) 
$myHeader = @{ Authorization = "Basic $myToken" }

$myURL = ("$DevOpsURL/$Organization/$Project/_apis/git/repositories").Replace(" ","%20")
$myResponse = Invoke-WebRequest -Method GET -Uri $myURL -Headers $myHeader
$myRepositories = $myResponse | ConvertFrom-Json | Select-Object -ExpandProperty value
foreach($myRepo in $myRepositories)
{
    $myRepoName = $myRepo.name
    $myRepoWebUrl = $myRepo.webUrl
    if($myRepoName -like 'DBA_*'){
        $mySourcePath = $LocalPath + "\DBA"
    }
    elseif($myRepoName -like 'BI_*'){
        $mySourcePath = $LocalPath + "\BI"
    }
    elseif($myRepoName -like 'ERP_*'){
        $mySourcePath = $LocalPath + "\ERP"
    }
    else{
        $mySourcePath = $LocalPath + "\Other"
    }
    if(!(Test-Path -path $mySourcePath))
    {
        New-Item -ItemType Directory -Path $mySourcePath | Out-Null
    }
    Set-Location $mySourcePath
    $mySourcePath = $mySourcePath + "\" + $myRepoName
    $myGitURL = $myRepoWebUrl.Replace(" ","%20").ToString()
    if(!(Test-Path $mySourcePath))
    {
        git clone $myGitURL
    }
    else{
        Set-Location $mySourcePath
        git pull $myGitURL
    }
}