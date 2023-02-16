param 
(
    [Parameter(Mandatory=$true)][string]$Auth,
    [Parameter(Mandatory=$true)][string]$Organization,
    [Parameter(Mandatory=$true)][string]$Project,
    [Parameter(Mandatory=$true)][string]$LocalPath
)
#--===============================================
$myBytes = [System.Text.Encoding]::ASCII.GetBytes($Auth)
$myToken = [System.Convert]::ToBase64String($myBytes) 
$myHeader = @{ Authorization = "Basic $myToken" }

$myURL = ("https://azure.okco.ir/$Organization/$Project/_apis/git/repositories").Replace(" ","%20")
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