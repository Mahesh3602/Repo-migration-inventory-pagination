param (
    [string]$BaseUrl           = "",
    [string]$Pat               = "",
    [string]$OutputCsv         = "BB_repo_inventory_updated.csv",
    [string]$SummaryCsv        = "BB_project_repo_counts.csv",
    [string]$ErrorLog          = "BB_repo_inventory_errors.log",
    [int]$DelayMs              = 250,
    [int]$MaxRetries           = 6,
    [int]$ThrottleLimit        = 4
)

# --- Validation Block ---
if ([string]::IsNullOrWhiteSpace($BaseUrl)) {
    Write-Host "ERROR: BaseUrl is missing." -ForegroundColor Red
    exit
}
if ([string]::IsNullOrWhiteSpace($Pat)) {
    Write-Host "ERROR: PAT token is missing." -ForegroundColor Red
    exit
}

$headers = @{ Authorization = "Bearer $Pat"; "Content-Type" = "application/json" }

# Cleanup
foreach ($file in $OutputCsv, $SummaryCsv, $ErrorLog) { if (Test-Path $file) { Remove-Item $file -Force } }

$script:csvInitialized = $false
$totalRepos = 0
$totalSkipped = 0
$projectSummaryList = New-Object System.Collections.Generic.List[PSCustomObject]

# --- API Wrapper ---
function Invoke-BBSApi {
    param ([string]$Url)
    $retry = 0
    while ($retry -lt $MaxRetries) {
        try {
            if ($DelayMs -gt 0) { Start-Sleep -Milliseconds $DelayMs }
            return Invoke-RestMethod -Uri $Url -Headers $headers -Method Get -ErrorAction Stop
        } catch {
            $sc = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
            if ($sc -in 429, 503, 504) {
                $wait = [math]::Min(60, [math]::Pow(2, $retry + 1))
                Start-Sleep -Seconds $wait
                $retry++
            } else { 
                "ERROR: $($_.Exception.Message) at $Url" | Out-File -FilePath $ErrorLog -Append
                return $null 
            }
        }
    }
    return $null
}

# --- Step 1: Projects ---
Write-Host "`nFetching projects..." -ForegroundColor Cyan
$allProjects = New-Object System.Collections.Generic.List[object]
$start = 0; $isLast = $false
while (-not $isLast) {
    $res = Invoke-BBSApi -Url "$BaseUrl/rest/api/1.0/projects?start=$start&limit=100"
    if ($null -eq $res) { $totalSkipped++; break }
    foreach ($p in $res.values) { $allProjects.Add($p) }
    $isLast = $res.isLastPage; $start = $res.nextPageStart
}
Write-Host "Total projects found: $($allProjects.Count)" -ForegroundColor Green

# --- Step 2: Repos & Metadata ---
$currentProjIdx = 1
foreach ($proj in $allProjects) {
    Write-Host "`n[$currentProjIdx/$($allProjects.Count)] $($proj.name)" -ForegroundColor Cyan
    $projectRepos = New-Object System.Collections.Generic.List[object]
    $start = 0; $isLast = $false
    while (-not $isLast) {
        $res = Invoke-BBSApi -Url "$BaseUrl/rest/api/1.0/projects/$($proj.key)/repos?start=$start&limit=100"
        if ($null -eq $res) { break }
        foreach ($r in $res.values) { $projectRepos.Add($r) }
        $isLast = $res.isLastPage; $start = $res.nextPageStart
    }

    # Add to Summary List
    $projectSummaryList.Add([PSCustomObject]@{
        Project   = $proj.name
        RepoCount = $projectRepos.Count
    })

    foreach ($repo in $projectRepos) {
        $totalRepos++
        
        # 1. Fetch Last Commit Date
        $commitRes = Invoke-BBSApi -Url "$BaseUrl/rest/api/1.0/projects/$($proj.key)/repos/$($repo.slug)/commits?limit=1"
        $lastCommitDate = ""
        if ($commitRes.values -and $commitRes.values.Count -gt 0) {
            $lastCommitDate = [datetimeoffset]::FromUnixTimeMilliseconds($commitRes.values[0].authorTimestamp).DateTime.ToString("yyyy-MM-dd hh:mm tt")
        }

        # 2. Fetch PR Count (Total)
        $prRes = Invoke-BBSApi -Url "$BaseUrl/rest/api/1.0/projects/$($proj.key)/repos/$($repo.slug)/pull-requests?state=ALL&limit=1"
        $prCount = if ($prRes) { $prRes.size } else { 0 }

        # Prepare Row in your specified format
        $row = [PSCustomObject]@{
            "project-key"               = $proj.key
            "project-name"              = $proj.name
            "repo"                      = $repo.name
            "url"                       = ($repo.links.clone | Where-Object { $_.name -eq 'http' -or $_.name -eq 'https' } | Select-Object -First 1).href
            "last-commit-date"          = $lastCommitDate
            "repo-size-in-bytes"        = "0"
            "attachments-size-in-bytes" = "0"
            "is-archived"               = if ($repo.status -eq 'ARCHIVED') { "True" } else { "False" }
            "pr-count"                  = $prCount
        }

        if (-not $script:csvInitialized) {
            $row | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8
            $script:csvInitialized = $true
        } else {
            $row | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8 -Append
        }
    }
    $currentProjIdx++
}

# Export Project Counts Summary
$projectSummaryList | Export-Csv -Path $SummaryCsv -NoTypeInformation -Encoding UTF8

# =============================================================================
#  SUMMARY
# =============================================================================
Write-Host "`n===== INVENTORY COMPLETE =====" -ForegroundColor Green
Write-Host "Total Projects  : $($allProjects.Count)"
Write-Host "Total Repos     : $totalRepos"
Write-Host "Skipped Projects: $totalSkipped"
Write-Host "Repo Inventory  : $OutputCsv"
Write-Host "Project Counts  : $SummaryCsv"
if ($totalSkipped -gt 0) {
    Write-Host "Error Log       : $ErrorLog  (review skipped items)" -ForegroundColor Yellow
}