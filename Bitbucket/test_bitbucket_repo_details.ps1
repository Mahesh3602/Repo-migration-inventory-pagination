param (
    [string]$BaseUrl           = "",
    [string]$Pat               = "",
    [string]$OutputCsv         = "BB_repo_inventory.csv",
    [string]$ProjectCountCsv   = "BB_project_repo_count.csv",
    [string]$ErrorLog          = "BB_repo_inventory_errors.log",
    [int]$DelayMs              = 250,
    [int]$MaxRetries           = 6
)

$headers = @{ Authorization = "Bearer $Pat"; "Content-Type" = "application/json" }

# ── Cleanup ───────────────────────────────────────────────────────────────────
foreach ($file in $OutputCsv, $ProjectCountCsv, $ErrorLog) { if (Test-Path $file) { Remove-Item $file -Force } }

$script:csvInitialized = $false
$allProjectStats = New-Object System.Collections.Generic.List[PSCustomObject]
$totalRepos = 0
$totalSkipped = 0

# ── Helper: Logging ───────────────────────────────────────────────────────────
function Write-Log {
    param([string]$Message)
    "[(Get-Date -Format 'HH:mm:ss')] $Message" | Out-File -FilePath $ErrorLog -Append
}

# ── API Wrapper ───────────────────────────────────────────────────────────────
function Invoke-BitbucketApi {
    param ([string]$Url)
    $attempt = 0
    while ($attempt -lt $MaxRetries) {
        try {
            if ($DelayMs -gt 0) { Start-Sleep -Milliseconds $DelayMs }
            return Invoke-RestMethod -Uri $Url -Headers $headers -Method Get -ErrorAction Stop
        }
        catch {
            $attempt++
            $statusCode = $_.Exception.Response.StatusCode.value__
            if ($statusCode -in 429, 503, 504) { Start-Sleep -Seconds ($attempt * 2) }
            else { return $null }
        }
    }
    return $null
}

# ── Step 1: Fetch Projects ────────────────────────────────────────────────────
Write-Host "`nFetching projects..." -ForegroundColor Cyan
$allProjects = New-Object System.Collections.Generic.List[object]
$isLastProjPage = $false; $projStart = 0
while (-not $isLastProjPage) {
    $response = Invoke-BitbucketApi -Url "$BaseUrl/rest/api/1.0/projects?start=$projStart&limit=100"
    if ($null -eq $response) { $totalSkipped++; break }
    foreach ($p in $response.values) { $allProjects.Add($p) }
    Write-Host "  Fetched $($allProjects.Count) projects so far..." -ForegroundColor Gray
    $isLastProjPage = $response.isLastPage; $projStart = $response.nextPageStart
}

$totalProjCount = $allProjects.Count
Write-Host "Total projects found: $totalProjCount`n" -ForegroundColor Green

# ── Step 2: Fetch Repos ───────────────────────────────────────────────────────
$currentProjIdx = 1
foreach ($proj in $allProjects) {
    $projKey = $proj.key
    $repoCountForThisProject = 0
    $pageIdx = 1
    
    Write-Host "[$currentProjIdx/$totalProjCount] $($proj.name)" -ForegroundColor Cyan
    
    $isLastRepoPage = $false; $repoStart = 0
    while (-not $isLastRepoPage) {
        $repoResponse = Invoke-BitbucketApi -Url "$BaseUrl/rest/api/1.0/projects/$projKey/repos?start=$repoStart&limit=100"
        
        if ($null -eq $repoResponse) { 
            $totalSkipped++
            Write-Log -Message "Failed to fetch repos for Project: $projKey"
            break 
        }
        
        $reposOnPage = $repoResponse.values.Count
        $repoCountForThisProject += $reposOnPage
        $totalRepos += $reposOnPage
        
        Write-Host "  Page $pageIdx — $reposOnPage repos (running total: $repoCountForThisProject)" -ForegroundColor Gray
        
        foreach ($repo in $repoResponse.values) {
            $sizeMB = if ($repo.size) { [math]::Round($repo.size / 1MB, 2) } else { "0" }
            $row = [PSCustomObject]@{
                "Project"        = $proj.name
                "Repository"     = $repo.name
                "RepoUrl"        = ($repo.links.clone | Where-Object { $_.name -eq 'https' -or $_.name -eq 'http' } | Select-Object -First 1).href
                "MetadataSizeMB" = "$sizeMB"
                "IsDisabled"     = if ($repo.status -eq 'ARCHIVED') { "True" } else { "False" }
            }

            if (-not $script:csvInitialized) {
                $row | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8
                $script:csvInitialized = $true
            } else {
                $row | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8 -Append
            }
        }
        $isLastRepoPage = $repoResponse.isLastPage
        $repoStart = $repoResponse.nextPageStart
        $pageIdx++
    }

    Write-Host "  Total repos for project: $repoCountForThisProject`n"
    $allProjectStats.Add([PSCustomObject]@{ "Project" = $proj.name; "RepoCount" = "$repoCountForThisProject" })
    $currentProjIdx++
}

# ── Finalize ──────────────────────────────────────────────────────────────────
$allProjectStats | Export-Csv -Path $ProjectCountCsv -NoTypeInformation -Encoding UTF8

# =============================================================================
#  SUMMARY
# =============================================================================
Write-Host "`n===== INVENTORY COMPLETE =====" -ForegroundColor Green
Write-Host "Total Projects  : $($allProjects.Count)"
Write-Host "Total Repos     : $totalRepos"
Write-Host "Skipped Projects: $totalSkipped"
Write-Host "Repo Inventory  : $OutputCsv"
Write-Host "Project Counts  : $ProjectCountCsv"
if ($totalSkipped -gt 0) {
    Write-Host "Error Log       : $ErrorLog  (review skipped items)" -ForegroundColor Yellow
}