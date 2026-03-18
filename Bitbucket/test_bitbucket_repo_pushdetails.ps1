# ============================================================
#  Bitbucket Repository Inventory Script
#  - Paginated project fetching (REST API 1.0)
#  - Sequential repo fetching with 100-item page limits
#  - Deep-dive activity analysis via Commits API
#  - Custom Activity Thresholds (Active/Stale/Inactive/Empty)
#  - Automated Unix-to-DateTime timestamp conversion
#  - Graceful 404 handling for restricted/empty metadata
#  - Intelligent API throttling and Max-Retry logic
#  - Clean CSV appending (No redundant headers)
# ============================================================

param (
    [string]$BaseUrl           = "",
    [string]$Pat               = "",
    [string]$OutputCsv         = "BB_repo_inventory_updated.csv",
    [string]$ErrorLog          = "BB_repo_inventory_errors.log",
    [int]$DelayMs              = 250,
    [int]$MaxRetries           = 6,
    [int]$activeThresholdDays  = 180,  # < 6 months
    [int]$staleThresholdDays   = 365   # 6-12 months (Stale), > 12 months (Inactive)
)

$headers = @{ Authorization = "Bearer $Pat"; "Content-Type" = "application/json" }

# ── Cleanup ───────────────────────────────────────────────────────────────────
foreach ($file in $OutputCsv, $ErrorLog) { if (Test-Path $file) { Remove-Item $file -Force } }

$script:csvInitialized = $false
$totalRepos = 0
$totalSkipped = 0

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
            $statusCode = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
            # Gracefully handle missing endpoints (like size/info)
            if ($statusCode -eq 404) { return $null }

            $attempt++
            if ($statusCode -in 429, 503, 504) { 
                Start-Sleep -Seconds ($attempt * 2) 
            }
            else { 
                "ERROR: $($_.Exception.Message) at $Url" | Out-File -FilePath $ErrorLog -Append
                return $null 
            }
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
    if ($null -eq $response) { break }
    foreach ($p in $response.values) { $allProjects.Add($p) }
    
    Write-Host "  Fetched $($allProjects.Count) projects so far..." -ForegroundColor Gray
    $isLastProjPage = $response.isLastPage; $projStart = $response.nextPageStart
}

Write-Host "Total projects found: $($allProjects.Count)" -ForegroundColor Green

# ── Step 2: Fetch Repos & Details ─────────────────────────────────────────────
$currentProjIdx = 1
foreach ($proj in $allProjects) {
    Write-Host "`n[$currentProjIdx/$($allProjects.Count)] $($proj.name)" -ForegroundColor Cyan
    $isLastRepoPage = $false; $repoStart = 0
    $repoPageIdx = 1
    $runningTotalForProj = 0
    $projectRepos = New-Object System.Collections.Generic.List[object]
    
    while (-not $isLastRepoPage) {
        $repoResponse = Invoke-BitbucketApi -Url "$BaseUrl/rest/api/1.0/projects/$($proj.key)/repos?start=$repoStart&limit=100"
        if ($null -eq $repoResponse) { break }
        
        $reposOnPage = $repoResponse.values.Count
        $runningTotalForProj += $reposOnPage
        
        Write-Host "  Repo page $repoPageIdx — $reposOnPage repos (running total: $runningTotalForProj)" -ForegroundColor Gray
        foreach ($r in $repoResponse.values) { $projectRepos.Add($r) }

        $isLastRepoPage = $repoResponse.isLastPage
        $repoStart = $repoResponse.nextPageStart
        $repoPageIdx++
    }

    if ($projectRepos.Count -gt 0) {
        Write-Host "  Fetching activity status sequentially for $($projectRepos.Count) repos..." -ForegroundColor Gray
        
        foreach ($repo in $projectRepos) {
            $totalRepos++

            # Fetch Last Commit Date & Activity Logic
            $commitRes = Invoke-BitbucketApi -Url "$BaseUrl/rest/api/1.0/projects/$($proj.key)/repos/$($repo.slug)/commits?limit=1"
            $lastDate = ""
            $activityStatus = "Empty"
            
            if ($commitRes.values -and $commitRes.values.Count -gt 0) {
                # Convert Unix Milliseconds to PowerShell DateTime
                $rawDate = [datetimeoffset]::FromUnixTimeMilliseconds($commitRes.values[0].authorTimestamp).DateTime
                $lastDate = $rawDate.ToString("yyyy-MM-dd hh:mm tt")
                
                # Activity categorization based on user thresholds
                $daysSince = (New-TimeSpan -Start $rawDate -End (Get-Date)).TotalDays
                if ($daysSince -lt $activeThresholdDays) { $activityStatus = "Active" }
                elseif ($daysSince -lt $staleThresholdDays) { $activityStatus = "Stale" }
                else { $activityStatus = "Inactive" }
            }

            # Build the cleaned-up output object
            $row = [PSCustomObject]@{
                "project-key"        = $proj.key
                "project-name"       = $proj.name
                "repo"               = $repo.name
                "url"                = ($repo.links.clone | Where-Object { $_.name -match 'http' } | Select-Object -First 1).href
                "last-commit-date"   = $lastDate
                "repo-size-in-bytes" = if ($repo.size) { $repo.size } else { "0" }
                "activity-status"    = $activityStatus
            }

            if (-not $script:csvInitialized) {
                $row | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8
                $script:csvInitialized = $true
            } else {
                $row | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8 -Append
            }
        }
    }
    
    Write-Host "  Total repos: $($projectRepos.Count)" -ForegroundColor Gray
    $currentProjIdx++
}

# ── Final Summary ─────────────────────────────────────────────────────────────
Write-Host "`n===== INVENTORY COMPLETE =====" -ForegroundColor Green
Write-Host "Total Projects  : $($allProjects.Count)"
Write-Host "Total Repos     : $totalRepos"
Write-Host "Repo Inventory  : $OutputCsv"