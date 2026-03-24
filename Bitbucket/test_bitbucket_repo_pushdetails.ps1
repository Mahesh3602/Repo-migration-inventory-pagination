# ============================================================
#  Bitbucket Repository Activity Analysis Script
#  - Paginated project fetching (REST API 1.0)
#  - Sequential repo fetching with 100-item page limits
#  - Deep-dive activity analysis via Commits API
#  - Probes UI-style /sizes for accurate MetadataSizeMB
#  - Custom Activity Thresholds (Active/Stale/Inactive/Empty)
#  - Http access token(Project admin & Repo admin permissions) is used for authentication.
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

# ── Step 0: Mandatory Cleanup ─────────────────────────────────────────────────
foreach ($file in $OutputCsv, $ErrorLog) { if (Test-Path $file) { Remove-Item $file -Force } }

$script:csvInitialized = $false
$totalRepos = 0
$totalSkipped = 0

# ── Helper: API Wrapper ───────────────────────────────────────────────────────
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
            if ($statusCode -eq 404) { return $null }
            $attempt++
            if ($statusCode -in 429, 503, 504) { Start-Sleep -Seconds ($attempt * 2) }
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
    # current limit is set to 2, set it to 100 for production efficiency
    $response = Invoke-BitbucketApi -Url "$BaseUrl/rest/api/1.0/projects?start=$projStart&limit=2"
    if ($null -eq $response) { break }
    foreach ($p in $response.values) { $allProjects.Add($p) }
    Write-Host "  Fetched $($allProjects.Count) projects so far..." -ForegroundColor Gray
    $isLastProjPage = $response.isLastPage; $projStart = $response.nextPageStart
}

$totalProjCount = $allProjects.Count
Write-Host "Total projects found: $totalProjCount" -ForegroundColor Green

# ── Step 2: Fetch Repos & Details ─────────────────────────────────────────────
$currentProjIdx = 1
foreach ($proj in $allProjects) {
    $projKey = $proj.key
    Write-Host "`n[$currentProjIdx/$totalProjCount] $($proj.name)" -ForegroundColor Cyan
    
    $projectRepos = New-Object System.Collections.Generic.List[object]
    $isLastRepoPage = $false; $repoStart = 0
    $repoPageIdx = 1
    $runningTotalForProj = 0

    while (-not $isLastRepoPage) {
        # current limit is set to 2, set it to 100 for production efficiency
        $repoResponse = Invoke-BitbucketApi -Url "$BaseUrl/rest/api/1.0/projects/$projKey/repos?start=$repoStart&limit=2"
        if ($null -eq $repoResponse) { $totalSkipped++; break }
        
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
            $repoSlug = $repo.slug

            # 1. Fetch Accurate Size
            $sizeMB = "0.00"
            # Based on tested in Postman
            $sizeRes = Invoke-BitbucketApi -Url "$BaseUrl/projects/$projKey/repos/$repoSlug/sizes"
            if ($null -ne $sizeRes -and $sizeRes.repository) {
                $sizeMB = [math]::Round($sizeRes.repository / 1MB, 2)
            }

            # 2. Fetch Last Commit
            $commitRes = Invoke-BitbucketApi -Url "$BaseUrl/rest/api/1.0/projects/$projKey/repos/$repoSlug/commits?limit=1"
            $lastDate = "No Commits"
            $activityStatus = "Empty"
            
            if ($commitRes.values -and $commitRes.values.Count -gt 0) {
                $rawDate = [datetimeoffset]::FromUnixTimeMilliseconds($commitRes.values[0].authorTimestamp).DateTime
                $lastDate = $rawDate.ToString("yyyy-MM-dd hh:mm tt")
                
                $daysSince = (New-TimeSpan -Start $rawDate -End (Get-Date)).TotalDays
                if ($daysSince -lt $activeThresholdDays) { $activityStatus = "Active" }
                elseif ($daysSince -lt $staleThresholdDays) { $activityStatus = "Stale" }
                else { $activityStatus = "Inactive" }
            }

            # ── Construct Row (Your Requested Order) ──
            $row = [PSCustomObject]@{
                "Project"         = $proj.name
                "ProjectKey"      = $projKey
                "Repository"      = $repo.name
                "RepoUrl"         = ($repo.links.clone | Where-Object { $_.name -match 'http' } | Select-Object -First 1).href
                "MetadataSizeMB"  = "$sizeMB"
                "IsDisabled"      = if ($repo.archived -or $repo.status -eq 'ARCHIVED') { "True" } else { "False" }
                "LastCommitDate"  = $lastDate
                "ActivityStatus"  = $activityStatus
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
Write-Host "Total Projects  : $totalProjCount"
Write-Host "Total Repos     : $totalRepos"
Write-Host "Skipped Projects: $totalSkipped"
Write-Host "Repo Inventory  : $OutputCsv"