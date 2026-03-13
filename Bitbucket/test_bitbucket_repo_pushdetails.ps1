param (
    [string]$BaseUrl           = "",
    [string]$Pat               = "",
    [string]$OutputCsv         = "BB_repo_inventory_updated.csv",
    [string]$ErrorLog          = "BB_repo_inventory_errors.log",
    [int]$DelayMs              = 250,
    [int]$MaxRetries           = 6
)

$headers = @{ Authorization = "Bearer $Pat"; "Content-Type" = "application/json" }

# ── Cleanup ───────────────────────────────────────────────────────────────────
foreach ($file in $OutputCsv, $ErrorLog) { if (Test-Path $file) { Remove-Item $file -Force } }

$script:csvInitialized = $false
$totalRepos = 0
$totalSkipped = 0

# ── API Wrapper (With 404 Graceful Handling) ──────────────────────────────────
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
            
            # If 404, the repo is likely empty; return null instead of retrying
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
    $isLastProjPage = $response.isLastPage; $projStart = $response.nextPageStart
}

# ── Step 2: Fetch Repos & Details ─────────────────────────────────────────────
$currentProjIdx = 1
foreach ($proj in $allProjects) {
    Write-Host "[$currentProjIdx/$($allProjects.Count)] $($proj.name)" -ForegroundColor Cyan
    $isLastRepoPage = $false; $repoStart = 0
    
    while (-not $isLastRepoPage) {
        $repoResponse = Invoke-BitbucketApi -Url "$BaseUrl/rest/api/1.0/projects/$($proj.key)/repos?start=$repoStart&limit=100"
        if ($null -eq $repoResponse) { break }
        
        foreach ($repo in $repoResponse.values) {
            $totalRepos++

            # Fetch Last Commit Date (Handles 404 for empty repos)
            $commitUrl = "$BaseUrl/rest/api/1.0/projects/$($proj.key)/repos/$($repo.slug)/commits?limit=1"
            $commitRes = Invoke-BitbucketApi -Url $commitUrl
            $lastDate = ""
            if ($commitRes.values -and $commitRes.values.Count -gt 0) {
                $lastDate = [datetimeoffset]::FromUnixTimeMilliseconds($commitRes.values[0].authorTimestamp).DateTime.ToString("yyyy-MM-dd hh:mm tt")
            }

            # Fetch PR Count
            $prUrl = "$BaseUrl/rest/api/1.0/projects/$($proj.key)/repos/$($repo.slug)/pull-requests?state=ALL&limit=1"
            $prRes = Invoke-BitbucketApi -Url $prUrl
            $prCount = if ($prRes) { $prRes.size } else { 0 }

            # Prepare Single Output Row
            $row = [PSCustomObject]@{
                "project-key"               = $proj.key
                "project-name"              = $proj.name
                "repo"                      = $repo.name
                "url"                       = ($repo.links.clone | Where-Object { $_.name -eq 'http' -or $_.name -eq 'https' } | Select-Object -First 1).href
                "last-commit-date"          = $lastDate
                "repo-size-in-bytes"        = if ($repo.size) { $repo.size } else { "0" }
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
        $isLastRepoPage = $repoResponse.isLastPage
        $repoStart = $repoResponse.nextPageStart
    }
    $currentProjIdx++
}

# ── Final Summary (Console Only) ──────────────────────────────────────────────
Write-Host "`n===== INVENTORY COMPLETE =====" -ForegroundColor Green
Write-Host "Total Projects : $($allProjects.Count)"
Write-Host "Total Repos    : $totalRepos"
Write-Host "Output File    : $OutputCsv"