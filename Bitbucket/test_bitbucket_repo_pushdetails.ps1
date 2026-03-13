param (
    [string]$BaseUrl           = "",
    [string]$Pat               = "",
    [string]$OutputCsv         = "BB_repo_inventory_updated.csv",
    [string]$ErrorLog          = "BB_repo_inventory_errors.log",
    [int]$DelayMs              = 250,
    [int]$MaxRetries           = 6,
    [int]$ParallelThreshold    = 50,
    [int]$ThrottleLimit        = 4
)

$headers = @{ Authorization = "Bearer $Pat"; "Content-Type" = "application/json" }

# Cleanup
foreach ($file in $OutputCsv, $ErrorLog) { if (Test-Path $file) { Remove-Item $file -Force } }

$script:csvInitialized = $false
$totalRepos = 0
$totalSkipped = 0
$activeThresholdDays = 180
$staleThresholdDays  = 365

# --- Activity Logic ---
function Get-ActivityStatus {
    param ([string]$LastCommitDate)
    if ($LastCommitDate -eq "Never") { return "Never Used" }
    try {
        $daysSince = (New-TimeSpan -Start ([datetime]$LastCommitDate) -End (Get-Date)).Days
        if ($daysSince -lt $activeThresholdDays) { return "Active" }
        if ($daysSince -lt $staleThresholdDays)  { return "Stale" }
        return "Inactive"
    } catch { return "Unknown" }
}

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
            } else { return $null }
        }
    }
    return $null
}

# --- Parallel Logic ---
function Get-LastCommitParallel {
    param ([object[]]$Repos, [string]$BaseUrl, [string]$Pat, [int]$Threads)
    $results = [hashtable]::Synchronized(@{})
    $sb = {
        param($repo, $baseUrl, $pat, $results)
        $hdrs = @{ Authorization = "Bearer $pat" }
        $url  = "$baseUrl/rest/api/1.0/projects/$($repo.project.key)/repos/$($repo.slug)/commits?limit=1"
        try {
            $res = Invoke-RestMethod -Uri $url -Headers $hdrs -Method Get
            if ($res.values -and $res.values.Count -gt 0) {
                $date = [datetimeoffset]::FromUnixTimeMilliseconds($res.values[0].authorTimestamp).DateTime
                $results[$repo.id] = $date.ToString("yyyy-MM-dd HH:mm:ss")
            } else { $results[$repo.id] = "Never" }
        } catch { $results[$repo.id] = "Unknown" }
    }
    $pool = [RunspaceFactory]::CreateRunspacePool(1, $Threads)
    $pool.Open()
    $jobs = foreach ($repo in $Repos) {
        $ps = [PowerShell]::Create().AddScript($sb).AddArgument($repo).AddArgument($BaseUrl).AddArgument($Pat).AddArgument($results)
        $ps.RunspacePool = $pool
        [PSCustomObject]@{ PS = $ps; Handle = $ps.BeginInvoke() }
    }
    foreach ($j in $jobs) { $j.PS.EndInvoke($j.Handle) | Out-Null; $j.PS.Dispose() }
    $pool.Close(); $pool.Dispose()
    return $results
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

# --- Step 2: Repos ---
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

    if ($projectRepos.Count -gt 0) {
        $commitData = Get-LastCommitParallel -Repos $projectRepos -BaseUrl $BaseUrl -Pat $Pat -Threads $ThrottleLimit
        foreach ($repo in $projectRepos) {
            $totalRepos++
            $lastDate = if ($commitData.ContainsKey($repo.id)) { $commitData[$repo.id] } else { "Never" }
            $row = [PSCustomObject]@{
                "Project"        = $proj.name
                "Repository"     = $repo.name
                "RepoUrl"        = ($repo.links.clone | Where-Object { $_.name -eq 'https' } | Select-Object -First 1).href
                "MetadataSizeMB" = "0"
                "IsDisabled"     = if ($repo.status -eq 'ARCHIVED') { "True" } else { "False" }
                "LastPushDate"   = $lastDate
                "ActivityStatus" = Get-ActivityStatus -LastCommitDate $lastDate
            }
            if (-not $script:csvInitialized) {
                $row | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8
                $script:csvInitialized = $true
            } else { $row | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8 -Append }
        }
    }
    $currentProjIdx++
}

# =============================================================================
#  SUMMARY
# =============================================================================
Write-Host "`n===== INVENTORY COMPLETE =====" -ForegroundColor Green
Write-Host "Total Projects  : $($allProjects.Count)"
Write-Host "Total Repos     : $totalRepos"
Write-Host "Skipped Projects: $totalSkipped"
Write-Host "Repo Inventory  : $OutputCsv"
if ($totalSkipped -gt 0) {
    Write-Host "Error Log       : $ErrorLog  (review skipped items)" -ForegroundColor Yellow
}