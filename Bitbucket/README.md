## Setup env variables
$env:BB_URL = "BB_URL"

$env:BB_PAT = "your_secret_token_here"

## Execute scripts
pwsh ./test_bitbucket_repo_details.ps1 -BaseUrl $env:BB_URL -Pat $env:BB_PAT


pwsh ./test_bitbucket_repo_pushdetails.ps1 -BaseUrl $env:BB_URL -Pat $env:BB_PAT