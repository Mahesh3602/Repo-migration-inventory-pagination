## Setup env variables
$env:ADO_ORG = "ADO_ORG"

$env:ADO_PAT = "TOKEN"

## Execute scripts
pwsh ./test_ado_repo_details.ps1 -Org $env:ADO_ORG -Pat $env:ADO_PAT



pwsh ./test_ado_repo_pushdetails.ps1 -Org $env:ADO_ORG -Pat $env:ADO_PAT



pwsh ./fetch_buildpipelines.ps1 -Org $env:ADO_ORG -Pat $env:ADO_PAT

