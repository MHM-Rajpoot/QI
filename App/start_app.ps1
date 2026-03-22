param(
    [string]$PythonCommand = "python"
)

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$setupGuide = Join-Path $projectRoot "secrets\environment_variables_setup.txt"
$secretKeyFile = Join-Path $projectRoot "secrets\secret_key.txt"
$passcodeFile = Join-Path $projectRoot "secrets\passcode.txt"

$requiredVariables = @(
    "SECRET_KEY",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_DATABASE"
)

$optionalVariables = @(
    "SNOWFLAKE_WAREHOUSE"
)

function Import-UserEnvironmentVariable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if (-not [string]::IsNullOrWhiteSpace((Get-Item "Env:$Name" -ErrorAction SilentlyContinue).Value)) {
        return
    }

    $userValue = [Environment]::GetEnvironmentVariable($Name, "User")
    if (-not [string]::IsNullOrWhiteSpace($userValue)) {
        Set-Item -Path "Env:$Name" -Value $userValue
    }
}

foreach ($name in $requiredVariables + $optionalVariables) {
    Import-UserEnvironmentVariable -Name $name
}

$missingRequiredVariables = @(
    $requiredVariables | Where-Object {
        [string]::IsNullOrWhiteSpace((Get-Item "Env:$_" -ErrorAction SilentlyContinue).Value)
    }
)

$hasEnvironmentCredentials = $missingRequiredVariables.Count -eq 0
$hasSecretKeyFile = Test-Path $secretKeyFile
$hasPasscodeFile = Test-Path $passcodeFile

if (-not $hasEnvironmentCredentials -and -not $hasPasscodeFile) {
    throw "Snowflake settings are missing from both environment variables and $passcodeFile. See $setupGuide"
}

if ([string]::IsNullOrWhiteSpace((Get-Item "Env:SECRET_KEY" -ErrorAction SilentlyContinue).Value) -and -not $hasSecretKeyFile) {
    throw "SECRET_KEY is missing from both environment variables and $secretKeyFile. See $setupGuide"
}

if ([string]::IsNullOrWhiteSpace((Get-Item "Env:SNOWFLAKE_WAREHOUSE" -ErrorAction SilentlyContinue).Value)) {
    Set-Item -Path "Env:SNOWFLAKE_WAREHOUSE" -Value "COMPUTE_WH"
}

Push-Location $projectRoot
try {
    & $PythonCommand app.py
}
finally {
    Pop-Location
}
