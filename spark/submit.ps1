param(
    [ValidateSet("local", "dev", "prod")]
    [string]$Env = "local",
    [string[]]$AppArgs = @()
)

$propertiesFile = Join-Path $PSScriptRoot "conf\$Env.properties"
$entrypoint = Join-Path $PSScriptRoot "kafka_raw_to_bronze.py"

if (-not (Test-Path $propertiesFile)) {
    throw "Spark properties file not found: $propertiesFile"
}

if (-not (Test-Path $entrypoint)) {
    throw "Spark entrypoint not found: $entrypoint"
}

$sparkSubmit = if ($env:SPARK_HOME) {
    Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
} else {
    "spark-submit"
}

& $sparkSubmit --properties-file $propertiesFile $entrypoint --properties-file $propertiesFile @AppArgs
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

