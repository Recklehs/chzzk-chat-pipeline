param(
    [string]$HostAddress,
    [int]$Port,
    [string[]]$UvicornArgs = @()
)

$root = Resolve-Path $PSScriptRoot
$venvPython = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path (Join-Path $root ".env")) -and -not $env:APP_ENV) {
    throw ".env 파일이 없고 APP_ENV 환경변수도 설정되지 않았습니다."
}

if (-not (Test-Path $venvPython)) {
    throw "Virtualenv Python not found: $venvPython"
}

$runtimeJson = & $venvPython -c "from collector.env_profiles import collect_runtime_environment; import json; print(json.dumps(collect_runtime_environment(), ensure_ascii=False))"
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$runtime = $runtimeJson | ConvertFrom-Json

if ($runtime.app_env -eq "test" -and $runtime.event_bus_backend -eq "pubsub") {
    Write-Host "[INFO] test 프로필 Pub/Sub raw topic 확인"
    & $venvPython -m collector.bootstrap_pubsub_raw_topic
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

$resolvedHost = if ($PSBoundParameters.ContainsKey("HostAddress")) { $HostAddress } else { $runtime.host }
$resolvedPort = if ($PSBoundParameters.ContainsKey("Port")) { $Port } else { [int]$runtime.port }

Write-Host "[INFO] 서버 시작: http://$resolvedHost`:$resolvedPort"
& $venvPython -m uvicorn chzzk_collector_server:app --host $resolvedHost --port $resolvedPort @UvicornArgs
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
