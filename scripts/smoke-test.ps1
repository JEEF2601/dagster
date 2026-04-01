param(
    [int]$TimeoutSeconds = 600,
    [int]$PollSeconds = 5
)

$ErrorActionPreference = "Stop"

function Get-LatestRunId {
    param(
        [string]$JobName
    )

    $query = "SELECT run_id FROM runs WHERE pipeline_name='$JobName' ORDER BY update_timestamp DESC LIMIT 1;"
    $runId = docker exec -i dagster-postgres-1 psql -U dagster -d dagster -t -A -c $query
    return $runId.Trim()
}

function Wait-RunCompletion {
    param(
        [string]$RunId,
        [int]$Timeout,
        [int]$Poll
    )

    $start = Get-Date

    while ($true) {
        $query = "SELECT status FROM runs WHERE run_id='$RunId';"
        $status = (docker exec -i dagster-postgres-1 psql -U dagster -d dagster -t -A -c $query).Trim()

        if ($status -eq "SUCCESS") {
            return $status
        }

        if ($status -eq "FAILURE" -or $status -eq "CANCELED") {
            return $status
        }

        $elapsed = (Get-Date) - $start
        if ($elapsed.TotalSeconds -ge $Timeout) {
            return "TIMEOUT"
        }

        Start-Sleep -Seconds $Poll
    }
}

Write-Host "Launching influx_r2_etl_job..."
docker compose exec -T dagster-webserver dagster job launch -m repo.repository -j influx_r2_etl_job | Out-Null
$influxRunId = Get-LatestRunId -JobName "influx_r2_etl_job"
Write-Host "influx run_id: $influxRunId"

Write-Host "Launching cryptocompare_r2_etl_job..."
docker compose exec -T dagster-webserver dagster job launch -m repo.repository -j cryptocompare_r2_etl_job | Out-Null
$cryptoRunId = Get-LatestRunId -JobName "cryptocompare_r2_etl_job"
Write-Host "cryptocompare run_id: $cryptoRunId"

Write-Host "Waiting for influx run completion..."
$influxStatus = Wait-RunCompletion -RunId $influxRunId -Timeout $TimeoutSeconds -Poll $PollSeconds
Write-Host "influx status: $influxStatus"

Write-Host "Waiting for cryptocompare run completion..."
$cryptoStatus = Wait-RunCompletion -RunId $cryptoRunId -Timeout $TimeoutSeconds -Poll $PollSeconds
Write-Host "cryptocompare status: $cryptoStatus"

if ($influxStatus -ne "SUCCESS" -or $cryptoStatus -ne "SUCCESS") {
    Write-Error "Smoke test failed. influx=$influxStatus cryptocompare=$cryptoStatus"
}

Write-Host "Smoke test passed."
