param(
    [string]$BatchName = "demo_run"
)

Set-Location $PSScriptRoot

batch-ingest --input-dir data/raw --db-path data/warehouse/ingestion.db --log-dir data/logs --reset-db --batch-name $BatchName
