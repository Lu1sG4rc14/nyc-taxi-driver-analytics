param(
    [string]$ProjectId = "nyc-taxi-driver-analytics",
    [string]$Region = "us-central1",
    [string]$ImageTag = (Get-Date -Format "yyyyMMddHHmmss"),
    [switch]$LocalDocker
)

$ErrorActionPreference = "Stop"

$image = "$Region-docker.pkg.dev/$ProjectId/taxi-driver-analytics/ingest:$ImageTag"

if ($LocalDocker) {
    Write-Host "Configuring Docker auth for $Region-docker.pkg.dev"
    gcloud auth configure-docker "$Region-docker.pkg.dev" --quiet
    if ($LASTEXITCODE -ne 0) { throw "gcloud auth configure-docker failed" }

    Write-Host "Building $image with local Docker"
    docker build -f app/ingest/Dockerfile -t $image .
    if ($LASTEXITCODE -ne 0) { throw "docker build failed" }

    Write-Host "Pushing $image"
    docker push $image
    if ($LASTEXITCODE -ne 0) { throw "docker push failed" }
}
else {
    Write-Host "Building and pushing $image with Cloud Build"
    gcloud builds submit --project $ProjectId --config cloudbuild.yaml --substitutions "_IMAGE=$image" .
    if ($LASTEXITCODE -ne 0) { throw "gcloud builds submit failed" }
}

Write-Host ""
Write-Host "Image pushed:"
Write-Host $image
Write-Host ""
Write-Host "Apply it with:"
Write-Host "terraform -chdir=infra apply -var `"ingest_image=$image`""
