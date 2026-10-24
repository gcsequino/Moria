param(
  [string]$ManagementOrigin = 'https://moria.gsequino.dev',
  [string]$SnowOrigin = 'https://snow.gsequino.dev'
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $repo
$env:MORIA_PUBLIC_ORIGIN = $ManagementOrigin
$env:MORIA_SNOW_PUBLIC_ORIGIN = $SnowOrigin
# Keep the normal localhost listeners and recovery commands alongside HTTPS ingress.
& pnpm start
exit $LASTEXITCODE
