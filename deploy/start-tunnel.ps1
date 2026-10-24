$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
$connector = Join-Path $repo '.local/cloudflare/cloudflared.exe'
$tokenFile = Join-Path $repo '.local/cloudflare/token'
if (!(Test-Path -LiteralPath $connector) -or !(Test-Path -LiteralPath $tokenFile)) {
  throw 'Install the official cloudflared connector and provision its token in .local/cloudflare first.'
}
& $connector tunnel --no-autoupdate run --token-file $tokenFile
exit $LASTEXITCODE
