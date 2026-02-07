# PowerShell script to encode secrets for Kestra
# Reads .env file and creates .env_encoded with base64-encoded values

$envFile = Join-Path $PSScriptRoot ".env"
$encodedFile = Join-Path $PSScriptRoot ".env_encoded"

if (-not (Test-Path $envFile)) {
    Write-Error ".env file not found at $envFile"
    exit 1
}

# Clear the encoded file if it exists
if (Test-Path $encodedFile) {
    Remove-Item $encodedFile
}

# Read each line from .env
Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    
    # Skip empty lines and comments
    if ($line -and -not $line.StartsWith("#")) {
        # Split on first = only
        $parts = $line.Split('=', 2)
        
        if ($parts.Count -eq 2) {
            $key = $parts[0].Trim()
            $value = $parts[1].Trim()
            
            # Base64 encode the value
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($value)
            $encodedValue = [Convert]::ToBase64String($bytes)
            
            # Write to encoded file with SECRET_ prefix
            "SECRET_$key=$encodedValue" | Out-File -Append -FilePath $encodedFile -Encoding utf8 -NoNewline
            "`n" | Out-File -Append -FilePath $encodedFile -Encoding utf8 -NoNewline
        }
    }
}

Write-Host "Secrets encoded successfully!" -ForegroundColor Green
Write-Host "Created: $encodedFile" -ForegroundColor Cyan
