param([string]$JdkPath = "")
$ErrorActionPreference = 'Stop'
if (-not $JdkPath) {
    if (Test-Path -LiteralPath "$env:USERPROFILE\.jdks\openjdk-23.0.2\bin\java.exe") {
        $JdkPath = "$env:USERPROFILE\.jdks\openjdk-23.0.2"
    } elseif ($env:JAVA_HOME) { $JdkPath = $env:JAVA_HOME }
    else { $JdkPath = Split-Path (Split-Path (Get-Command java).Source) }
}
& (Join-Path $PSScriptRoot 'compila.ps1') -JdkPath $JdkPath
$classes = Join-Path $PSScriptRoot 'out\classes'
& "$JdkPath\bin\javac.exe" -encoding UTF-8 -cp $classes -d $classes (Join-Path $PSScriptRoot 'tests\ComprovaPractica.java')
if ($LASTEXITCODE -ne 0) { throw 'No es poden compilar les proves.' }
& "$JdkPath\bin\java.exe" -cp $classes ComprovaPractica
if ($LASTEXITCODE -ne 0) { throw 'Hi ha proves que fallen.' }
