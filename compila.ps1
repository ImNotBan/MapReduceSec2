param([string]$JdkPath = "")
$ErrorActionPreference = 'Stop'
if (-not $JdkPath) {
    if (Test-Path -LiteralPath "$env:USERPROFILE\.jdks\openjdk-23.0.2\bin\javac.exe") {
        $JdkPath = "$env:USERPROFILE\.jdks\openjdk-23.0.2"
    } elseif ($env:JAVA_HOME) { $JdkPath = $env:JAVA_HOME }
    else { $JdkPath = Split-Path (Split-Path (Get-Command javac).Source) }
}
$classes = Join-Path $PSScriptRoot 'out\classes'
if (Test-Path -LiteralPath $classes) {
    $resolvedClasses = (Resolve-Path -LiteralPath $classes).Path
    $expectedClasses = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot 'out\classes'))
    if ($resolvedClasses -ne $expectedClasses) { throw 'Directori de compilacio inesperat.' }
    Remove-Item -LiteralPath $resolvedClasses -Recurse -Force
}
New-Item -ItemType Directory -Path $classes -Force | Out-Null
$sources = @(Get-ChildItem -LiteralPath (Join-Path $PSScriptRoot 'src') -Filter '*.java' | ForEach-Object FullName)
& "$JdkPath\bin\javac.exe" -encoding UTF-8 -d $classes @sources
if ($LASTEXITCODE -ne 0) { throw 'La compilacio ha fallat.' }
& "$JdkPath\bin\jar.exe" --create --file (Join-Path $PSScriptRoot 'out\WordCount.jar') --main-class WordCount -C $classes .
if ($LASTEXITCODE -ne 0) { throw 'No es pot crear el JAR.' }
Write-Host 'Compilacio correcta: out/classes i out/WordCount.jar'
