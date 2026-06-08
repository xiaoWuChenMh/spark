$root = "F:\code\workpace\study\intellij idea\source\spark\core\src\main\scala\org\apache\spark\util"
$skill = "C:\Users\Zz\.trae\skills\understand"
$batches = Get-Content "$root\.understand-anything\intermediate\batches.json" -Raw | ConvertFrom-Json
foreach ($b in $batches.batches) {
  $i = $b.batchIndex
  $inputPath = "$root\.understand-anything\tmp\ua-file-analyzer-input-$i.json"
  $outputPath = "$root\.understand-anything\tmp\ua-file-extract-results-$i.json"
  $batchFiles = @()
  foreach ($f in $b.files) {
    $entry = @{ path = $f.path; language = $f.language; sizeLines = [int]$f.sizeLines; fileCategory = $f.fileCategory }
    $batchFiles += $entry
  }
  $inputData = @{ projectRoot = $root; batchFiles = $batchFiles; batchImportData = $b.batchImportData }
  $inputData | ConvertTo-Json -Depth 10 -Compress | Set-Content $inputPath
  Write-Host "Batch $i`: $($b.files.Count) files"
  node "$skill\extract-structure.mjs" $inputPath $outputPath 2>&1 | Out-Null
  if (Test-Path $outputPath) { Write-Host "  -> done" } else { Write-Host "  -> FAILED" }
}
