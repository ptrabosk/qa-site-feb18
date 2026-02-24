Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

[System.Windows.Forms.Application]::EnableVisualStyles()

function Get-JsonObject {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return @{}
    }

    $raw = Get-Content -LiteralPath $Path -Raw -ErrorAction Stop
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @{}
    }
    return $raw | ConvertFrom-Json
}

function Write-JsonObject {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        $Value
    )

    $json = $Value | ConvertTo-Json -Depth 100 -Compress
    $pretty = Format-JsonPretty -Json $json -IndentSize 2
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $pretty, $utf8NoBom)
}

function Format-JsonPretty {
    param(
        [Parameter(Mandatory = $true)][string]$Json,
        [int]$IndentSize = 2
    )

    if ([string]::IsNullOrWhiteSpace($Json)) { return $Json }

    $sb = New-Object System.Text.StringBuilder
    $indent = 0
    $inString = $false
    $escape = $false

    foreach ($ch in $Json.ToCharArray()) {
        if ($inString) {
            [void]$sb.Append($ch)
            if ($escape) {
                $escape = $false
            } elseif ($ch -eq '\') {
                $escape = $true
            } elseif ($ch -eq '"') {
                $inString = $false
            }
            continue
        }

        switch ($ch) {
            '"' {
                $inString = $true
                [void]$sb.Append($ch)
            }
            '{' {
                [void]$sb.Append($ch)
                [void]$sb.AppendLine()
                $indent++
                [void]$sb.Append((' ' * ($indent * $IndentSize)))
            }
            '[' {
                [void]$sb.Append($ch)
                [void]$sb.AppendLine()
                $indent++
                [void]$sb.Append((' ' * ($indent * $IndentSize)))
            }
            '}' {
                [void]$sb.AppendLine()
                $indent = [Math]::Max(0, $indent - 1)
                [void]$sb.Append((' ' * ($indent * $IndentSize)))
                [void]$sb.Append($ch)
            }
            ']' {
                [void]$sb.AppendLine()
                $indent = [Math]::Max(0, $indent - 1)
                [void]$sb.Append((' ' * ($indent * $IndentSize)))
                [void]$sb.Append($ch)
            }
            ',' {
                [void]$sb.Append($ch)
                [void]$sb.AppendLine()
                [void]$sb.Append((' ' * ($indent * $IndentSize)))
            }
            ':' {
                [void]$sb.Append(": ")
            }
            default {
                if (-not [char]::IsWhiteSpace($ch)) {
                    [void]$sb.Append($ch)
                }
            }
        }
    }

    return $sb.ToString()
}

function Get-ScenarioCount {
    param([Parameter(Mandatory = $true)]$Json)

    if ($null -eq $Json) { return 0 }

    if ($Json -is [System.Array]) {
        if ($Json.Count -eq 0) { return 0 }
        $allMessageLike = $true
        foreach ($item in $Json) {
            if ($null -eq $item -or -not ($item.PSObject.Properties.Name -contains 'message_text' -or $item.PSObject.Properties.Name -contains 'message_type' -or $item.PSObject.Properties.Name -contains 'content' -or $item.PSObject.Properties.Name -contains 'role')) {
                $allMessageLike = $false
                break
            }
        }
        if ($allMessageLike) { return 1 }
        return $Json.Count
    }

    if ($Json.PSObject.Properties.Name -contains 'scenarios') {
        $scenarios = $Json.scenarios
        if ($scenarios -is [System.Array]) {
            if ($scenarios.Count -eq 0) { return 0 }
            $allMessageLike = $true
            foreach ($item in $scenarios) {
                if ($null -eq $item -or -not ($item.PSObject.Properties.Name -contains 'message_text' -or $item.PSObject.Properties.Name -contains 'message_type' -or $item.PSObject.Properties.Name -contains 'content' -or $item.PSObject.Properties.Name -contains 'role')) {
                    $allMessageLike = $false
                    break
                }
            }
            if ($allMessageLike) { return 1 }
            return $scenarios.Count
        }

        if ($scenarios -and -not ($scenarios -is [string])) {
            return @($scenarios.PSObject.Properties).Count
        }
    }

    return 0
}

function Get-TemplateCount {
    param([Parameter(Mandatory = $true)]$Json)

    if ($null -eq $Json) { return 0 }
    if ($Json -is [System.Array]) { return $Json.Count }
    if ($Json.PSObject.Properties.Name -contains 'templates' -and $Json.templates -is [System.Array]) {
        return $Json.templates.Count
    }
    return 0
}

$script:ScriptFolder = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:CurrentFolder = $script:ScriptFolder
$candidateRoots = @(
    $script:ScriptFolder,
    (Split-Path -Parent $script:ScriptFolder),
    (Split-Path -Parent (Split-Path -Parent $script:ScriptFolder))
) | Select-Object -Unique
foreach ($candidate in $candidateRoots) {
    if ((Test-Path -LiteralPath (Join-Path $candidate "scenarios.json")) -and (Test-Path -LiteralPath (Join-Path $candidate "templates.json"))) {
        $script:CurrentFolder = $candidate
        break
    }
}

$form = New-Object System.Windows.Forms.Form
$form.Text = "Scenario & Template Manager"
$form.StartPosition = "CenterScreen"
$form.Size = New-Object System.Drawing.Size(760, 420)
$form.MinimumSize = New-Object System.Drawing.Size(740, 400)
$form.BackColor = [System.Drawing.Color]::FromArgb(245, 247, 252)
$form.Font = New-Object System.Drawing.Font("Segoe UI", 9)

$title = New-Object System.Windows.Forms.Label
$title.Text = "Scenario & Template Manager"
$title.Font = New-Object System.Drawing.Font("Segoe UI Semibold", 14)
$title.AutoSize = $true
$title.Location = New-Object System.Drawing.Point(18, 16)
$form.Controls.Add($title)

$folderLabel = New-Object System.Windows.Forms.Label
$folderLabel.AutoSize = $false
$folderLabel.Size = New-Object System.Drawing.Size(700, 20)
$folderLabel.Location = New-Object System.Drawing.Point(20, 52)
$folderLabel.Text = "Folder: $script:CurrentFolder"
$form.Controls.Add($folderLabel)

$chooseFolderBtn = New-Object System.Windows.Forms.Button
$chooseFolderBtn.Text = "Choose Folder"
$chooseFolderBtn.Size = New-Object System.Drawing.Size(120, 34)
$chooseFolderBtn.Location = New-Object System.Drawing.Point(596, 16)
$form.Controls.Add($chooseFolderBtn)

$scenariosGroup = New-Object System.Windows.Forms.GroupBox
$scenariosGroup.Text = "Scenarios"
$scenariosGroup.Location = New-Object System.Drawing.Point(20, 86)
$scenariosGroup.Size = New-Object System.Drawing.Size(340, 130)
$form.Controls.Add($scenariosGroup)

$scenariosMeta = New-Object System.Windows.Forms.Label
$scenariosMeta.AutoSize = $true
$scenariosMeta.Location = New-Object System.Drawing.Point(12, 28)
$scenariosMeta.Text = "Items: 0"
$scenariosGroup.Controls.Add($scenariosMeta)

$uploadScenariosBtn = New-Object System.Windows.Forms.Button
$uploadScenariosBtn.Text = "Upload JSON / CSV"
$uploadScenariosBtn.Size = New-Object System.Drawing.Size(145, 34)
$uploadScenariosBtn.Location = New-Object System.Drawing.Point(12, 58)
$scenariosGroup.Controls.Add($uploadScenariosBtn)

$clearScenariosBtn = New-Object System.Windows.Forms.Button
$clearScenariosBtn.Text = "Clear Scenarios"
$clearScenariosBtn.Size = New-Object System.Drawing.Size(145, 34)
$clearScenariosBtn.Location = New-Object System.Drawing.Point(170, 58)
$scenariosGroup.Controls.Add($clearScenariosBtn)

$templatesGroup = New-Object System.Windows.Forms.GroupBox
$templatesGroup.Text = "Templates"
$templatesGroup.Location = New-Object System.Drawing.Point(380, 86)
$templatesGroup.Size = New-Object System.Drawing.Size(340, 130)
$form.Controls.Add($templatesGroup)

$templatesMeta = New-Object System.Windows.Forms.Label
$templatesMeta.AutoSize = $true
$templatesMeta.Location = New-Object System.Drawing.Point(12, 28)
$templatesMeta.Text = "Items: 0"
$templatesGroup.Controls.Add($templatesMeta)

$uploadTemplatesBtn = New-Object System.Windows.Forms.Button
$uploadTemplatesBtn.Text = "Upload JSON"
$uploadTemplatesBtn.Size = New-Object System.Drawing.Size(145, 34)
$uploadTemplatesBtn.Location = New-Object System.Drawing.Point(12, 58)
$templatesGroup.Controls.Add($uploadTemplatesBtn)

$clearTemplatesBtn = New-Object System.Windows.Forms.Button
$clearTemplatesBtn.Text = "Clear Templates"
$clearTemplatesBtn.Size = New-Object System.Drawing.Size(145, 34)
$clearTemplatesBtn.Location = New-Object System.Drawing.Point(170, 58)
$templatesGroup.Controls.Add($clearTemplatesBtn)

$openFolderBtn = New-Object System.Windows.Forms.Button
$openFolderBtn.Text = "Open Current Folder"
$openFolderBtn.Size = New-Object System.Drawing.Size(170, 34)
$openFolderBtn.Location = New-Object System.Drawing.Point(20, 228)
$form.Controls.Add($openFolderBtn)

$statusGroup = New-Object System.Windows.Forms.GroupBox
$statusGroup.Text = "Status"
$statusGroup.Location = New-Object System.Drawing.Point(20, 270)
$statusGroup.Size = New-Object System.Drawing.Size(700, 96)
$form.Controls.Add($statusGroup)

$statusText = New-Object System.Windows.Forms.TextBox
$statusText.Multiline = $true
$statusText.ReadOnly = $true
$statusText.BorderStyle = "FixedSingle"
$statusText.BackColor = [System.Drawing.Color]::White
$statusText.Size = New-Object System.Drawing.Size(676, 62)
$statusText.Location = New-Object System.Drawing.Point(12, 22)
$statusText.Text = "Ready."
$statusGroup.Controls.Add($statusText)

function Set-Status {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [bool]$IsError = $false
    )

    $statusText.Text = $Message
    if ($IsError) {
        $statusText.ForeColor = [System.Drawing.Color]::FromArgb(176, 35, 24)
    } else {
        $statusText.ForeColor = [System.Drawing.Color]::FromArgb(51, 71, 107)
    }
}

function Get-ScenariosPath {
    return Join-Path $script:CurrentFolder "scenarios.json"
}

function Get-TemplatesPath {
    return Join-Path $script:CurrentFolder "templates.json"
}

function Refresh-Meta {
    try {
        $scenariosJson = Get-JsonObject -Path (Get-ScenariosPath)
        $templatesJson = Get-JsonObject -Path (Get-TemplatesPath)
        $scenariosMeta.Text = "Items: $(Get-ScenarioCount -Json $scenariosJson)"
        $templatesMeta.Text = "Items: $(Get-TemplateCount -Json $templatesJson)"
    } catch {
        Set-Status -Message ("Failed to read JSON files: " + $_.Exception.Message) -IsError $true
    }
}

function Convert-ScenarioContainerToList {
    param($Container)

    if ($null -eq $Container) { return @() }

    if ($Container -is [System.Array]) {
        return @($Container)
    }

    if ($Container.PSObject.Properties.Name -contains 'scenarios') {
        $sc = $Container.scenarios
        if ($sc -is [System.Array]) {
            return @($sc)
        }
        if ($sc -and -not ($sc -is [string])) {
            $items = @()
            foreach ($p in $sc.PSObject.Properties) {
                $items += $p.Value
            }
            return $items
        }
    }

    return @()
}

function Merge-ScenariosById {
    param(
        [array]$Existing = @(),
        [array]$Incoming = @()
    )

    if ($null -eq $Existing) { $Existing = @() }
    if ($null -eq $Incoming) { $Incoming = @() }

    $result = @()
    foreach ($item in $Existing) { $result += (Normalize-ScenarioRecordForStorage -Scenario $item) }

    $idToIndex = @{}
    for ($i = 0; $i -lt $result.Count; $i++) {
        $id = (Get-StringValue $result[$i].id).Trim()
        if ($id -and -not $idToIndex.ContainsKey($id)) {
            $idToIndex[$id] = $i
        }
    }

    function Convert-ObjectToHashtable {
        param($InputObject)

        $map = @{}
        if ($null -eq $InputObject) { return $map }

        if ($InputObject -is [System.Collections.IDictionary]) {
            foreach ($key in $InputObject.Keys) {
                $map[[string]$key] = $InputObject[$key]
            }
            return $map
        }

        if ($InputObject.PSObject -and $InputObject.PSObject.Properties) {
            foreach ($p in $InputObject.PSObject.Properties) {
                $map[$p.Name] = $p.Value
            }
        }

        return $map
    }

    function Merge-Hashtable {
        param($Base, $Incoming)

        $out = @{}
        $baseMap = Convert-ObjectToHashtable -InputObject $Base
        foreach ($k in $baseMap.Keys) {
            $out[$k] = $baseMap[$k]
        }
        $incomingMap = Convert-ObjectToHashtable -InputObject $Incoming
        foreach ($k in $incomingMap.Keys) {
            $out[$k] = $incomingMap[$k]
        }
        return $out
    }

    function Merge-ScenarioRecord {
        param($ExistingScenario, $IncomingScenario)

        $baseNorm = Normalize-ScenarioRecordForStorage -Scenario $ExistingScenario
        $incomingNorm = Normalize-ScenarioRecordForStorage -Scenario $IncomingScenario
        $merged = Merge-Hashtable -Base $baseNorm -Incoming $incomingNorm

        $existingRightPanel = if ($baseNorm) { $baseNorm.rightPanel } else { $null }
        $incomingRightPanel = if ($incomingNorm) { $incomingNorm.rightPanel } else { $null }
        if ($existingRightPanel -or $incomingRightPanel) {
            $merged.rightPanel = Merge-Hashtable -Base $existingRightPanel -Incoming $incomingRightPanel
        }

        return (Normalize-ScenarioRecordForStorage -Scenario $merged)
    }

    $updated = 0
    $added = 0
    foreach ($item in $Incoming) {
        $itemNorm = Normalize-ScenarioRecordForStorage -Scenario $item
        $incomingId = (Get-StringValue $itemNorm.id).Trim()
        if ($incomingId -and $idToIndex.ContainsKey($incomingId)) {
            $targetIndex = [int]$idToIndex[$incomingId]
            $result[$targetIndex] = Merge-ScenarioRecord -ExistingScenario $result[$targetIndex] -IncomingScenario $itemNorm
            $updated++
            continue
        }

        $result += $itemNorm
        $added++
        if ($incomingId) {
            $idToIndex[$incomingId] = $result.Count - 1
        }
    }

    return @{
        scenarios = $result
        updated   = $updated
        added     = $added
    }
}

function Get-StringValue {
    param($Value)
    if ($null -eq $Value) { return "" }
    $text = [string]$Value
    # Normalize styled unicode glyphs (e.g., mathematical bold letters) to plain text.
    $text = $text.Normalize([Text.NormalizationForm]::FormKC)
    return $text
}

function Has-StyledMathChars {
    param([string]$Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return $false }
    return [regex]::IsMatch($Text, "\uD835[\uDC00-\uDFFF]")
}

function Parse-JsonText {
    param([string]$Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return $null }
    try {
        return ($Text | ConvertFrom-Json)
    } catch {
        return $null
    }
}

function Convert-ToStringArray {
    param($Value)

    $out = @()
    if ($null -eq $Value) { return ,$out }

    if ($Value -is [System.Array]) {
        foreach ($item in $Value) {
            $txt = (Get-StringValue $item).Trim()
            if ($txt -and $txt -ne "{}" -and $txt -ne "[]") { $out += $txt }
        }
        return ,$out
    }

    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($entry in $Value.GetEnumerator()) {
            $txt = (Get-StringValue $entry.Value).Trim()
            if ($txt -and $txt -ne "{}" -and $txt -ne "[]") { $out += $txt }
        }
        return ,$out
    }

    if ($Value.PSObject -and $Value.PSObject.Properties.Count -gt 0 -and -not ($Value -is [string])) {
        foreach ($p in $Value.PSObject.Properties) {
            $txt = (Get-StringValue $p.Value).Trim()
            if ($txt -and $txt -ne "{}" -and $txt -ne "[]") { $out += $txt }
        }
        return ,$out
    }

    $single = (Get-StringValue $Value).Trim()
    if ($single -and $single -ne "{}" -and $single -ne "[]") { $out += $single }
    return ,$out
}

function Get-UniqueTrimmedStringArray {
    param($Value)

    $seen = @{}
    $result = @()
    foreach ($item in (Convert-ToStringArray -Value $Value)) {
        $txt = (Get-StringValue $item).Trim()
        if (-not $txt) { continue }
        $key = $txt.ToLowerInvariant()
        if ($seen.ContainsKey($key)) { continue }
        $seen[$key] = $true
        $result += $txt
    }
    return ,$result
}

function Parse-ListLikeText {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) { return ,@() }
    $trimmed = $Text.Trim()
    if ($trimmed -eq "[]") { return ,@() }

    $jsonParsed = Parse-JsonText -Text $trimmed
    if ($jsonParsed -is [System.Array]) {
        return (Convert-ToStringArray -Value $jsonParsed)
    }
    if ($jsonParsed -isnot [System.Array] -and $null -ne $jsonParsed) {
        return (Convert-ToStringArray -Value $jsonParsed)
    }

    $matches = [regex]::Matches($trimmed, "'([^']*)'|`"([^`"]*)`"")
    if ($matches.Count -gt 0) {
        $arr = @()
        foreach ($m in $matches) {
            $value = if ($m.Groups[1].Success) { $m.Groups[1].Value } else { $m.Groups[2].Value }
            if (-not [string]::IsNullOrWhiteSpace($value)) { $arr += $value.Trim() }
        }
        return $arr
    }

    $fallback = $trimmed.Trim('[', ']')
    if ([string]::IsNullOrWhiteSpace($fallback)) { return ,@() }
    return (Convert-ToStringArray -Value ($fallback -split "[,`n`r]+" | ForEach-Object { $_.Trim(" `"`'") } | Where-Object { $_ }))
}

function Normalize-ScenarioNotes {
    param($NotesValue)

    $notesOut = @{}
    $keyOrder = @()
    if ($null -eq $NotesValue) { return [pscustomobject]@{} }

    $sourceEntries = @()
    if ($NotesValue -is [System.Collections.IDictionary]) {
        foreach ($entry in $NotesValue.GetEnumerator()) {
            $sourceEntries += @{
                key   = [string]$entry.Key
                value = $entry.Value
            }
        }
    } elseif ($NotesValue.PSObject -and $NotesValue.PSObject.Properties.Count -gt 0) {
        foreach ($prop in $NotesValue.PSObject.Properties) {
            $sourceEntries += @{
                key   = [string]$prop.Name
                value = $prop.Value
            }
        }
    } else {
        return [pscustomobject]@{}
    }

    foreach ($entry in $sourceEntries) {
        $rawKey = (Get-StringValue $entry.key).Trim()
        $key = Normalize-GuidelineCategoryKey -Heading $rawKey
        if (-not $notesOut.Contains($key)) {
            $notesOut[$key] = @()
            $keyOrder += $key
        }

        $items = Convert-ToStringArray -Value $entry.value
        foreach ($item in $items) {
            $txt = (Get-StringValue $item).Trim()
            if (-not $txt) { continue }

            $headingMatch = [regex]::Match($txt, '^\*{0,2}\s*#\s*(.+)$')
            if ($headingMatch.Success) {
                $movedKey = Normalize-GuidelineCategoryKey -Heading $headingMatch.Groups[1].Value
                if (-not $notesOut.Contains($movedKey)) {
                    $notesOut[$movedKey] = @()
                    $keyOrder += $movedKey
                }
                continue
            }

            $notesOut[$key] += $txt
        }
    }

    # If important contains SEND TO CS markers, move those lines.
    if ($notesOut.Contains('important')) {
        $keep = @()
        foreach ($item in $notesOut['important']) {
            $txt = (Get-StringValue $item).Trim()
            if ($txt -match 'send\s*to\s*cs|cssupport@|post-purchase|shipping inquiries on a current order') {
                if (-not $notesOut.Contains('send_to_cs')) {
                    $notesOut['send_to_cs'] = @()
                    $keyOrder += 'send_to_cs'
                }
                $notesOut['send_to_cs'] += $txt
                continue
            }
            if ($txt -eq '**') { continue }
            $keep += $txt
        }
        $notesOut['important'] = $keep
    }

    $clean = [ordered]@{}
    foreach ($k in $keyOrder) {
        if (-not $notesOut.Contains($k)) { continue }
        $arr = Get-UniqueTrimmedStringArray -Value $notesOut[$k]
        if ($arr.Count -gt 0) { $clean[$k] = $arr }
    }
    return [pscustomobject]$clean
}

function Normalize-ScenarioRecordForStorage {
    param($Scenario)

    $out = @{}
    if ($null -eq $Scenario) { return $out }
    if ($Scenario -is [System.Collections.IDictionary]) {
        foreach ($key in $Scenario.Keys) {
            $out[[string]$key] = $Scenario[$key]
        }
    } elseif ($Scenario.PSObject -and $Scenario.PSObject.Properties) {
        foreach ($p in $Scenario.PSObject.Properties) {
            $out[$p.Name] = $p.Value
        }
    }

    $rightPanel = @{}
    if ($out.Contains('rightPanel') -and $out.rightPanel) {
        if ($out.rightPanel -is [System.Collections.IDictionary]) {
            foreach ($key in $out.rightPanel.Keys) {
                $rightPanel[[string]$key] = $out.rightPanel[$key]
            }
        } elseif ($out.rightPanel.PSObject) {
            foreach ($p in $out.rightPanel.PSObject.Properties) {
                $rightPanel[$p.Name] = $p.Value
            }
        }
    }

    if ($out.Contains('source') -and -not $rightPanel.ContainsKey('source')) {
        $rightPanel['source'] = $out['source']
        $out.Remove('source')
    }
    if ($out.Contains('browsingHistory') -and -not $rightPanel.ContainsKey('browsingHistory')) {
        $rightPanel['browsingHistory'] = $out['browsingHistory']
        $out.Remove('browsingHistory')
    }
    if ($out.Contains('browsing_history') -and -not $rightPanel.ContainsKey('browsingHistory')) {
        $rightPanel['browsingHistory'] = $out['browsing_history']
        $out.Remove('browsing_history')
    }
    if ($out.Contains('orders') -and -not $rightPanel.ContainsKey('orders')) {
        $rightPanel['orders'] = $out['orders']
        $out.Remove('orders')
    }
    if ($out.Contains('templatesUsed') -and -not $rightPanel.ContainsKey('templates')) {
        $rightPanel['templates'] = $out['templatesUsed']
        $out.Remove('templatesUsed')
    }

    if ($rightPanel.Count -gt 0) {
        $out['rightPanel'] = $rightPanel
    }

    $blocklistedSource = @()
    if ($out.Contains('blocklisted_words')) {
        $blocklistedSource = $out.blocklisted_words
    } elseif ($out.Contains('blocklistedWords')) {
        $blocklistedSource = $out.blocklistedWords
    }
    $out['blocklisted_words'] = Get-UniqueTrimmedStringArray -Value $blocklistedSource
    if ($out.Contains('blocklistedWords')) { $out.Remove('blocklistedWords') }

    $escalationSource = @()
    if ($out.Contains('escalation_preferences')) {
        $escalationSource = $out.escalation_preferences
    } elseif ($out.Contains('escalationPreferences')) {
        $escalationSource = $out.escalationPreferences
    }
    $out['escalation_preferences'] = Get-UniqueTrimmedStringArray -Value $escalationSource
    if ($out.Contains('escalationPreferences')) { $out.Remove('escalationPreferences') }

    $notesValue = $null
    if ($out.Contains('notes')) { $notesValue = $out['notes'] }
    elseif ($out.Contains('guidelines')) { $notesValue = $out['guidelines'] }
    $out['notes'] = Normalize-ScenarioNotes -NotesValue $notesValue
    if ($out.Contains('guidelines')) { $out.Remove('guidelines') }

    return $out
}

function Normalize-MessageMedia {
    param($Media)

    function Clean-MediaUrl {
        param([string]$UrlText)

        $url = (Get-StringValue $UrlText).Trim()
        if (-not $url) { return "" }
        $url = $url -replace '\\\"', '"'
        $url = $url -replace "\\\\'", "'"
        if (
            ($url.Length -ge 2) -and
            (
                ($url.StartsWith('"') -and $url.EndsWith('"')) -or
                ($url.StartsWith("'") -and $url.EndsWith("'"))
            )
        ) {
            $url = $url.Substring(1, $url.Length - 2).Trim()
        }
        $url = [regex]::Replace($url, '(?i);(name|filename|type)\s*=\s*%22([^%]*)%22', ';$1="$2"')
        if ($url -match "(?i)(smil(\.xml)?|application/smil)") { return "" }
        if ($url -match ';(?:name|filename|type)\s*=\s*"[^"]*$') {
            $url = "$url`""
        }
        if ($url -match "(?i)(smil(\.xml)?|application/smil)") { return "" }
        return $url.Trim()
    }

    function Extract-MediaUrls {
        param([string]$Text)

        $inputText = (Get-StringValue $Text).Trim()
        if (-not $inputText) { return @() }

        $urls = @()
        $jsonParsed = Parse-JsonText -Text $inputText
        if ($jsonParsed -is [System.Array]) {
            foreach ($item in $jsonParsed) {
                $clean = Clean-MediaUrl -UrlText $item
                if ($clean) { $urls += $clean }
            }
            if ($urls.Count -gt 0) { return $urls }
        }

        $searchText = $inputText -replace '\\\"', '"'
        $searchText = $searchText -replace "\\\\'", "'"
        $matches = [regex]::Matches(
            $searchText,
            "https?://[^\s;`"`'<>]+(?:\s*;\s*[a-z0-9_-]+\s*=\s*(?:""[^""]*""|'[^']*'|[^;\s`"`'<>]+))*",
            [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
        )
        foreach ($m in $matches) {
            $clean = Clean-MediaUrl -UrlText $m.Value
            if ($clean) { $urls += $clean }
        }
        return $urls
    }

    $result = @()
    if ($null -eq $Media) { return ,$result }

    $mediaItems = @()
    if ($Media -is [System.Array]) {
        $mediaItems = $Media
    } else {
        $mediaItems = @($Media)
    }

    foreach ($item in $mediaItems) {
        $text = (Get-StringValue $item).Trim()
        if (-not $text) { continue }
        $extracted = Extract-MediaUrls -Text $text
        if ($extracted.Count -gt 0) {
            $result += $extracted
            continue
        }
        $cleaned = Clean-MediaUrl -UrlText $text
        if ($cleaned) { $result += $cleaned }
    }
    return ,$result
}

function Normalize-GuidelineCategoryKey {
    param([string]$Heading)

    if ([string]::IsNullOrWhiteSpace($Heading)) { return "important" }
    $h = $Heading.Trim().ToLower()
    $h = [regex]::Replace($h, "^[^a-z0-9]+", "")
    $h = $h -replace "&", "and"
    $h = [regex]::Replace($h, "[^a-z0-9]+", "_").Trim("_")

    if ($h -match "send.*cs") { return "send_to_cs" }
    if ($h -match "^escalate$|^escalation$|escalat") { return "escalate" }
    if ($h -match "^tone$") { return "tone" }
    if ($h -match "template") { return "templates" }
    if ($h -match "do.*and.*don|dos_and_donts|don_ts|donts") { return "dos_and_donts" }
    if ($h -match "drive.*purchase") { return "drive_to_purchase" }
    if ($h -match "promo") { return "promo_and_exclusions" }
    if (-not $h) { return "important" }
    return $h
}

function Parse-CompanyNotesToCategories {
    param([string]$NotesText)

    $notes = [ordered]@{}
    if ([string]::IsNullOrWhiteSpace($NotesText)) { return [pscustomobject]@{} }

    $lines = $NotesText -split "`r?`n"
    $currentKey = "important"
    $notes[$currentKey] = @()

    foreach ($rawLine in $lines) {
        $line = ($rawLine | ForEach-Object { "$_" }).Trim()
        if (-not $line) { continue }

        if ($line.StartsWith("#")) {
            $heading = $line.TrimStart("#").Trim()
            $currentKey = Normalize-GuidelineCategoryKey -Heading $heading
            if (-not $notes.Contains($currentKey)) {
                $notes[$currentKey] = @()
            }
            continue
        }

        $itemRaw = $line
        if ($itemRaw.StartsWith("•")) { $itemRaw = $itemRaw.Substring(1).Trim() }
        if ($itemRaw.StartsWith("-")) { $itemRaw = $itemRaw.Substring(1).Trim() }
        if (-not $itemRaw) { continue }
        $item = (Get-StringValue $itemRaw).Trim()
        if (Has-StyledMathChars -Text $itemRaw) {
            $item = "**$item**"
        }
        $notes[$currentKey] += $item
    }

    $clean = [ordered]@{}
    foreach ($k in $notes.Keys) {
        $arr = @($notes[$k] | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($arr.Count -gt 0) { $clean[$k] = $arr }
    }
    return [pscustomobject]$clean
}

function Normalize-ColumnKey {
    param([string]$Name)

    $text = (Get-StringValue $Name).Trim().ToLowerInvariant()
    if (-not $text) { return "" }
    return [regex]::Replace($text, "[^a-z0-9]", "")
}

function Get-RowValue {
    param(
        [Parameter(Mandatory = $true)]$Row,
        [Parameter(Mandatory = $true)][string[]]$Candidates
    )

    if ($null -eq $Row) { return "" }
    $properties = @($Row.PSObject.Properties)
    if ($properties.Count -eq 0) { return "" }

    foreach ($candidate in $Candidates) {
        if ([string]::IsNullOrWhiteSpace($candidate)) { continue }
        $direct = $properties | Where-Object { $_.Name -eq $candidate } | Select-Object -First 1
        if ($direct) {
            $directValue = Get-StringValue $direct.Value
            if (-not [string]::IsNullOrWhiteSpace($directValue)) { return $directValue }
        }
    }

    foreach ($candidate in $Candidates) {
        $candidateKey = Normalize-ColumnKey -Name $candidate
        if (-not $candidateKey) { continue }
        foreach ($prop in $properties) {
            if ((Normalize-ColumnKey -Name $prop.Name) -eq $candidateKey) {
                $value = Get-StringValue $prop.Value
                if (-not [string]::IsNullOrWhiteSpace($value)) { return $value }
            }
        }
    }

    return ""
}

function Convert-JsonParsedToArray {
    param($Value)

    if ($null -eq $Value) { return @() }
    if ($Value -is [System.Array]) { return @($Value) }
    if ($Value -is [System.Collections.IDictionary]) { return @($Value) }
    if ($Value.PSObject -and $Value.PSObject.Properties.Count -gt 0) { return @($Value) }
    return @()
}

function Normalize-ConversationMessageType {
    param([string]$TypeRaw)

    $value = (Get-StringValue $TypeRaw).Trim().ToLowerInvariant()
    if (-not $value) { return "customer" }
    if ($value -in @("agent", "assistant", "support", "csr", "rep")) { return "agent" }
    if ($value -in @("system", "automation", "bot")) { return "system" }
    if ($value -in @("inbound", "incoming", "customer", "user", "client")) { return "customer" }
    if ($value -in @("outbound", "outgoing")) { return "agent" }
    return $value
}

function Convert-CsvRowToScenario {
    param([Parameter(Mandatory = $true)]$Row)

    $conversation = @()
    $conversationRaw = Get-RowValue -Row $Row -Candidates @("CONVERSATION_JSON", "CONVERSATION", "MESSAGES_JSON", "MESSAGES")
    $conversationParsed = Parse-JsonText -Text $conversationRaw
    $conversationItems = Convert-JsonParsedToArray -Value $conversationParsed
    if ($conversationItems.Count -gt 0) {
        foreach ($msg in $conversationItems) {
            if ($null -eq $msg) { continue }
            $typeRaw = Get-StringValue (@(
                $msg.message_type,
                $msg.type,
                $msg.role,
                $msg.direction,
                $msg.sender,
                $msg.speaker |
                Where-Object { $null -ne $_ } |
                Select-Object -First 1
            ))
            $agentId = (Get-StringValue (@(
                $msg.agent,
                $msg.agent_id,
                $msg.agentId,
                $msg.agentID |
                Where-Object { $null -ne $_ } |
                Select-Object -First 1
            ))).Trim()
            $normalizedType = Normalize-ConversationMessageType -TypeRaw $typeRaw
            if (-not (Get-StringValue $typeRaw).Trim() -and $agentId) {
                $normalizedType = "agent"
            }
            $entry = @{
                message_media = Normalize-MessageMedia -Media @($msg.message_media, $msg.media, $msg.attachments | Where-Object { $null -ne $_ } | Select-Object -First 1)
                message_text  = Get-StringValue (@($msg.message_text, $msg.text, $msg.content, $msg.body | Where-Object { $null -ne $_ } | Select-Object -First 1))
                message_type  = $normalizedType
            }
            if ($agentId) { $entry.agent = $agentId }
            $messageId = (Get-StringValue (@($msg.message_id, $msg.id | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
            if ($messageId) { $entry.message_id = $messageId }
            $dateTime = (Get-StringValue (@($msg.date_time, $msg.created_at, $msg.timestamp | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
            if ($dateTime) { $entry.date_time = $dateTime }
            if (-not [string]::IsNullOrWhiteSpace((Get-StringValue $entry.message_text))) {
                $conversation += $entry
            }
        }
    }

    $browsingHistory = @()
    $productsRaw = Get-RowValue -Row $Row -Candidates @("LAST_5_PRODUCTS", "LAST5_PRODUCTS", "BROWSING_HISTORY", "RECENT_PRODUCTS")
    $productsParsed = Parse-JsonText -Text $productsRaw
    $productItems = Convert-JsonParsedToArray -Value $productsParsed
    if ($productItems.Count -gt 0) {
        foreach ($p in $productItems) {
            if ($null -eq $p) { continue }
            $name = (Get-StringValue (@($p.product_name, $p.name, $p.product, $p.title | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
            $link = (Get-StringValue (@($p.product_link, $p.link, $p.url | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
            $viewDate = (Get-StringValue (@($p.view_date, $p.last_viewed, $p.time_ago, $p.viewed_at | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
            if (-not $name -and -not $link) { continue }
            $historyItem = @{ item = if ($name) { $name } else { $link } }
            if ($link) { $historyItem.link = $link }
            if ($viewDate) { $historyItem.timeAgo = $viewDate }
            $browsingHistory += $historyItem
        }
    }

    $ordersOut = @()
    $ordersRaw = Get-RowValue -Row $Row -Candidates @("ORDERS", "ORDER_HISTORY", "PAST_ORDERS")
    $ordersParsed = Parse-JsonText -Text $ordersRaw
    $orderItems = Convert-JsonParsedToArray -Value $ordersParsed
    if ($orderItems.Count -gt 0) {
        foreach ($order in $orderItems) {
            if ($null -eq $order) { continue }
            $itemsOut = @()
            $productsForOrder = Convert-JsonParsedToArray -Value (@($order.products, $order.items, $order.line_items | Where-Object { $null -ne $_ } | Select-Object -First 1))
            if ($productsForOrder.Count -gt 0) {
                foreach ($prod in $productsForOrder) {
                    if ($null -eq $prod) { continue }
                    $itemOut = @{
                        name = (Get-StringValue (@($prod.product_name, $prod.name, $prod.product, $prod.title | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
                    }
                    $priceValue = @($prod.product_price, $prod.price, $prod.unit_price | Where-Object { $null -ne $_ } | Select-Object -First 1)
                    if ($null -eq $priceValue) { $priceValue = $prod.price }
                    if ($null -ne $priceValue -and -not [string]::IsNullOrWhiteSpace((Get-StringValue $priceValue))) {
                        $itemOut.price = $priceValue
                    }
                    $prodLink = (Get-StringValue (@($prod.product_link, $prod.link, $prod.url | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
                    if ($prodLink) { $itemOut.productLink = $prodLink }
                    if (-not [string]::IsNullOrWhiteSpace((Get-StringValue $itemOut.name)) -or $itemOut.ContainsKey("price") -or $itemOut.ContainsKey("productLink")) {
                        $itemsOut += $itemOut
                    }
                }
            }

            $orderOut = @{
                orderNumber = (Get-StringValue (@($order.order_number, $order.order_id, $order.number | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
                orderDate   = (Get-StringValue (@($order.order_date, $order.date, $order.created_at | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
                items       = $itemsOut
            }
            $orderLink = (Get-StringValue (@($order.order_status_url, $order.order_status_link, $order.link, $order.status_url, $order.status_link | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
            if ($orderLink) { $orderOut.link = $orderLink }
            $trackingLink = (Get-StringValue (@($order.order_tracking_link, $order.tracking_link, $order.tracking_url, $order.order_tracking_url | Where-Object { $null -ne $_ } | Select-Object -First 1))).Trim()
            if ($trackingLink) {
                $orderOut.trackingLink = $trackingLink
                $orderOut.order_tracking_link = $trackingLink
            }
            if ($null -ne $order.total -and -not [string]::IsNullOrWhiteSpace((Get-StringValue $order.total))) {
                $orderOut.total = $order.total
            }
            if ($orderOut.orderNumber -or $orderOut.orderDate -or $orderOut.items.Count -gt 0 -or $orderOut.link -or $orderOut.total -or $orderOut.trackingLink) {
                $ordersOut += $orderOut
            }
        }
    }

    $companyWebsite = (Get-RowValue -Row $Row -Candidates @("COMPANY_WEBSITE", "WEBSITE", "SITE_URL")).Trim()
    $rightPanel = @{
        source = @{
            label = "Website"
            value = $companyWebsite
            date  = ""
        }
    }
    if ($browsingHistory.Count -gt 0) { $rightPanel.browsingHistory = $browsingHistory }
    if ($ordersOut.Count -gt 0) { $rightPanel.orders = $ordersOut }

    $notesText = [string](Get-RowValue -Row $Row -Candidates @("COMPANY_NOTES", "NOTES", "GUIDELINES", "INTERNAL_NOTES"))
    if ($null -eq $notesText) { $notesText = "" }
    $notesText = $notesText.Trim()
    $notes = Parse-CompanyNotesToCategories -NotesText $notesText

    return @{
        id                     = (Get-RowValue -Row $Row -Candidates @("SEND_ID", "SCENARIO_ID", "ID")).Trim()
        companyName            = (Get-RowValue -Row $Row -Candidates @("COMPANY_NAME", "BRAND", "COMPANY")).Trim()
        companyWebsite         = $companyWebsite
        agentName              = (Get-RowValue -Row $Row -Candidates @("PERSONA", "AGENT_NAME", "AGENT")).Trim()
        messageTone            = (Get-RowValue -Row $Row -Candidates @("MESSAGE_TONE", "TONE")).Trim()
        conversation           = $conversation
        notes                  = $notes
        rightPanel             = $rightPanel
        escalation_preferences = Convert-ToStringArray -Value (Parse-ListLikeText -Text (Get-RowValue -Row $Row -Candidates @("ESCALATION_TOPICS", "ESCALATION_PREFERENCES", "ESCALATIONS")))
        blocklisted_words      = Convert-ToStringArray -Value (Parse-ListLikeText -Text (Get-RowValue -Row $Row -Candidates @("BLOCKLISTED_WORDS", "BLOCKLIST_WORDS", "BLOCKLIST", "BLOCKED_WORDS")))
    }
}

function Ensure-Directory {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function Normalize-CompanyKey {
    param([string]$Name)
    $text = (Get-StringValue $Name).Trim().ToLowerInvariant()
    if (-not $text) { return "" }
    $text = [regex]::Replace($text, "\s+", " ")
    return $text
}

function Convert-CompanyKeyToSlug {
    param([string]$CompanyKey)
    $slug = [regex]::Replace(($CompanyKey.ToLowerInvariant()), "[^a-z0-9]+", "-").Trim("-")
    if (-not $slug) { return "company" }
    return $slug
}

function Get-TemplateListFromContainer {
    param($Container)

    if ($null -eq $Container) { return @() }
    if ($Container -is [System.Array]) { return @($Container) }
    if ($Container.PSObject.Properties.Name -contains 'templates' -and $Container.templates -is [System.Array]) {
        return @($Container.templates)
    }
    return @()
}

function Convert-RecordToHashtable {
    param($InputObject)

    $map = @{}
    if ($null -eq $InputObject) { return $map }

    if ($InputObject -is [System.Collections.IDictionary]) {
        foreach ($key in $InputObject.Keys) {
            $map[[string]$key] = $InputObject[$key]
        }
        return $map
    }

    if ($InputObject.PSObject -and $InputObject.PSObject.Properties) {
        foreach ($p in $InputObject.PSObject.Properties) {
            $map[$p.Name] = $p.Value
        }
    }

    return $map
}

function Normalize-TemplateRecordForStorage {
    param($Template)

    $source = Convert-RecordToHashtable -InputObject $Template
    $canonical = @('name', 'content', 'id', 'shortcut', 'companyName')
    $out = [ordered]@{}

    $name = (Get-StringValue $source.name).Trim()
    $content = (Get-StringValue $source.content).Trim()
    $id = (Get-StringValue $source.id).Trim()
    $shortcut = (Get-StringValue $source.shortcut).Trim()
    $companyName = (Get-StringValue $source.companyName).Trim()

    $out.name = $name
    $out.content = $content
    if ($id) { $out.id = $id }
    if ($shortcut) { $out.shortcut = $shortcut }
    if ($companyName) { $out.companyName = $companyName }

    foreach ($key in $source.Keys) {
        if ($canonical -contains [string]$key) { continue }
        $out[[string]$key] = $source[$key]
    }

    return $out
}

function Normalize-TemplateKeyComponent {
    param([string]$Value)

    $text = (Get-StringValue $Value).Trim().ToLowerInvariant()
    if (-not $text) { return "" }
    $text = [regex]::Replace($text, "\s+", " ")
    return $text
}

function Get-TemplateCompositeKey {
    param($Template)

    $normalized = Normalize-TemplateRecordForStorage -Template $Template
    $companyKey = Normalize-TemplateKeyComponent -Value $normalized.companyName
    $nameKey = Normalize-TemplateKeyComponent -Value $normalized.name
    $shortcutKey = Normalize-TemplateKeyComponent -Value $normalized.shortcut
    if (-not $nameKey) { return "" }
    return "$companyKey|$nameKey|$shortcutKey"
}

function Get-TemplateIdentity {
    param($Template)

    $normalized = Normalize-TemplateRecordForStorage -Template $Template
    $idRaw = (Get-StringValue $normalized.id).Trim()
    $idKey = ""
    if ($idRaw) {
        $idKey = Normalize-TemplateKeyComponent -Value $idRaw
    }
    $compositeKey = Get-TemplateCompositeKey -Template $normalized

    return @{
        idKey = $idKey
        compositeKey = $compositeKey
    }
}

function Merge-TemplateRecord {
    param($ExistingTemplate, $IncomingTemplate)

    $baseMap = Convert-RecordToHashtable -InputObject (Normalize-TemplateRecordForStorage -Template $ExistingTemplate)
    $incomingMap = Convert-RecordToHashtable -InputObject (Normalize-TemplateRecordForStorage -Template $IncomingTemplate)
    $merged = @{}

    foreach ($key in $baseMap.Keys) {
        $merged[$key] = $baseMap[$key]
    }
    foreach ($key in $incomingMap.Keys) {
        $merged[$key] = $incomingMap[$key]
    }

    return (Normalize-TemplateRecordForStorage -Template $merged)
}

function Get-TemplateCanonicalSnapshot {
    param($Template)

    $normalized = Normalize-TemplateRecordForStorage -Template $Template
    return [ordered]@{
        id = (Get-StringValue $normalized.id).Trim()
        companyName = (Get-StringValue $normalized.companyName).Trim()
        name = (Get-StringValue $normalized.name).Trim()
        shortcut = (Get-StringValue $normalized.shortcut).Trim()
        content = (Get-StringValue $normalized.content).Trim()
    }
}

function Test-TemplateCanonicalChanged {
    param($ExistingTemplate, $CandidateTemplate)

    $existingSnapshot = Get-TemplateCanonicalSnapshot -Template $ExistingTemplate
    $candidateSnapshot = Get-TemplateCanonicalSnapshot -Template $CandidateTemplate
    $existingSig = $existingSnapshot | ConvertTo-Json -Compress -Depth 10
    $candidateSig = $candidateSnapshot | ConvertTo-Json -Compress -Depth 10
    return $existingSig -ne $candidateSig
}

function Merge-TemplatesIncremental {
    param(
        [array]$Existing = @(),
        [array]$Incoming = @()
    )

    if ($null -eq $Existing) { $Existing = @() }
    if ($null -eq $Incoming) { $Incoming = @() }

    $result = @()
    foreach ($item in $Existing) {
        $result += (Normalize-TemplateRecordForStorage -Template $item)
    }

    # Token maps let incoming rows collapse deterministically; last row wins.
    $tokenById = @{}
    $tokenByComposite = @{}
    for ($i = 0; $i -lt $result.Count; $i++) {
        $token = "E:$i"
        $identity = Get-TemplateIdentity -Template $result[$i]
        if ($identity.idKey -and -not $tokenById.ContainsKey($identity.idKey)) {
            $tokenById[$identity.idKey] = $token
        }
        if ($identity.compositeKey -and -not $tokenByComposite.ContainsKey($identity.compositeKey)) {
            $tokenByComposite[$identity.compositeKey] = $token
        }
    }

    $winnerByToken = @{}
    $tokenOrder = @()
    $seenIncomingTokens = @{}

    $invalidSkipped = 0
    $incomingDuplicates = 0

    foreach ($item in $Incoming) {
        $normalizedIncoming = Normalize-TemplateRecordForStorage -Template $item
        $name = (Get-StringValue $normalizedIncoming.name).Trim()
        $content = (Get-StringValue $normalizedIncoming.content).Trim()
        if (-not $name -or -not $content) {
            $invalidSkipped++
            continue
        }

        $identity = Get-TemplateIdentity -Template $normalizedIncoming
        if (-not $identity.idKey -and -not $identity.compositeKey) {
            $invalidSkipped++
            continue
        }

        $token = ""
        $resolvedBy = "new"

        if ($identity.idKey -and $tokenById.ContainsKey($identity.idKey)) {
            $token = [string]$tokenById[$identity.idKey]
            $resolvedBy = "id"
        } elseif ($identity.compositeKey -and $tokenByComposite.ContainsKey($identity.compositeKey)) {
            $token = [string]$tokenByComposite[$identity.compositeKey]
            $resolvedBy = "composite"
        } else {
            $token = "N:$($tokenOrder.Count + 1)"
        }

        if ($seenIncomingTokens.ContainsKey($token)) {
            $incomingDuplicates++
        } else {
            $seenIncomingTokens[$token] = $true
            $tokenOrder += $token
        }

        $winnerByToken[$token] = @{
            template = $normalizedIncoming
            resolvedBy = $resolvedBy
        }

        if ($identity.idKey) {
            $tokenById[$identity.idKey] = $token
        }
        if ($identity.compositeKey) {
            $tokenByComposite[$identity.compositeKey] = $token
        }
    }

    $added = 0
    $updated = 0
    $unchanged = 0
    $matchedById = 0
    $matchedByComposite = 0

    foreach ($token in $tokenOrder) {
        $winner = $winnerByToken[$token]
        if ($token -like "E:*") {
            $indexText = $token.Substring(2)
            $targetIndex = -1
            if (-not [int]::TryParse($indexText, [ref]$targetIndex)) {
                continue
            }
            if ($targetIndex -lt 0 -or $targetIndex -ge $result.Count) {
                continue
            }

            if ($winner.resolvedBy -eq "id") {
                $matchedById++
            } elseif ($winner.resolvedBy -eq "composite") {
                $matchedByComposite++
            }

            $existingRecord = $result[$targetIndex]
            $mergedRecord = Merge-TemplateRecord -ExistingTemplate $existingRecord -IncomingTemplate $winner.template
            if (Test-TemplateCanonicalChanged -ExistingTemplate $existingRecord -CandidateTemplate $mergedRecord) {
                $result[$targetIndex] = $mergedRecord
                $updated++
            } else {
                $unchanged++
            }
            continue
        }

        $result += $winner.template
        $added++
    }

    return @{
        templates = $result
        added = $added
        updated = $updated
        unchanged = $unchanged
        incomingDuplicates = $incomingDuplicates
        matchedById = $matchedById
        matchedByComposite = $matchedByComposite
        invalidSkipped = $invalidSkipped
    }
}

function Build-RuntimeArtifacts {
    param(
        [switch]$Quiet
    )

    $dataRoot = Join-Path $script:CurrentFolder "data"
    $scenarioRoot = Join-Path $dataRoot "scenarios"
    $scenarioChunksRoot = Join-Path $scenarioRoot "chunks"
    $templateRoot = Join-Path $dataRoot "templates"
    $templateCompaniesRoot = Join-Path $templateRoot "companies"

    Ensure-Directory -Path $dataRoot
    Ensure-Directory -Path $scenarioRoot
    Ensure-Directory -Path $scenarioChunksRoot
    Ensure-Directory -Path $templateRoot
    Ensure-Directory -Path $templateCompaniesRoot

    $chunkSize = 5

    # ---- Scenarios runtime index/chunks ----
    $scenarioContainer = Get-JsonObject -Path (Get-ScenariosPath)
    $scenarioList = @(Convert-ScenarioContainerToList -Container $scenarioContainer)
    if ($null -eq $scenarioList) { $scenarioList = @() }

    $scenarioOrder = @()
    $scenarioByKey = [ordered]@{}
    $scenarioById = [ordered]@{}
    $scenarioChunkBuckets = @{}

    for ($i = 0; $i -lt $scenarioList.Count; $i++) {
        $scenarioKey = [string]($i + 1)
        $scenarioRecord = Normalize-ScenarioRecordForStorage -Scenario $scenarioList[$i]
        $scenarioOrder += $scenarioKey

        $chunkNumber = [int]([math]::Floor($i / $chunkSize) + 1)
        $chunkBase = ("chunk_{0:D4}" -f $chunkNumber)
        $chunkFileName = "$chunkBase.json"
        if (-not $scenarioChunkBuckets.ContainsKey($chunkFileName)) {
            $scenarioChunkBuckets[$chunkFileName] = [ordered]@{}
        }
        $scenarioChunkBuckets[$chunkFileName][$scenarioKey] = $scenarioRecord

        $scenarioId = (Get-StringValue $scenarioRecord.id).Trim()
        $companyName = (Get-StringValue $scenarioRecord.companyName).Trim()
        $scenarioByKey[$scenarioKey] = [ordered]@{
            id        = $scenarioId
            companyName = $companyName
            chunkFile = "data/scenarios/chunks/$chunkFileName"
        }
        if ($scenarioId -and -not $scenarioById.Contains($scenarioId)) {
            $scenarioById[$scenarioId] = $scenarioKey
        }
    }

    $keptScenarioChunks = @{}
    foreach ($chunkFileName in $scenarioChunkBuckets.Keys) {
        $chunkPath = Join-Path $scenarioChunksRoot $chunkFileName
        Write-JsonObject -Path $chunkPath -Value ([ordered]@{
            version = 1
            chunk = [System.IO.Path]::GetFileNameWithoutExtension($chunkFileName)
            scenarios = $scenarioChunkBuckets[$chunkFileName]
        })
        $keptScenarioChunks[$chunkFileName] = $true
    }

    Get-ChildItem -LiteralPath $scenarioChunksRoot -File -Filter "*.json" | ForEach-Object {
        if (-not $keptScenarioChunks.ContainsKey($_.Name)) {
            Remove-Item -LiteralPath $_.FullName -Force
        }
    }

    Write-JsonObject -Path (Join-Path $scenarioRoot "index.json") -Value ([ordered]@{
        version = 1
        chunkSize = $chunkSize
        order = $scenarioOrder
        byKey = $scenarioByKey
        byId = $scenarioById
    })

    # ---- Templates runtime index/global/company bundles ----
    $templatesContainer = Get-JsonObject -Path (Get-TemplatesPath)
    $templateList = @(Get-TemplateListFromContainer -Container $templatesContainer)
    if ($null -eq $templateList) { $templateList = @() }

    $scenarioCompanyKeys = @{}
    foreach ($scenario in $scenarioList) {
        $companyKey = Normalize-CompanyKey -Name $scenario.companyName
        if ($companyKey) { $scenarioCompanyKeys[$companyKey] = $true }
    }

    $globalTemplates = @()
    $templatesByCompany = @{}
    foreach ($template in $templateList) {
        $companyKey = Normalize-CompanyKey -Name $template.companyName
        if (-not $companyKey) {
            $globalTemplates += $template
            continue
        }
        if (-not $scenarioCompanyKeys.ContainsKey($companyKey)) {
            continue
        }
        if (-not $templatesByCompany.ContainsKey($companyKey)) {
            $templatesByCompany[$companyKey] = @()
        }
        $templatesByCompany[$companyKey] += $template
    }

    Write-JsonObject -Path (Join-Path $templateRoot "global.json") -Value @{ templates = $globalTemplates }

    $templateCompaniesMap = [ordered]@{}
    $usedSlugs = @{}
    $keptCompanyTemplateFiles = @{}
    $companyKeys = @($templatesByCompany.Keys | Sort-Object)
    foreach ($companyKey in $companyKeys) {
        $baseSlug = Convert-CompanyKeyToSlug -CompanyKey $companyKey
        $slug = $baseSlug
        $suffix = 2
        while ($usedSlugs.ContainsKey($slug)) {
            $slug = "$baseSlug-$suffix"
            $suffix++
        }
        $usedSlugs[$slug] = $true

        $fileName = "$slug.json"
        $filePath = Join-Path $templateCompaniesRoot $fileName
        Write-JsonObject -Path $filePath -Value ([ordered]@{
            companyKey = $companyKey
            templates = $templatesByCompany[$companyKey]
        })

        $templateCompaniesMap[$companyKey] = "data/templates/companies/$fileName"
        $keptCompanyTemplateFiles[$fileName] = $true
    }

    Get-ChildItem -LiteralPath $templateCompaniesRoot -File -Filter "*.json" | ForEach-Object {
        if (-not $keptCompanyTemplateFiles.ContainsKey($_.Name)) {
            Remove-Item -LiteralPath $_.FullName -Force
        }
    }

    Write-JsonObject -Path (Join-Path $templateRoot "index.json") -Value ([ordered]@{
        version = 1
        globalFile = "data/templates/global.json"
        companies = $templateCompaniesMap
    })

    $summary = [ordered]@{
        scenarios = $scenarioList.Count
        scenarioChunks = $scenarioChunkBuckets.Keys.Count
        templates = $templateList.Count
        templateCompanies = $companyKeys.Count
    }

    if (-not $Quiet) {
        Set-Status -Message "Runtime artifacts built. Scenarios: $($summary.scenarios), Chunks: $($summary.scenarioChunks), Templates: $($summary.templates), Template companies: $($summary.templateCompanies)."
    }
    return $summary
}

function Import-JsonToPath {
    param(
        [Parameter(Mandatory = $true)][string]$TargetPath,
        [Parameter(Mandatory = $true)][string]$Label
    )

    $dialog = New-Object System.Windows.Forms.OpenFileDialog
    $dialog.InitialDirectory = $script:CurrentFolder
    $dialog.RestoreDirectory = $true
    $dialog.Filter = "Template sources (*.json;*.csv)|*.json;*.csv|JSON files (*.json)|*.json|CSV files (*.csv)|*.csv|All files (*.*)|*.*"
    $dialog.FilterIndex = 1
    $dialog.Multiselect = $false
    if ($dialog.ShowDialog() -ne [System.Windows.Forms.DialogResult]::OK) {
        return
    }

    try {
        $isTemplateImport = [string]::Equals(
            [System.IO.Path]::GetFileName($TargetPath),
            "templates.json",
            [System.StringComparison]::OrdinalIgnoreCase
        )
        $ext = [System.IO.Path]::GetExtension($dialog.FileName).ToLowerInvariant()

        # Template imports are incremental: add/update only, keep missing existing rows.
        if ($isTemplateImport) {
            $incomingTemplates = @()
            if ($ext -eq ".csv") {
                $rows = Import-Csv -LiteralPath $dialog.FileName
                foreach ($row in $rows) {
                    $name = (Get-StringValue ($row.TEMPLATE_TITLE, $row.TEMPLATE_NAME, $row.NAME, $row.TEMPLATE, $row.TITLE | Where-Object { $_ } | Select-Object -First 1)).Trim()
                    $content = (Get-StringValue ($row.TEMPLATE_TEXT, $row.CONTENT, $row.TEMPLATE_CONTENT, $row.BODY, $row.TEXT, $row.MESSAGE | Where-Object { $_ } | Select-Object -First 1)).Trim()
                    $shortcut = (Get-StringValue ($row.SHORTCUT, $row.CODE, $row.KEYWORD | Where-Object { $_ } | Select-Object -First 1)).Trim()
                    $company = (Get-StringValue ($row.COMPANY_NAME, $row.COMPANY, $row.BRAND | Where-Object { $_ } | Select-Object -First 1)).Trim()
                    $templateId = (Get-StringValue ($row.TEMPLATE_ID, $row.ID | Where-Object { $_ } | Select-Object -First 1)).Trim()

                    if (-not $name -or -not $content) { continue }

                    $template = @{
                        name = $name
                        content = $content
                    }
                    if ($templateId) { $template.id = $templateId }
                    if ($shortcut) { $template.shortcut = $shortcut }
                    if ($company) { $template.companyName = $company }
                    $incomingTemplates += $template
                }
            } else {
                $raw = Get-Content -LiteralPath $dialog.FileName -Raw -ErrorAction Stop
                $parsed = $raw | ConvertFrom-Json
                $incomingTemplates = @(Get-TemplateListFromContainer -Container $parsed)
            }
            if ($null -eq $incomingTemplates) { $incomingTemplates = @() }

            $existingContainer = Get-JsonObject -Path $TargetPath
            $existingTemplates = @(Get-TemplateListFromContainer -Container $existingContainer)
            if ($null -eq $existingTemplates) { $existingTemplates = @() }

            $merge = Merge-TemplatesIncremental -Existing $existingTemplates -Incoming $incomingTemplates
            Write-JsonObject -Path $TargetPath -Value @{ templates = $merge.templates }
            $artifactSummary = Build-RuntimeArtifacts -Quiet
            Refresh-Meta

            $sourceName = if ($ext -eq ".csv") { "CSV" } else { $dialog.SafeFileName }
            Set-Status -Message (
                "$Label merged from $sourceName. Added: $($merge.added), Updated: $($merge.updated), Unchanged: $($merge.unchanged), " +
                "Duplicates: $($merge.incomingDuplicates), MatchedById: $($merge.matchedById), MatchedByComposite: $($merge.matchedByComposite), " +
                "Skipped: $($merge.invalidSkipped). Runtime artifacts refreshed ($($artifactSummary.scenarioChunks) scenario chunks, $($artifactSummary.templateCompanies) template company bundles)."
            )
            return
        }

        if ($ext -eq ".csv") {
            throw "CSV import is only supported for templates.json in this manager."
        }

        $raw = Get-Content -LiteralPath $dialog.FileName -Raw -ErrorAction Stop
        $parsed = $raw | ConvertFrom-Json
        Write-JsonObject -Path $TargetPath -Value $parsed
        $artifactSummary = Build-RuntimeArtifacts -Quiet
        Refresh-Meta
        Set-Status -Message "$Label updated from $($dialog.SafeFileName). Runtime artifacts refreshed ($($artifactSummary.scenarioChunks) scenario chunks, $($artifactSummary.templateCompanies) template company bundles)."
    } catch {
        Set-Status -Message ("Failed to import ${Label}: " + $_.Exception.Message) -IsError $true
    }
}

function Import-ScenariosFromFile {
    param(
        [Parameter(Mandatory = $true)][string]$TargetPath
    )

    $dialog = New-Object System.Windows.Forms.OpenFileDialog
    $dialog.InitialDirectory = $script:CurrentFolder
    $dialog.RestoreDirectory = $true
    $dialog.Filter = "Scenario sources (*.json;*.csv)|*.json;*.csv|JSON files (*.json)|*.json|CSV files (*.csv)|*.csv|All files (*.*)|*.*"
    $dialog.Multiselect = $false
    if ($dialog.ShowDialog() -ne [System.Windows.Forms.DialogResult]::OK) {
        return
    }

    try {
        $existingObj = Get-JsonObject -Path $TargetPath
        $existingList = @(Convert-ScenarioContainerToList -Container $existingObj)
        if ($null -eq $existingList) { $existingList = @() }
        $ext = [System.IO.Path]::GetExtension($dialog.FileName).ToLowerInvariant()
        if ($ext -eq ".csv") {
            $rows = Import-Csv -LiteralPath $dialog.FileName
            $incomingScenarios = @()
            $invalidRows = 0
            $parseErrorRows = 0
            $rowIndex = 0
            foreach ($row in $rows) {
                $rowIndex++
                try {
                    $scenario = Convert-CsvRowToScenario -Row $row
                    $scenarioId = (Get-StringValue $scenario.id).Trim()
                    $companyName = (Get-StringValue $scenario.companyName).Trim()
                    if (-not $scenarioId -or -not $companyName) {
                        $invalidRows++
                        continue
                    }
                    $incomingScenarios += $scenario
                } catch {
                    $parseErrorRows++
                }
            }
            if ($null -eq $incomingScenarios) { $incomingScenarios = @() }
            if ($incomingScenarios.Count -eq 0) {
                throw "No valid scenarios found in CSV. Ensure SEND_ID and COMPANY_NAME columns are populated."
            }
            $merge = Merge-ScenariosById -Existing $existingList -Incoming $incomingScenarios
            Write-JsonObject -Path $TargetPath -Value @{ scenarios = $merge.scenarios }
            $artifactSummary = Build-RuntimeArtifacts -Quiet
            Refresh-Meta
            Set-Status -Message "scenarios.json updated from CSV. Added: $($merge.added), Updated: $($merge.updated), Skipped: $invalidRows, Parse errors: $parseErrorRows. Runtime artifacts refreshed ($($artifactSummary.scenarioChunks) chunks)."
            return
        }

        $raw = Get-Content -LiteralPath $dialog.FileName -Raw -ErrorAction Stop
        $parsed = $raw | ConvertFrom-Json
        $incomingList = Convert-ScenarioContainerToList -Container $parsed
        if ($incomingList.Count -eq 0) {
            throw "No scenarios found in selected file."
        }
        if ($null -eq $incomingList) { $incomingList = @() }
        $merge = Merge-ScenariosById -Existing $existingList -Incoming $incomingList
        Write-JsonObject -Path $TargetPath -Value @{ scenarios = $merge.scenarios }
        $artifactSummary = Build-RuntimeArtifacts -Quiet
        Refresh-Meta
        Set-Status -Message "scenarios.json updated from $($dialog.SafeFileName). Added: $($merge.added), Updated: $($merge.updated). Runtime artifacts refreshed ($($artifactSummary.scenarioChunks) chunks)."
    } catch {
        Set-Status -Message ("Failed to import scenarios source: " + $_.Exception.Message) -IsError $true
    }
}

$chooseFolderBtn.Add_Click({
    $dialog = New-Object System.Windows.Forms.FolderBrowserDialog
    $dialog.SelectedPath = $script:CurrentFolder
    if ($dialog.ShowDialog() -ne [System.Windows.Forms.DialogResult]::OK) {
        return
    }

    $script:CurrentFolder = $dialog.SelectedPath
    $folderLabel.Text = "Folder: $script:CurrentFolder"
    Refresh-Meta
    Set-Status -Message "Connected folder: $script:CurrentFolder"
})

$uploadScenariosBtn.Add_Click({
    Import-ScenariosFromFile -TargetPath (Get-ScenariosPath)
})

$uploadTemplatesBtn.Add_Click({
    Import-JsonToPath -TargetPath (Get-TemplatesPath) -Label "templates.json"
})

$clearScenariosBtn.Add_Click({
    $result = [System.Windows.Forms.MessageBox]::Show(
        "Clear scenarios.json and reset it to { `"scenarios`": [] }?",
        "Confirm Clear",
        [System.Windows.Forms.MessageBoxButtons]::YesNo,
        [System.Windows.Forms.MessageBoxIcon]::Warning
    )
    if ($result -ne [System.Windows.Forms.DialogResult]::Yes) { return }

    try {
        Write-JsonObject -Path (Get-ScenariosPath) -Value @{ scenarios = @() }
        $artifactSummary = Build-RuntimeArtifacts -Quiet
        Refresh-Meta
        Set-Status -Message "scenarios.json cleared. Runtime artifacts refreshed ($($artifactSummary.scenarioChunks) chunks)."
    } catch {
        Set-Status -Message ("Failed to clear scenarios.json: " + $_.Exception.Message) -IsError $true
    }
})

$clearTemplatesBtn.Add_Click({
    $result = [System.Windows.Forms.MessageBox]::Show(
        "Clear templates.json and reset it to { `"templates`": [] }?",
        "Confirm Clear",
        [System.Windows.Forms.MessageBoxButtons]::YesNo,
        [System.Windows.Forms.MessageBoxIcon]::Warning
    )
    if ($result -ne [System.Windows.Forms.DialogResult]::Yes) { return }

    try {
        Write-JsonObject -Path (Get-TemplatesPath) -Value @{ templates = @() }
        $artifactSummary = Build-RuntimeArtifacts -Quiet
        Refresh-Meta
        Set-Status -Message "templates.json cleared. Runtime artifacts refreshed ($($artifactSummary.templateCompanies) template company bundles)."
    } catch {
        Set-Status -Message ("Failed to clear templates.json: " + $_.Exception.Message) -IsError $true
    }
})

$openFolderBtn.Add_Click({
    try {
        Start-Process explorer.exe $script:CurrentFolder | Out-Null
    } catch {
        Set-Status -Message ("Could not open folder: " + $_.Exception.Message) -IsError $true
    }
})

try {
    Build-RuntimeArtifacts -Quiet | Out-Null
} catch {
    Set-Status -Message ("Failed to build runtime artifacts on startup: " + $_.Exception.Message) -IsError $true
}

Refresh-Meta
[void]$form.ShowDialog()
