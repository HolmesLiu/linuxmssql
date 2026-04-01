using System.Data;
using System.Globalization;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Xml;
using Microsoft.Data.SqlClient;
using SqlServerTool.UbuntuService.Models;

namespace SqlServerTool.UbuntuService.Services;

public sealed partial class SqlTransferService
{
    private static List<string> LoadTablesFromExcel(string excelPath, string sheetName)
    {
        using ZipArchive zip = ZipFile.OpenRead(excelPath);
        List<string> shared = LoadSharedStrings(zip);
        string sheetPath = ResolveSheetPath(zip, sheetName);
        ZipArchiveEntry sheetEntry = zip.GetEntry(sheetPath) ?? throw new InvalidOperationException($"找不到工作表: {sheetPath}");

        XmlDocument doc = new();
        using (Stream stream = sheetEntry.Open()) { doc.Load(stream); }

        XmlNamespaceManager nsmgr = new(doc.NameTable);
        nsmgr.AddNamespace("m", "http://schemas.openxmlformats.org/spreadsheetml/2006/main");

        List<string> tables = [];
        XmlNodeList? rows = doc.SelectNodes("//m:sheetData/m:row", nsmgr);
        if (rows is null) return tables;

        foreach (XmlNode row in rows)
        {
            XmlNode? cellA = row.SelectSingleNode("m:c[starts-with(@r,'A')]", nsmgr);
            if (cellA is null) continue;

            string text = ResolveCellText(cellA, shared, nsmgr);
            if (!string.IsNullOrWhiteSpace(text) && text.StartsWith("tb_", StringComparison.OrdinalIgnoreCase))
            {
                tables.Add(text.Trim());
            }
        }

        return tables.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
    }

    private static List<string> LoadSharedStrings(ZipArchive zip)
    {
        ZipArchiveEntry? entry = zip.GetEntry("xl/sharedStrings.xml");
        if (entry is null) return [];

        XmlDocument doc = new();
        using (Stream stream = entry.Open()) { doc.Load(stream); }

        XmlNamespaceManager nsmgr = new(doc.NameTable);
        nsmgr.AddNamespace("m", "http://schemas.openxmlformats.org/spreadsheetml/2006/main");

        List<string> values = [];
        XmlNodeList? nodes = doc.SelectNodes("//m:si", nsmgr);
        if (nodes is null) return values;

        foreach (XmlNode si in nodes)
        {
            XmlNodeList? tNodes = si.SelectNodes(".//m:t", nsmgr);
            if (tNodes is null) { values.Add(string.Empty); continue; }
            StringBuilder sb = new();
            foreach (XmlNode t in tNodes) sb.Append(t.InnerText);
            values.Add(sb.ToString());
        }

        return values;
    }

    private static string ResolveSheetPath(ZipArchive zip, string sheetName)
    {
        XmlDocument wb = new();
        using (Stream stream = (zip.GetEntry("xl/workbook.xml") ?? throw new InvalidOperationException("缺少 workbook.xml")).Open()) { wb.Load(stream); }

        XmlNamespaceManager nsmgr = new(wb.NameTable);
        nsmgr.AddNamespace("m", "http://schemas.openxmlformats.org/spreadsheetml/2006/main");
        nsmgr.AddNamespace("r", "http://schemas.openxmlformats.org/officeDocument/2006/relationships");

        XmlNode? sheet = wb.SelectSingleNode($"//m:sheets/m:sheet[translate(@name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')='{sheetName.ToLowerInvariant()}']", nsmgr)
            ?? wb.SelectSingleNode("//m:sheets/m:sheet", nsmgr)
            ?? throw new InvalidOperationException("Excel 中没有工作表。");

        string relationId = sheet.Attributes?["id", "http://schemas.openxmlformats.org/officeDocument/2006/relationships"]?.Value
            ?? throw new InvalidOperationException("工作表关系ID缺失。");

        XmlDocument rel = new();
        using (Stream stream = (zip.GetEntry("xl/_rels/workbook.xml.rels") ?? throw new InvalidOperationException("缺少 workbook.xml.rels")).Open()) { rel.Load(stream); }

        XmlNamespaceManager rmgr = new(rel.NameTable);
        rmgr.AddNamespace("p", "http://schemas.openxmlformats.org/package/2006/relationships");

        XmlNode? relNode = rel.SelectSingleNode($"//p:Relationship[@Id='{relationId}']", rmgr)
            ?? rel.SelectSingleNode($"//Relationship[@Id='{relationId}']")
            ?? throw new InvalidOperationException("找不到工作表关系。");

        string target = relNode.Attributes?["Target"]?.Value ?? throw new InvalidOperationException("工作表关系Target缺失。");
        return target.StartsWith("xl/", StringComparison.OrdinalIgnoreCase) ? target : $"xl/{target.TrimStart('/')}";
    }

    private static string ResolveCellText(XmlNode cell, IReadOnlyList<string> shared, XmlNamespaceManager nsmgr)
    {
        XmlNode? valueNode = cell.SelectSingleNode("m:v", nsmgr);
        if (valueNode is null) return string.Empty;

        string raw = valueNode.InnerText;
        if (!string.Equals(cell.Attributes?["t"]?.Value, "s", StringComparison.OrdinalIgnoreCase))
        {
            return raw;
        }

        return int.TryParse(raw, out int idx) && idx >= 0 && idx < shared.Count ? shared[idx] : string.Empty;
    }

    private static async Task<DailyBackupState> LoadDailyBackupStateAsync(string statePath, CancellationToken cancellationToken)
    {
        if (!File.Exists(statePath)) return new DailyBackupState();
        await using FileStream stream = File.OpenRead(statePath);
        DailyBackupState? state = await JsonSerializer.DeserializeAsync<DailyBackupState>(stream, JsonOptions, cancellationToken);
        return state ?? new DailyBackupState();
    }

    private static async Task SaveDailyBackupStateAsync(string statePath, DailyBackupState state, CancellationToken cancellationToken)
    {
        await using FileStream stream = File.Create(statePath);
        await JsonSerializer.SerializeAsync(stream, state, JsonOptions, cancellationToken);
    }

    private static string FormatWatermark(object value)
    {
        return value switch
        {
            DateTime dt => dt.ToString("O", CultureInfo.InvariantCulture),
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
        };
    }
}