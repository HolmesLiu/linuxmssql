using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using SqlServerTool.UbuntuService.Models;

namespace SqlServerTool.UbuntuService.Services;

public sealed partial class SqlTransferService
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    public async Task<IReadOnlyList<string>> GetTableNamesAsync(string connectionString, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT s.name + '.' + t.name AS TableName
            FROM sys.tables t
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            ORDER BY s.name, t.name;
            """;

        List<string> tables = [];
        await using SqlConnection connection = new(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using SqlCommand command = new(sql, connection);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            tables.Add(reader.GetString(0));
        }

        return tables;
    }

    public async Task<ExportResult> ExportAsync(ExportRequest request, CancellationToken cancellationToken)
    {
        ValidateExportRequest(request);

        Directory.CreateDirectory(request.OutputDirectory);
        string batchDirectory = Path.Combine(request.OutputDirectory, $"Export_{request.Format}_{request.Mode}_{DateTime.Now:yyyyMMdd_HHmmss}");
        Directory.CreateDirectory(batchDirectory);

        IReadOnlyList<string> tables = request.Tables.Count > 0
            ? request.Tables
            : await GetTableNamesAsync(request.ConnectionString, cancellationToken);

        int fileCount = 0;

        await using SqlConnection connection = new(request.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        foreach (string table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();

            (string schemaName, string tableName) = ParseTableName(table);
            DataTable data = await LoadTableDataAsync(connection, schemaName, tableName, request, cancellationToken);

            string filePrefix = BuildFilePrefix(schemaName, tableName, request);

            switch (request.Format.ToLowerInvariant())
            {
                case "json":
                {
                    string jsonPath = Path.Combine(batchDirectory, $"{filePrefix}.json");
                    await File.WriteAllTextAsync(jsonPath, BuildJson(schemaName, tableName, data, request), new UTF8Encoding(false), cancellationToken);
                    fileCount++;
                    break;
                }
                case "csv":
                {
                    string csvPath = Path.Combine(batchDirectory, $"{filePrefix}.csv");
                    await File.WriteAllTextAsync(csvPath, BuildCsv(data), new UTF8Encoding(false), cancellationToken);
                    fileCount++;
                    break;
                }
                default:
                {
                    string sqlPath = Path.Combine(batchDirectory, $"{filePrefix}.sql");
                    await File.WriteAllTextAsync(sqlPath, BuildSqlInserts(schemaName, tableName, data), new UTF8Encoding(false), cancellationToken);
                    fileCount++;
                    break;
                }
            }
        }

        return new ExportResult
        {
            BatchDirectory = batchDirectory,
            FileCount = fileCount
        };
    }

    public async Task<ImportResult> ImportAsync(ImportRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.ConnectionString))
        {
            throw new InvalidOperationException("ConnectionString 不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.InputPath))
        {
            throw new InvalidOperationException("InputPath 不能为空。");
        }

        string format = request.Format.ToLowerInvariant();
        if (!File.Exists(request.InputPath))
        {
            throw new FileNotFoundException("找不到导入文件。", request.InputPath);
        }

        await using SqlConnection connection = new(request.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        return format switch
        {
            "json" => await ImportJsonAsync(connection, request, cancellationToken),
            "csv" => await ImportCsvAsync(connection, request, cancellationToken),
            _ => await ImportSqlAsync(connection, request, cancellationToken)
        };
    }

    private async Task<DataTable> LoadTableDataAsync(
        SqlConnection connection,
        string schemaName,
        string tableName,
        ExportRequest request,
        CancellationToken cancellationToken)
    {
        string qualified = $"{EscapeIdentifier(schemaName)}.{EscapeIdentifier(tableName)}";
        await using SqlCommand command = BuildSelectCommand(connection, qualified, request);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

        DataTable table = new();
        table.Load(reader);
        return table;
    }

    private SqlCommand BuildSelectCommand(SqlConnection connection, string qualifiedTableName, ExportRequest request)
    {
        string mode = request.Mode.ToLowerInvariant();

        if (mode == "latest")
        {
            SqlCommand cmd = new(
                $"SELECT TOP (@TopCount) * FROM {qualifiedTableName} ORDER BY {EscapeIdentifier(request.FilterColumn)} DESC;",
                connection);
            cmd.Parameters.AddWithValue("@TopCount", request.LatestCount);
            return cmd;
        }

        if (mode == "range")
        {
            SqlCommand cmd = new(
                $"SELECT * FROM {qualifiedTableName} WHERE {EscapeIdentifier(request.FilterColumn)} >= @RangeStart AND {EscapeIdentifier(request.FilterColumn)} <= @RangeEnd ORDER BY {EscapeIdentifier(request.FilterColumn)} ASC;",
                connection);
            cmd.Parameters.AddWithValue("@RangeStart", ParseFilterValue(request.RangeStart, request.FilterDataType));
            cmd.Parameters.AddWithValue("@RangeEnd", ParseFilterValue(request.RangeEnd, request.FilterDataType));
            return cmd;
        }

        return new SqlCommand($"SELECT * FROM {qualifiedTableName};", connection);
    }

    private static object ParseFilterValue(string value, string filterDataType)
    {
        return filterDataType.ToLowerInvariant() switch
        {
            "number" => decimal.Parse(value, CultureInfo.InvariantCulture),
            "datetime" => DateTime.Parse(value, CultureInfo.InvariantCulture),
            _ => value
        };
    }

    private static string BuildSqlInserts(string schemaName, string tableName, DataTable data)
    {
        StringBuilder builder = new();
        string qualified = $"{EscapeIdentifier(schemaName)}.{EscapeIdentifier(tableName)}";

        builder.AppendLine($"-- Exported at {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        builder.AppendLine($"-- Table: {qualified}");
        builder.AppendLine("SET NOCOUNT ON;");
        builder.AppendLine();

        List<string> columns = data.Columns.Cast<DataColumn>().Select(column => EscapeIdentifier(column.ColumnName)).ToList();
        string columnList = string.Join(", ", columns);

        foreach (DataRow row in data.Rows)
        {
            IEnumerable<string> values = data.Columns.Cast<DataColumn>().Select(column => ToSqlLiteral(row[column]));
            builder.AppendLine($"INSERT INTO {qualified} ({columnList}) VALUES ({string.Join(", ", values)});");
        }

        if (data.Rows.Count == 0)
        {
            builder.AppendLine("-- No rows returned for this table/mode.");
        }

        builder.AppendLine("GO");
        return builder.ToString();
    }

    private static string BuildJson(string schemaName, string tableName, DataTable data, ExportRequest request)
    {
        var payload = new
        {
            table = $"{schemaName}.{tableName}",
            format = request.Format,
            mode = request.Mode,
            filterColumn = request.FilterColumn,
            filterDataType = request.FilterDataType,
            latestCount = request.LatestCount,
            rangeStart = request.RangeStart,
            rangeEnd = request.RangeEnd,
            rows = data.Rows.Cast<DataRow>().Select(row => data.Columns.Cast<DataColumn>().ToDictionary(
                c => c.ColumnName,
                c => NormalizeJsonValue(row[c])))
        };

        return JsonSerializer.Serialize(payload, JsonOptions);
    }

    private static string BuildCsv(DataTable data)
    {
        StringBuilder builder = new();

        builder.AppendLine(string.Join(",", data.Columns.Cast<DataColumn>().Select(c => EscapeCsv(c.ColumnName))));

        foreach (DataRow row in data.Rows)
        {
            IEnumerable<string> values = data.Columns.Cast<DataColumn>().Select(c => EscapeCsv(FormatCsvValue(row[c])));
            builder.AppendLine(string.Join(",", values));
        }

        return builder.ToString();
    }

    private static async Task<ImportResult> ImportSqlAsync(SqlConnection connection, ImportRequest request, CancellationToken cancellationToken)
    {
        string script = await File.ReadAllTextAsync(request.InputPath, cancellationToken);
        string[] batches = SplitSqlBatches(script);
        int affected = 0;

        foreach (string batch in batches)
        {
            if (string.IsNullOrWhiteSpace(batch))
            {
                continue;
            }

            await using SqlCommand cmd = new(batch, connection);
            affected += await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        return new ImportResult { AffectedRows = affected };
    }

    private static async Task<ImportResult> ImportJsonAsync(SqlConnection connection, ImportRequest request, CancellationToken cancellationToken)
    {
        string json = await File.ReadAllTextAsync(request.InputPath, cancellationToken);
        using JsonDocument doc = JsonDocument.Parse(json);

        string tableName = !string.IsNullOrWhiteSpace(request.TargetTable)
            ? request.TargetTable
            : doc.RootElement.GetProperty("table").GetString() ?? throw new InvalidOperationException("JSON 中缺少 table 字段。");

        if (!doc.RootElement.TryGetProperty("rows", out JsonElement rowsElement) || rowsElement.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("JSON 中缺少 rows 数组。");
        }

        int affected = 0;
        foreach (JsonElement row in rowsElement.EnumerateArray())
        {
            affected += await InsertJsonRowAsync(connection, tableName, row, cancellationToken);
        }

        return new ImportResult { AffectedRows = affected };
    }

    private static async Task<int> InsertJsonRowAsync(SqlConnection connection, string tableName, JsonElement row, CancellationToken cancellationToken)
    {
        List<string> columns = [];
        List<string> parameters = [];
        SqlCommand command = new() { Connection = connection };

        int index = 0;
        foreach (JsonProperty prop in row.EnumerateObject())
        {
            string parameterName = $"@p{index++}";
            columns.Add(EscapeIdentifier(prop.Name));
            parameters.Add(parameterName);
            command.Parameters.AddWithValue(parameterName, JsonValueToObject(prop.Value));
        }

        command.CommandText = $"INSERT INTO {NormalizeQualifiedName(tableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)});";
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<ImportResult> ImportCsvAsync(SqlConnection connection, ImportRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.TargetTable))
        {
            throw new InvalidOperationException("CSV 导入必须指定 --target-table。\n");
        }

        string[] lines = await File.ReadAllLinesAsync(request.InputPath, cancellationToken);
        if (lines.Length == 0)
        {
            return new ImportResult { AffectedRows = 0 };
        }

        string[] headers = ParseCsvLine(lines[0]);
        int affected = 0;

        for (int i = 1; i < lines.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i]))
            {
                continue;
            }

            string[] values = ParseCsvLine(lines[i]);
            affected += await InsertCsvRowAsync(connection, request.TargetTable, headers, values, cancellationToken);
        }

        return new ImportResult { AffectedRows = affected };
    }

    private static async Task<int> InsertCsvRowAsync(
        SqlConnection connection,
        string tableName,
        string[] headers,
        string[] values,
        CancellationToken cancellationToken)
    {
        SqlCommand command = new() { Connection = connection };
        List<string> columns = [];
        List<string> parameters = [];

        for (int i = 0; i < headers.Length; i++)
        {
            string parameterName = $"@p{i}";
            columns.Add(EscapeIdentifier(headers[i]));
            parameters.Add(parameterName);
            command.Parameters.AddWithValue(parameterName, i < values.Length ? values[i] : string.Empty);
        }

        command.CommandText = $"INSERT INTO {NormalizeQualifiedName(tableName)} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)});";
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string[] ParseCsvLine(string line)
    {
        List<string> values = [];
        StringBuilder current = new();
        bool inQuotes = false;

        for (int i = 0; i < line.Length; i++)
        {
            char ch = line[i];
            if (ch == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    current.Append('"');
                    i++;
                }
                else
                {
                    inQuotes = !inQuotes;
                }

                continue;
            }

            if (ch == ',' && !inQuotes)
            {
                values.Add(current.ToString());
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        values.Add(current.ToString());
        return values.ToArray();
    }

    private static string[] SplitSqlBatches(string script)
    {
        StringBuilder current = new();
        List<string> batches = [];

        using StringReader reader = new(script);
        while (reader.ReadLine() is { } line)
        {
            if (line.Trim().Equals("GO", StringComparison.OrdinalIgnoreCase))
            {
                batches.Add(current.ToString());
                current.Clear();
                continue;
            }

            current.AppendLine(line);
        }

        if (current.Length > 0)
        {
            batches.Add(current.ToString());
        }

        return batches.ToArray();
    }

    private static object JsonValueToObject(JsonElement value)
    {
        return value.ValueKind switch
        {
            JsonValueKind.Null => DBNull.Value,
            JsonValueKind.Number when value.TryGetInt64(out long intValue) => intValue,
            JsonValueKind.Number when value.TryGetDecimal(out decimal decimalValue) => decimalValue,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => value.ToString()
        };
    }

    private static object? NormalizeJsonValue(object value)
    {
        if (value == DBNull.Value)
        {
            return null;
        }

        return value switch
        {
            DateTime dt => dt.ToString("O", CultureInfo.InvariantCulture),
            byte[] bytes => Convert.ToBase64String(bytes),
            _ => value
        };
    }

    private static string FormatCsvValue(object value)
    {
        if (value == DBNull.Value)
        {
            return string.Empty;
        }

        return value switch
        {
            DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
            byte[] bytes => Convert.ToBase64String(bytes),
            bool b => b ? "1" : "0",
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
        };
    }

    private static string EscapeCsv(string value)
    {
        string escaped = value.Replace("\"", "\"\"", StringComparison.Ordinal);
        return $"\"{escaped}\"";
    }

    private static (string SchemaName, string TableName) ParseTableName(string table)
    {
        string[] parts = table.Split('.', 2);
        return parts.Length == 2 ? (parts[0], parts[1]) : ("dbo", parts[0]);
    }

    private static string BuildFilePrefix(string schemaName, string tableName, ExportRequest request)
    {
        string prefix = $"{schemaName}.{tableName}.{request.Format}.{request.Mode}";

        if (request.Mode.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            prefix += $".{SanitizeSegment(request.FilterColumn)}.top{request.LatestCount}";
        }

        if (request.Mode.Equals("range", StringComparison.OrdinalIgnoreCase))
        {
            prefix += $".{SanitizeSegment(request.FilterColumn)}.{SanitizeSegment(request.RangeStart)}_to_{SanitizeSegment(request.RangeEnd)}";
        }

        return prefix;
    }

    private static string SanitizeSegment(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "na";
        }

        char[] invalid = Path.GetInvalidFileNameChars();
        StringBuilder sb = new();

        foreach (char ch in value)
        {
            sb.Append(invalid.Contains(ch) || char.IsWhiteSpace(ch) ? '_' : ch);
        }

        return sb.ToString();
    }

    private static string NormalizeQualifiedName(string tableName)
    {
        (string schemaName, string objectName) = ParseTableName(tableName);
        return $"{EscapeIdentifier(schemaName)}.{EscapeIdentifier(objectName)}";
    }

    private static string EscapeIdentifier(string value)
    {
        return $"[{value.Replace("]", "]]", StringComparison.Ordinal)}]";
    }

    private static string ToSqlLiteral(object value)
    {
        if (value == DBNull.Value)
        {
            return "NULL";
        }

        return value switch
        {
            string text => $"N'{text.Replace("'", "''", StringComparison.Ordinal)}'",
            char ch => $"N'{ch.ToString().Replace("'", "''", StringComparison.Ordinal)}'",
            bool boolean => boolean ? "1" : "0",
            byte or short or int or long or sbyte or ushort or uint or ulong
                => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "0",
            decimal number => number.ToString(CultureInfo.InvariantCulture),
            double number => number.ToString("R", CultureInfo.InvariantCulture),
            float number => number.ToString("R", CultureInfo.InvariantCulture),
            DateTime dateTime => $"'{dateTime:yyyy-MM-dd HH:mm:ss.fff}'",
            Guid guid => $"'{guid:D}'",
            byte[] bytes => $"0x{Convert.ToHexString(bytes)}",
            TimeSpan time => $"'{time:c}'",
            _ => $"N'{Convert.ToString(value, CultureInfo.InvariantCulture)?.Replace("'", "''", StringComparison.Ordinal)}'"
        };
    }

    private static void ValidateExportRequest(ExportRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.ConnectionString))
        {
            throw new InvalidOperationException("ConnectionString 不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.OutputDirectory))
        {
            throw new InvalidOperationException("OutputDirectory 不能为空。");
        }

        string format = request.Format.ToLowerInvariant();
        if (format is not ("sql" or "json" or "csv"))
        {
            throw new InvalidOperationException("Format 仅支持 sql/json/csv。");
        }

        string mode = request.Mode.ToLowerInvariant();
        if (mode is not ("all" or "latest" or "range"))
        {
            throw new InvalidOperationException("Mode 仅支持 all/latest/range。");
        }

        if (mode == "latest")
        {
            if (string.IsNullOrWhiteSpace(request.FilterColumn))
            {
                throw new InvalidOperationException("latest 模式必须提供 FilterColumn。");
            }

            if (request.LatestCount <= 0)
            {
                throw new InvalidOperationException("latest 模式下 LatestCount 必须大于 0。");
            }
        }

        if (mode == "range")
        {
            if (string.IsNullOrWhiteSpace(request.FilterColumn))
            {
                throw new InvalidOperationException("range 模式必须提供 FilterColumn。");
            }

            if (string.IsNullOrWhiteSpace(request.RangeStart) || string.IsNullOrWhiteSpace(request.RangeEnd))
            {
                throw new InvalidOperationException("range 模式必须提供 RangeStart 和 RangeEnd。");
            }
        }
    }
}

