using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using SqlServerExportTool.Models;

namespace SqlServerExportTool.Services;

public sealed class SqlExportService
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    public async Task<IReadOnlyList<string>> GetDatabaseNamesAsync(string connectionString)
    {
        const string sql = """
            SELECT name
            FROM sys.databases
            WHERE state = 0
              AND database_id > 4
            ORDER BY name;
            """;

        List<string> databases = [];

        using SqlConnection connection = new(connectionString);
        await connection.OpenAsync();
        using SqlCommand command = new(sql, connection);
        using SqlDataReader reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            databases.Add(reader.GetString(0));
        }

        return databases;
    }

    public async Task TestConnectionAsync(string connectionString)
    {
        using SqlConnection connection = new(connectionString);
        await connection.OpenAsync();
    }

    public async Task<IReadOnlyList<string>> GetTableNamesAsync(string connectionString)
    {
        const string sql = """
            SELECT s.name AS SchemaName, t.name AS TableName
            FROM sys.tables t
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            ORDER BY s.name, t.name;
            """;

        List<string> tables = [];

        using SqlConnection connection = new(connectionString);
        await connection.OpenAsync();
        using SqlCommand command = new(sql, connection);
        using SqlDataReader reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            tables.Add($"{reader.GetString(0)}.{reader.GetString(1)}");
        }

        return tables;
    }

    public async Task<ExportBatchResult> ExportTablesAsync(
        string connectionString,
        string outputDirectory,
        IReadOnlyList<string> tables,
        ExportRequest request,
        CancellationToken cancellationToken)
    {
        string batchDirectory = Path.Combine(outputDirectory, BuildBatchFolderName(request));
        Directory.CreateDirectory(batchDirectory);

        int exportedCount = 0;

        using SqlConnection connection = new(connectionString);
        await connection.OpenAsync(cancellationToken);

        foreach (string table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string[] parts = table.Split('.', 2);
            if (parts.Length != 2)
            {
                throw new InvalidOperationException($"Invalid table name format: {table}");
            }

            string schemaName = parts[0];
            string tableName = parts[1];

            List<ColumnDefinition> columns = await LoadColumnsAsync(connection, schemaName, tableName, cancellationToken);
            List<string> primaryKeys = await LoadPrimaryKeyColumnsAsync(connection, schemaName, tableName, cancellationToken);
            DataTable data = await LoadTableDataAsync(connection, schemaName, tableName, request, cancellationToken);

            exportedCount += await WriteTableFilesAsync(
                batchDirectory,
                schemaName,
                tableName,
                columns,
                primaryKeys,
                data,
                request,
                cancellationToken);
        }

        return new ExportBatchResult
        {
            BatchDirectory = batchDirectory,
            ExportedFileCount = exportedCount
        };
    }

    private async Task<int> WriteTableFilesAsync(
        string batchDirectory,
        string schemaName,
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<string> primaryKeys,
        DataTable data,
        ExportRequest request,
        CancellationToken cancellationToken)
    {
        string prefix = BuildFilePrefix(schemaName, tableName, request);

        switch (request.Format)
        {
            case "json":
            {
                string filePath = Path.Combine(batchDirectory, $"{prefix}.json");
                string json = BuildJsonPayload(schemaName, tableName, columns, data, request);
                await File.WriteAllTextAsync(filePath, json, new UTF8Encoding(false), cancellationToken);
                return 1;
            }
            case "csv":
            {
                string schemaPath = Path.Combine(batchDirectory, $"{prefix}.schema.json");
                string csvPath = Path.Combine(batchDirectory, $"{prefix}.data.csv");
                string schemaJson = BuildSchemaJson(schemaName, tableName, columns, primaryKeys, request);
                string csv = BuildCsv(data);
                await File.WriteAllTextAsync(schemaPath, schemaJson, new UTF8Encoding(false), cancellationToken);
                await File.WriteAllTextAsync(csvPath, csv, new UTF8Encoding(false), cancellationToken);
                return 2;
            }
            default:
            {
                string filePath = Path.Combine(batchDirectory, $"{prefix}.sql");
                string sql = BuildSqlScript(schemaName, tableName, columns, primaryKeys, data);
                await File.WriteAllTextAsync(filePath, sql, new UTF8Encoding(false), cancellationToken);
                return 1;
            }
        }
    }

    private async Task<List<ColumnDefinition>> LoadColumnsAsync(
        SqlConnection connection,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT
                c.column_id,
                c.name AS ColumnName,
                ty.name AS DataType,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE s.name = @schemaName AND t.name = @tableName
            ORDER BY c.column_id;
            """;

        List<ColumnDefinition> columns = [];
        using SqlCommand command = new(sql, connection);
        command.Parameters.AddWithValue("@schemaName", schemaName);
        command.Parameters.AddWithValue("@tableName", tableName);
        using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(new ColumnDefinition
            {
                ColumnName = reader.GetString(1),
                DataType = reader.GetString(2),
                MaxLength = Convert.ToInt16(reader.GetValue(3), CultureInfo.InvariantCulture),
                Precision = Convert.ToByte(reader.GetValue(4), CultureInfo.InvariantCulture),
                Scale = Convert.ToByte(reader.GetValue(5), CultureInfo.InvariantCulture),
                IsNullable = Convert.ToBoolean(reader.GetValue(6), CultureInfo.InvariantCulture),
                IsIdentity = Convert.ToBoolean(reader.GetValue(7), CultureInfo.InvariantCulture)
            });
        }

        return columns;
    }

    private async Task<List<string>> LoadPrimaryKeyColumnsAsync(
        SqlConnection connection,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT c.name
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE i.is_primary_key = 1 AND s.name = @schemaName AND t.name = @tableName
            ORDER BY ic.key_ordinal;
            """;

        List<string> keys = [];
        using SqlCommand command = new(sql, connection);
        command.Parameters.AddWithValue("@schemaName", schemaName);
        command.Parameters.AddWithValue("@tableName", tableName);
        using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            keys.Add(reader.GetString(0));
        }

        return keys;
    }

    private async Task<DataTable> LoadTableDataAsync(
        SqlConnection connection,
        string schemaName,
        string tableName,
        ExportRequest request,
        CancellationToken cancellationToken)
    {
        string qualifiedName = $"{EscapeIdentifier(schemaName)}.{EscapeIdentifier(tableName)}";

        using SqlCommand command = request.Mode switch
        {
            "latest" => BuildLatestCommand(connection, qualifiedName, request),
            "range" => BuildRangeCommand(connection, qualifiedName, request),
            _ => new SqlCommand($"SELECT * FROM {qualifiedName};", connection)
        };

        using SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
        DataTable table = new();
        table.Load(reader);
        return table;
    }

    private SqlCommand BuildLatestCommand(SqlConnection connection, string qualifiedName, ExportRequest request)
    {
        SqlCommand command = new(
            $"SELECT TOP (@TopCount) * FROM {qualifiedName} ORDER BY {EscapeIdentifier(request.FilterColumn)} DESC;",
            connection);
        command.Parameters.AddWithValue("@TopCount", request.LatestCount);
        return command;
    }

    private SqlCommand BuildRangeCommand(SqlConnection connection, string qualifiedName, ExportRequest request)
    {
        SqlCommand command = new(
            $"SELECT * FROM {qualifiedName} WHERE {EscapeIdentifier(request.FilterColumn)} >= @RangeStart AND {EscapeIdentifier(request.FilterColumn)} <= @RangeEnd ORDER BY {EscapeIdentifier(request.FilterColumn)} ASC;",
            connection);
        command.Parameters.Add("@RangeStart", SqlDbType.Variant).Value = ParseFilterValue(request.RangeStart, request.FilterDataType);
        command.Parameters.Add("@RangeEnd", SqlDbType.Variant).Value = ParseFilterValue(request.RangeEnd, request.FilterDataType);
        return command;
    }

    private static object ParseFilterValue(string value, string filterDataType)
    {
        return filterDataType switch
        {
            "number" => decimal.Parse(value, CultureInfo.InvariantCulture),
            "datetime" => DateTime.Parse(value, CultureInfo.InvariantCulture),
            _ => value
        };
    }

    private string BuildSqlScript(
        string schemaName,
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<string> primaryKeys,
        DataTable data)
    {
        StringBuilder builder = new();
        string qualifiedName = $"{EscapeIdentifier(schemaName)}.{EscapeIdentifier(tableName)}";

        builder.AppendLine($"-- Exported at {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        builder.AppendLine($"-- Table: {qualifiedName}");
        builder.AppendLine("SET NOCOUNT ON;");
        builder.AppendLine();
        builder.AppendLine($"IF OBJECT_ID(N'{schemaName}.{tableName}', N'U') IS NOT NULL");
        builder.AppendLine($"    DROP TABLE {qualifiedName};");
        builder.AppendLine("GO");
        builder.AppendLine();
        builder.AppendLine($"CREATE TABLE {qualifiedName}");
        builder.AppendLine("(");

        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition column = columns[i];
            string nullability = column.IsNullable ? "NULL" : "NOT NULL";
            string identity = column.IsIdentity ? " IDENTITY(1,1)" : string.Empty;
            string trailingComma = i == columns.Count - 1 && primaryKeys.Count == 0 ? string.Empty : ",";
            builder.AppendLine($"    {EscapeIdentifier(column.ColumnName)} {BuildSqlType(column)}{identity} {nullability}{trailingComma}");
        }

        if (primaryKeys.Count > 0)
        {
            builder.AppendLine($"    CONSTRAINT {EscapeIdentifier($"PK_{schemaName}_{tableName}")} PRIMARY KEY ({string.Join(", ", primaryKeys.Select(EscapeIdentifier))})");
        }

        builder.AppendLine(");");
        builder.AppendLine("GO");
        builder.AppendLine();
        builder.Append(BuildInsertStatements(schemaName, tableName, columns, data));
        return builder.ToString();
    }

    private string BuildInsertStatements(
        string schemaName,
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        DataTable data)
    {
        StringBuilder builder = new();
        string qualifiedName = $"{EscapeIdentifier(schemaName)}.{EscapeIdentifier(tableName)}";
        string columnList = string.Join(", ", columns.Select(x => EscapeIdentifier(x.ColumnName)));
        bool hasIdentityColumn = columns.Any(x => x.IsIdentity);

        if (hasIdentityColumn)
        {
            builder.AppendLine($"SET IDENTITY_INSERT {qualifiedName} ON;");
        }

        foreach (DataRow row in data.Rows)
        {
            List<string> values = [];

            foreach (ColumnDefinition column in columns)
            {
                values.Add(ToSqlLiteral(row[column.ColumnName]));
            }

            builder.AppendLine($"INSERT INTO {qualifiedName} ({columnList}) VALUES ({string.Join(", ", values)});");
        }

        if (data.Rows.Count == 0)
        {
            builder.AppendLine($"-- [{schemaName}].[{tableName}] has no rows.");
        }

        if (hasIdentityColumn)
        {
            builder.AppendLine($"SET IDENTITY_INSERT {qualifiedName} OFF;");
        }

        builder.AppendLine("GO");
        return builder.ToString();
    }

    private string BuildJsonPayload(
        string schemaName,
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        DataTable data,
        ExportRequest request)
    {
        object payload = new
        {
            table = $"{schemaName}.{tableName}",
            format = request.Format,
            mode = request.Mode,
            filterColumn = request.FilterColumn,
            filterDataType = request.FilterDataType,
            latestCount = request.LatestCount,
            rangeStart = request.RangeStart,
            rangeEnd = request.RangeEnd,
            schema = columns.Select(column => new
            {
                column.ColumnName,
                column.DataType,
                column.MaxLength,
                column.Precision,
                column.Scale,
                column.IsNullable,
                column.IsIdentity
            }),
            rows = data.Rows.Cast<DataRow>().Select(row => columns.ToDictionary(
                column => column.ColumnName,
                column => NormalizeValueForJson(row[column.ColumnName])))
        };

        return JsonSerializer.Serialize(payload, JsonOptions);
    }

    private string BuildSchemaJson(
        string schemaName,
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<string> primaryKeys,
        ExportRequest request)
    {
        object payload = new
        {
            table = $"{schemaName}.{tableName}",
            format = request.Format,
            mode = request.Mode,
            filterColumn = request.FilterColumn,
            filterDataType = request.FilterDataType,
            latestCount = request.LatestCount,
            rangeStart = request.RangeStart,
            rangeEnd = request.RangeEnd,
            primaryKeys,
            columns = columns.Select(column => new
            {
                column.ColumnName,
                column.DataType,
                column.MaxLength,
                column.Precision,
                column.Scale,
                column.IsNullable,
                column.IsIdentity
            })
        };

        return JsonSerializer.Serialize(payload, JsonOptions);
    }

    private string BuildCsv(DataTable data)
    {
        StringBuilder builder = new();

        IEnumerable<string> headers = data.Columns.Cast<DataColumn>().Select(column => EscapeCsv(column.ColumnName));
        builder.AppendLine(string.Join(",", headers));

        foreach (DataRow row in data.Rows)
        {
            IEnumerable<string> values = data.Columns.Cast<DataColumn>().Select(column => EscapeCsv(FormatCsvValue(row[column])));
            builder.AppendLine(string.Join(",", values));
        }

        return builder.ToString();
    }

    private static string EscapeCsv(string value)
    {
        string escaped = value.Replace("\"", "\"\"", StringComparison.Ordinal);
        return $"\"{escaped}\"";
    }

    private static string FormatCsvValue(object value)
    {
        if (value == DBNull.Value)
        {
            return string.Empty;
        }

        return value switch
        {
            DateTime dateTime => dateTime.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
            byte[] bytes => Convert.ToBase64String(bytes),
            bool boolean => boolean ? "1" : "0",
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
        };
    }

    private static object? NormalizeValueForJson(object value)
    {
        if (value == DBNull.Value)
        {
            return null;
        }

        return value switch
        {
            DateTime dateTime => dateTime.ToString("O", CultureInfo.InvariantCulture),
            byte[] bytes => Convert.ToBase64String(bytes),
            _ => value
        };
    }

    private static string BuildBatchFolderName(ExportRequest request)
    {
        return $"Export_{request.Format}_{request.Mode}_{DateTime.Now:yyyyMMdd_HHmmss}";
    }

    private static string BuildFilePrefix(string schemaName, string tableName, ExportRequest request)
    {
        string prefix = $"{schemaName}.{tableName}.{request.Format}.{request.Mode}";

        if (request.Mode == "latest")
        {
            prefix += $".{SanitizeFileSegment(request.FilterColumn)}.top{request.LatestCount}";
        }
        else if (request.Mode == "range")
        {
            prefix += $".{SanitizeFileSegment(request.FilterColumn)}.{SanitizeFileSegment(request.RangeStart)}_to_{SanitizeFileSegment(request.RangeEnd)}";
        }

        return prefix;
    }

    private static string SanitizeFileSegment(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "na";
        }

        char[] invalidChars = Path.GetInvalidFileNameChars();
        StringBuilder builder = new();

        foreach (char ch in value)
        {
            builder.Append(invalidChars.Contains(ch) || char.IsWhiteSpace(ch) ? '_' : ch);
        }

        return builder.ToString();
    }

    private static string BuildSqlType(ColumnDefinition column)
    {
        return column.DataType.ToLowerInvariant() switch
        {
            "nvarchar" or "nchar" => $"{column.DataType}({FormatUnicodeLength(column.MaxLength)})",
            "varchar" or "char" or "binary" or "varbinary" => $"{column.DataType}({FormatLength(column.MaxLength)})",
            "decimal" or "numeric" => $"{column.DataType}({column.Precision},{column.Scale})",
            "datetime2" or "datetimeoffset" or "time" => $"{column.DataType}({column.Scale})",
            _ => column.DataType
        };
    }

    private static string FormatLength(short maxLength)
    {
        return maxLength == -1 ? "MAX" : maxLength.ToString(CultureInfo.InvariantCulture);
    }

    private static string FormatUnicodeLength(short maxLength)
    {
        if (maxLength == -1)
        {
            return "MAX";
        }

        return (maxLength / 2).ToString(CultureInfo.InvariantCulture);
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

    private sealed class ColumnDefinition
    {
        public required string ColumnName { get; init; }

        public required string DataType { get; init; }

        public required short MaxLength { get; init; }

        public required byte Precision { get; init; }

        public required byte Scale { get; init; }

        public required bool IsNullable { get; init; }

        public required bool IsIdentity { get; init; }
    }
}
