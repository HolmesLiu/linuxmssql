using System.Data;
using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using SqlServerTool.UbuntuService.Models;

namespace SqlServerTool.UbuntuService.Services;

public sealed partial class SqlTransferService
{
    private async Task<List<ColumnInfo>> GetColumnsAsync(SqlConnection connection, string schemaName, string tableName, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT c.name AS ColumnName, t.name AS SqlType, c.is_identity
            FROM sys.columns c
            INNER JOIN sys.tables tb ON tb.object_id = c.object_id
            INNER JOIN sys.schemas s ON s.schema_id = tb.schema_id
            INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE s.name = @schemaName AND tb.name = @tableName
            ORDER BY c.column_id;
            """;

        List<ColumnInfo> columns = [];
        await using SqlCommand cmd = new(sql, connection);
        cmd.Parameters.AddWithValue("@schemaName", schemaName);
        cmd.Parameters.AddWithValue("@tableName", tableName);
        await using SqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(new ColumnInfo
            {
                Name = reader.GetString(0),
                SqlType = reader.GetString(1),
                IsIdentity = reader.GetBoolean(2)
            });
        }

        return columns;
    }

    private async Task<DataTable> LoadAllRowsAsync(SqlConnection connection, string schemaName, string tableName, CancellationToken cancellationToken)
    {
        string qualified = $"{EscapeIdentifier(schemaName)}.{EscapeIdentifier(tableName)}";
        await using SqlCommand cmd = new($"SELECT * FROM {qualified};", connection);
        await using SqlDataReader reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
        DataTable table = new();
        table.Load(reader);
        return table;
    }

    private async Task<DataTable> LoadIncrementalRowsAsync(SqlConnection connection, string schemaName, string tableName, string incrColumn, string watermark, IReadOnlyList<ColumnInfo> columns, string fallbackType, CancellationToken cancellationToken)
    {
        string qualified = $"{EscapeIdentifier(schemaName)}.{EscapeIdentifier(tableName)}";
        await using SqlCommand cmd = new($"SELECT * FROM {qualified} WHERE {EscapeIdentifier(incrColumn)} > @watermark ORDER BY {EscapeIdentifier(incrColumn)} ASC;", connection);

        ColumnInfo? col = columns.FirstOrDefault(c => c.Name.Equals(incrColumn, StringComparison.OrdinalIgnoreCase));
        cmd.Parameters.AddWithValue("@watermark", ParseWatermark(watermark, col?.SqlType ?? string.Empty, fallbackType));

        await using SqlDataReader reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
        DataTable table = new();
        table.Load(reader);
        return table;
    }

    private static bool IsTimeColumn(ColumnInfo column)
    {
        string sqlType = column.SqlType.ToLowerInvariant();
        return sqlType.Contains("date", StringComparison.Ordinal) || sqlType.Contains("time", StringComparison.Ordinal);
    }

    private static object ParseWatermark(string watermark, string sqlType, string fallbackType)
    {
        string t = sqlType.ToLowerInvariant();
        if (t.Contains("date") || t.Contains("time")) return DateTime.Parse(watermark, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        if (t is "int" or "bigint" or "smallint" or "tinyint") return long.Parse(watermark, CultureInfo.InvariantCulture);
        if (t is "decimal" or "numeric" or "float" or "real" or "money" or "smallmoney") return decimal.Parse(watermark, CultureInfo.InvariantCulture);

        return fallbackType.ToLowerInvariant() switch
        {
            "datetime" => DateTime.Parse(watermark, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            "number" => decimal.Parse(watermark, CultureInfo.InvariantCulture),
            _ => watermark
        };
    }

    private async Task<object?> QueryMaxValueAsync(SqlConnection connection, string schemaName, string tableName, string columnName, CancellationToken cancellationToken)
    {
        string qualified = $"{EscapeIdentifier(schemaName)}.{EscapeIdentifier(tableName)}";
        await using SqlCommand cmd = new($"SELECT MAX({EscapeIdentifier(columnName)}) FROM {qualified};", connection);
        return await cmd.ExecuteScalarAsync(cancellationToken);
    }

    private async Task<int> WriteByFormatAsync(string outputDirectory, string filePrefix, string schemaName, string tableName, DataTable data, string format, CancellationToken cancellationToken)
    {
        string f = format.ToLowerInvariant();

        if (f == "json")
        {
            string path = Path.Combine(outputDirectory, $"{filePrefix}.json");
            ExportRequest req = new() { ConnectionString = string.Empty, OutputDirectory = string.Empty, Format = "json", Mode = "daily" };
            await File.WriteAllTextAsync(path, BuildJson(schemaName, tableName, data, req), new UTF8Encoding(false), cancellationToken);
            return 1;
        }

        if (f == "csv")
        {
            string path = Path.Combine(outputDirectory, $"{filePrefix}.csv");
            await File.WriteAllTextAsync(path, BuildCsv(data), new UTF8Encoding(false), cancellationToken);
            return 1;
        }

        string sqlPath = Path.Combine(outputDirectory, $"{filePrefix}.sql");
        await File.WriteAllTextAsync(sqlPath, BuildSqlInserts(schemaName, tableName, data), new UTF8Encoding(false), cancellationToken);
        return 1;
    }

    private sealed class ColumnInfo
    {
        public required string Name { get; init; }

        public required string SqlType { get; init; }

        public required bool IsIdentity { get; init; }
    }

    private sealed class DailyBackupState
    {
        public Dictionary<string, TableBackupState> Tables { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class TableBackupState
    {
        public string IncrementalColumn { get; set; } = string.Empty;

        public string Watermark { get; set; } = string.Empty;

        public DateTime LastBackupUtc { get; set; }

        public string LastMode { get; set; } = string.Empty;

        public bool TrackRowUpdates { get; set; }
    }
}
