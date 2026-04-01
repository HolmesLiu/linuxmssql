using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using SqlServerTool.UbuntuService.Models;

namespace SqlServerTool.UbuntuService.Services;

public sealed partial class SqlTransferService
{
    private const string DailyBackupStateFileName = ".daily_backup_state.json";

    public async Task<DailyBackupResult> DailyBackupFromExcelAsync(DailyBackupRequest request, CancellationToken cancellationToken)
    {
        ValidateDailyBackupRequest(request);

        List<TableDefinition> tables = LoadTablesFromExcel(request.ExcelPath, request.SheetName);
        if (tables.Count == 0)
        {
            throw new InvalidOperationException("Excel 中未识别到表名（A列需是 tb_ 开头）。");
        }

        Directory.CreateDirectory(request.OutputRootDirectory);
        string dayDirectory = Path.Combine(request.OutputRootDirectory, DateTime.Now.ToString("yyyy-MM-dd"));
        Directory.CreateDirectory(dayDirectory);

        string statePath = Path.Combine(request.OutputRootDirectory, DailyBackupStateFileName);
        DailyBackupState state = await LoadDailyBackupStateAsync(statePath, cancellationToken);

        int fullCount = 0;
        int incrCount = 0;
        int fileCount = 0;
        List<DailySummaryRow> summaryRows = [];

        await using SqlConnection connection = new(request.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        foreach (TableDefinition tableDef in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();

            (string schemaName, string tableName) = ParseTableName(tableDef.TableName);
            string tableKey = $"{schemaName}.{tableName}";
            List<ColumnInfo> columns = await GetColumnsAsync(connection, schemaName, tableName, cancellationToken);
            if (columns.Count == 0)
            {
                continue;
            }

            state.Tables.TryGetValue(tableKey, out TableBackupState? tableState);
            tableState ??= new TableBackupState();

            string incrColumn = ResolveIncrementalColumn(request, tableState, columns);
            bool isFull = string.IsNullOrWhiteSpace(incrColumn) || string.IsNullOrWhiteSpace(tableState.Watermark);

            DataTable data = isFull
                ? await LoadAllRowsAsync(connection, schemaName, tableName, cancellationToken)
                : await LoadIncrementalRowsAsync(connection, schemaName, tableName, incrColumn, tableState.Watermark, columns, request.FilterDataType, cancellationToken);

            string modeTag = isFull ? "FULL" : "INCR";
            string prefix = $"{DateTime.Now:HHmmss}_{modeTag}_{schemaName}.{tableName}";
            if (data.Rows.Count > 0 || isFull)
            {
                fileCount += await WriteByFormatAsync(dayDirectory, prefix, schemaName, tableName, data, request.Format, cancellationToken);
            }

            if (!string.IsNullOrWhiteSpace(incrColumn))
            {
                object? maxValue = await QueryMaxValueAsync(connection, schemaName, tableName, incrColumn, cancellationToken);
                if (maxValue is not null && maxValue != DBNull.Value)
                {
                    tableState.Watermark = FormatWatermark(maxValue);
                }
            }

            tableState.IncrementalColumn = incrColumn;
            tableState.LastBackupUtc = DateTime.UtcNow;
            tableState.LastMode = isFull ? "FULL" : "INCR";
            state.Tables[tableKey] = tableState;

            if (isFull) fullCount++; else incrCount++;

            summaryRows.Add(new DailySummaryRow
            {
                TableName = $"{schemaName}.{tableName}",
                Description = tableDef.Description,
                Category = tableDef.Category,
                Mode = isFull ? "全量" : "增量",
                RowCount = data.Rows.Count,
                IncrementalColumn = incrColumn,
                Watermark = tableState.Watermark ?? string.Empty,
                FilePrefix = prefix
            });
        }

        await SaveDailyBackupStateAsync(statePath, state, cancellationToken);
        string summaryPath = await WriteDailySummaryFileAsync(dayDirectory, summaryRows, cancellationToken);

        return new DailyBackupResult
        {
            DayDirectory = dayDirectory,
            SummaryFilePath = summaryPath,
            FullTableCount = fullCount,
            IncrementalTableCount = incrCount,
            CreatedFileCount = fileCount
        };
    }

    private static void ValidateDailyBackupRequest(DailyBackupRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.ConnectionString))
        {
            throw new InvalidOperationException("ConnectionString 不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.ExcelPath) || !File.Exists(request.ExcelPath))
        {
            throw new InvalidOperationException("ExcelPath 无效或文件不存在。");
        }

        if (string.IsNullOrWhiteSpace(request.OutputRootDirectory))
        {
            throw new InvalidOperationException("OutputRootDirectory 不能为空。");
        }

        string format = request.Format.ToLowerInvariant();
        if (format is not ("sql" or "json" or "csv"))
        {
            throw new InvalidOperationException("Format 仅支持 sql/json/csv。");
        }
    }

    private static string ResolveIncrementalColumn(DailyBackupRequest request, TableBackupState tableState, IReadOnlyList<ColumnInfo> columns)
    {
        if (!string.IsNullOrWhiteSpace(request.IncrementalColumn) && columns.Any(c => c.Name.Equals(request.IncrementalColumn, StringComparison.OrdinalIgnoreCase)))
        {
            return request.IncrementalColumn;
        }

        if (!string.IsNullOrWhiteSpace(tableState.IncrementalColumn) && columns.Any(c => c.Name.Equals(tableState.IncrementalColumn, StringComparison.OrdinalIgnoreCase)))
        {
            return tableState.IncrementalColumn;
        }

        string[] candidates = ["UpdateTime", "UpdatedAt", "ModifyTime", "ModifiedAt", "CreateTime", "CreatedAt", "AddTime", "InputTime", "TestTime", "TestDate", "CreateDate"];
        foreach (string candidate in candidates)
        {
            ColumnInfo? match = columns.FirstOrDefault(c => c.Name.Equals(candidate, StringComparison.OrdinalIgnoreCase));
            if (match is not null)
            {
                return match.Name;
            }
        }

        return columns.FirstOrDefault(c => c.IsIdentity)?.Name ?? string.Empty;
    }
}
