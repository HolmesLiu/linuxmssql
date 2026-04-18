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
            throw new InvalidOperationException("Excel 中未识别到有效表名（A列需填写表名）。");
        }

        Console.WriteLine($"[daily-backup] 开始处理 {tables.Count} 个 Excel 表。");

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
            DateTime tableRunStartedUtc = DateTime.UtcNow;

            (string schemaName, string tableName) = ParseTableName(tableDef.TableName);
            string tableKey = $"{schemaName}.{tableName}";
            List<ColumnInfo> columns = await GetColumnsAsync(connection, schemaName, tableName, cancellationToken);
            if (columns.Count == 0)
            {
                Console.WriteLine($"[daily-backup] 跳过表 {tableKey}: 数据库中不存在或无可读取列。");
                continue;
            }

            Console.WriteLine($"[daily-backup] 开始处理表 {tableKey}。");

            state.Tables.TryGetValue(tableKey, out TableBackupState? tableState);
            tableState ??= new TableBackupState();

            string notes = string.Empty;
            string incrColumn;
            bool isFull;
            DataTable data;

            if (tableDef.TrackRowUpdates)
            {
                List<string> rowUpdateColumns = ResolveRowUpdateColumns(request, tableDef, tableState, columns, ref notes);
                incrColumn = string.Join(";", rowUpdateColumns);
                isFull = rowUpdateColumns.Count == 0 || tableState.LastBackupUtc == default;

                data = isFull
                    ? await LoadAllRowsAsync(connection, schemaName, tableName, cancellationToken)
                    : await LoadIncrementalRowsByTimeColumnsAsync(connection, schemaName, tableName, rowUpdateColumns, tableState.LastBackupUtc, cancellationToken);
            }
            else
            {
                incrColumn = ResolveIncrementalColumn(request, tableState, columns);
                isFull = string.IsNullOrWhiteSpace(incrColumn) || string.IsNullOrWhiteSpace(tableState.Watermark);

                data = isFull
                    ? await LoadAllRowsAsync(connection, schemaName, tableName, cancellationToken)
                    : await LoadIncrementalRowsAsync(connection, schemaName, tableName, incrColumn, tableState.Watermark, columns, request.FilterDataType, cancellationToken);
            }

            if (!string.IsNullOrWhiteSpace(notes))
            {
                Console.WriteLine($"[daily-backup] 表 {tableKey}: {notes}");
            }

            string modeTag = isFull ? "FULL" : "INCR";
            string prefix = $"{DateTime.Now:HHmmss}_{modeTag}_{schemaName}.{tableName}";
            if (data.Rows.Count > 0 || isFull)
            {
                fileCount += await WriteByFormatAsync(dayDirectory, prefix, schemaName, tableName, data, request.Format, cancellationToken);
                Console.WriteLine($"[daily-backup] 表 {tableKey} 已导出，模式 {(isFull ? "FULL" : "INCR")}，行数 {data.Rows.Count}。");
            }
            else
            {
                Console.WriteLine($"[daily-backup] 表 {tableKey} 本次无增量数据，未生成文件。");
            }

            if (tableDef.TrackRowUpdates)
            {
                tableState.Watermark = tableRunStartedUtc.ToString("O", CultureInfo.InvariantCulture);
            }
            else if (!string.IsNullOrWhiteSpace(incrColumn))
            {
                object? maxValue = await QueryMaxValueAsync(connection, schemaName, tableName, incrColumn, cancellationToken);
                if (maxValue is not null && maxValue != DBNull.Value)
                {
                    tableState.Watermark = FormatWatermark(maxValue);
                }
            }

            tableState.IncrementalColumn = incrColumn;
            tableState.LastBackupUtc = tableRunStartedUtc;
            tableState.LastMode = isFull ? "FULL" : "INCR";
            tableState.TrackRowUpdates = tableDef.TrackRowUpdates;
            state.Tables[tableKey] = tableState;

            if (isFull) fullCount++; else incrCount++;

            summaryRows.Add(new DailySummaryRow
            {
                TableName = $"{schemaName}.{tableName}",
                Description = tableDef.Description,
                Category = tableDef.Category,
                TrackRowUpdates = tableDef.TrackRowUpdates,
                RowUpdateColumn = tableDef.TrackRowUpdates ? incrColumn : tableDef.RowUpdateColumn,
                Mode = isFull ? "全量" : "增量",
                Notes = notes,
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

    private static List<string> ResolveRowUpdateColumns(DailyBackupRequest request, TableDefinition tableDefinition, TableBackupState tableState, IReadOnlyList<ColumnInfo> columns, ref string notes)
    {
        if (!string.IsNullOrWhiteSpace(tableDefinition.RowUpdateColumn))
        {
            ColumnInfo? explicitMatch = columns.FirstOrDefault(c => c.Name.Equals(tableDefinition.RowUpdateColumn, StringComparison.OrdinalIgnoreCase));
            if (explicitMatch is null)
            {
                notes = $"Excel 指定的存量更新时间字段 {tableDefinition.RowUpdateColumn} 在数据库中不存在，本次继续自动识别。";
            }
            else if (!IsTimeColumn(explicitMatch))
            {
                notes = $"Excel 指定的存量更新时间字段 {tableDefinition.RowUpdateColumn} 不是时间类型，本次继续自动识别。";
            }
            else
            {
                return [explicitMatch.Name];
            }
        }

        if (!string.IsNullOrWhiteSpace(request.IncrementalColumn))
        {
            ColumnInfo? globalMatch = columns.FirstOrDefault(c => c.Name.Equals(request.IncrementalColumn, StringComparison.OrdinalIgnoreCase) && IsTimeColumn(c));
            if (globalMatch is not null)
            {
                notes = $"已启用存量行更新，使用全局增量字段 {globalMatch.Name} 作为时间判断字段。";
                return [globalMatch.Name];
            }
        }

        List<string> allTimeColumns = columns
            .Where(IsTimeColumn)
            .Select(c => c.Name)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (allTimeColumns.Count > 0)
        {
            notes = $"已启用存量行更新，按上次采集时间匹配任意时间字段，使用字段: {string.Join(", ", allTimeColumns)}。";
            return allTimeColumns;
        }

        notes = "已启用存量行更新，但未识别到可用时间字段，本次退回全量导出。";
        return [];
    }
}
