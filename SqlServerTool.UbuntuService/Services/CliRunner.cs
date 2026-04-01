using Microsoft.Extensions.DependencyInjection;
using SqlServerTool.UbuntuService.Models;

namespace SqlServerTool.UbuntuService.Services;

public static class CliRunner
{
    public static async Task<int> RunAsync(string[] args, IServiceProvider services, CancellationToken cancellationToken)
    {
        if (args.Length == 0)
        {
            PrintHelp();
            return 1;
        }

        SqlTransferService service = services.GetRequiredService<SqlTransferService>();
        Dictionary<string, string> options = ParseOptions(args.Skip(1).ToArray());

        try
        {
            switch (args[0].ToLowerInvariant())
            {
                case "export":
                {
                    ExportRequest request = BuildExportRequest(options);
                    ExportResult result = await service.ExportAsync(request, cancellationToken);
                    Console.WriteLine($"导出完成: {result.BatchDirectory}, 文件数: {result.FileCount}");
                    return 0;
                }
                case "import":
                {
                    ImportRequest request = BuildImportRequest(options);
                    ImportResult result = await service.ImportAsync(request, cancellationToken);
                    Console.WriteLine($"导入完成: 影响行数 {result.AffectedRows}");
                    return 0;
                }
                case "tables":
                {
                    string connectionString = GetRequired(options, "connection");
                    IReadOnlyList<string> tables = await service.GetTableNamesAsync(connectionString, cancellationToken);
                    foreach (string table in tables)
                    {
                        Console.WriteLine(table);
                    }

                    return 0;
                }
                case "daily-backup":
                {
                    DailyBackupRequest request = BuildDailyBackupRequest(options);
                    DailyBackupResult result = await service.DailyBackupFromExcelAsync(request, cancellationToken);
                    Console.WriteLine($"每日备份完成: {result.DayDirectory}, 全量表 {result.FullTableCount}, 增量表 {result.IncrementalTableCount}, 文件数 {result.CreatedFileCount}");
                    return 0;
                }
                default:
                    PrintHelp();
                    return 1;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"执行失败: {ex.Message}");
            return 2;
        }
    }

    private static ExportRequest BuildExportRequest(Dictionary<string, string> options)
    {
        string tablesRaw = GetOptional(options, "tables", string.Empty);
        return new ExportRequest
        {
            ConnectionString = GetRequired(options, "connection"),
            OutputDirectory = GetRequired(options, "output"),
            Format = GetOptional(options, "format", "sql"),
            Mode = GetOptional(options, "mode", "all"),
            FilterColumn = GetOptional(options, "filter-column", string.Empty),
            FilterDataType = GetOptional(options, "filter-type", "datetime"),
            LatestCount = int.TryParse(GetOptional(options, "latest-count", "1"), out int latestCount) ? latestCount : 1,
            RangeStart = GetOptional(options, "range-start", string.Empty),
            RangeEnd = GetOptional(options, "range-end", string.Empty),
            Tables = string.IsNullOrWhiteSpace(tablesRaw)
                ? []
                : tablesRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList()
        };
    }

    private static ImportRequest BuildImportRequest(Dictionary<string, string> options)
    {
        return new ImportRequest
        {
            ConnectionString = GetRequired(options, "connection"),
            InputPath = GetRequired(options, "input"),
            Format = GetOptional(options, "format", "sql"),
            TargetTable = GetOptional(options, "target-table", string.Empty)
        };
    }

    private static DailyBackupRequest BuildDailyBackupRequest(Dictionary<string, string> options)
    {
        return new DailyBackupRequest
        {
            ConnectionString = GetRequired(options, "connection"),
            ExcelPath = GetRequired(options, "excel"),
            OutputRootDirectory = GetRequired(options, "output-root"),
            SheetName = GetOptional(options, "sheet", "sheet1"),
            Format = GetOptional(options, "format", "json"),
            IncrementalColumn = GetOptional(options, "incremental-column", string.Empty),
            FilterDataType = GetOptional(options, "filter-type", "datetime")
        };
    }

    private static Dictionary<string, string> ParseOptions(string[] args)
    {
        Dictionary<string, string> options = new(StringComparer.OrdinalIgnoreCase);

        for (int index = 0; index < args.Length; index++)
        {
            string key = args[index];
            if (!key.StartsWith("--", StringComparison.Ordinal))
            {
                continue;
            }

            if (index + 1 >= args.Length)
            {
                throw new InvalidOperationException($"缺少参数值: {key}");
            }

            options[key[2..]] = args[index + 1];
            index++;
        }

        return options;
    }

    private static string GetRequired(Dictionary<string, string> options, string key)
    {
        if (!options.TryGetValue(key, out string? value) || string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException($"缺少必填参数: --{key}");
        }

        return value;
    }

    private static string GetOptional(Dictionary<string, string> options, string key, string defaultValue)
    {
        if (!options.TryGetValue(key, out string? value) || string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        return value;
    }

    private static void PrintHelp()
    {
        Console.WriteLine("用法:");
        Console.WriteLine("  export --connection <conn> --output <dir> [--format sql|json|csv] [--mode all|latest|range] [--tables dbo.A,dbo.B]");
        Console.WriteLine("         [--filter-column CreatedAt] [--latest-count 100] [--range-start 2026-01-01] [--range-end 2026-01-31] [--filter-type datetime|number|text]");
        Console.WriteLine("  import --connection <conn> --input <file-or-dir> [--format sql|json|csv] [--target-table dbo.A]");
        Console.WriteLine("  tables --connection <conn>");
        Console.WriteLine("  daily-backup --connection <conn> --excel <path.xlsx> --output-root <dir> [--sheet sheet1] [--format json|csv|sql]");
        Console.WriteLine("              [--incremental-column UpdateTime] [--filter-type datetime|number|text]");
    }
}