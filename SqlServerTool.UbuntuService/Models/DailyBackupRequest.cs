namespace SqlServerTool.UbuntuService.Models;

public sealed class DailyBackupRequest
{
    public required string ConnectionString { get; init; }

    public required string ExcelPath { get; init; }

    public required string OutputRootDirectory { get; init; }

    public string SheetName { get; init; } = "sheet1";

    public string Format { get; init; } = "json";

    public string IncrementalColumn { get; init; } = string.Empty;

    public string FilterDataType { get; init; } = "datetime";
}