namespace SqlServerTool.UbuntuService.Models;

public sealed class DailyBackupResult
{
    public required string DayDirectory { get; init; }

    public required int FullTableCount { get; init; }

    public required int IncrementalTableCount { get; init; }

    public required int CreatedFileCount { get; init; }
}