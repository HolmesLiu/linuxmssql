namespace SqlServerTool.UbuntuService.Models;

public sealed class ExportResult
{
    public required string BatchDirectory { get; init; }

    public required int FileCount { get; init; }
}