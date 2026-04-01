namespace SqlServerTool.UbuntuService.Models;

public sealed class ImportRequest
{
    public required string ConnectionString { get; init; }

    public required string InputPath { get; init; }

    public string Format { get; init; } = "sql";

    public string TargetTable { get; init; } = string.Empty;
}