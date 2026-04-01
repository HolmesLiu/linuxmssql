namespace SqlServerTool.UbuntuService.Models;

public sealed class ExportRequest
{
    public required string ConnectionString { get; init; }

    public required string OutputDirectory { get; init; }

    public string Format { get; init; } = "sql";

    public string Mode { get; init; } = "all";

    public string FilterColumn { get; init; } = string.Empty;

    public string FilterDataType { get; init; } = "datetime";

    public int LatestCount { get; init; } = 1;

    public string RangeStart { get; init; } = string.Empty;

    public string RangeEnd { get; init; } = string.Empty;

    public IReadOnlyList<string> Tables { get; init; } = [];
}