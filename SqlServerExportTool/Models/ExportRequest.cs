namespace SqlServerExportTool.Models;

public sealed class ExportRequest
{
    public required string Format { get; init; }

    public required string Mode { get; init; }

    public string FilterColumn { get; init; } = string.Empty;

    public int LatestCount { get; init; } = 1;

    public string RangeStart { get; init; } = string.Empty;

    public string RangeEnd { get; init; } = string.Empty;

    public string FilterDataType { get; init; } = "datetime";
}
