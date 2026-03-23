namespace SqlServerExportTool.Models;

public sealed class ExportBatchResult
{
    public required string BatchDirectory { get; init; }

    public required int ExportedFileCount { get; init; }
}
