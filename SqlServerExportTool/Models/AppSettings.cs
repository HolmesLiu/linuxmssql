namespace SqlServerExportTool.Models;

public sealed class AppSettings
{
    public string ServerHost { get; set; } = string.Empty;

    public string ServerPort { get; set; } = "1433";

    public string UserName { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;

    public string SelectedDatabase { get; set; } = string.Empty;

    public string ExportFormat { get; set; } = "sql";

    public string ExportMode { get; set; } = "all";

    public string FilterColumn { get; set; } = string.Empty;

    public int LatestCount { get; set; } = 1;

    public string RangeStart { get; set; } = string.Empty;

    public string RangeEnd { get; set; } = string.Empty;

    public string FilterDataType { get; set; } = "datetime";

    public string OutputDirectory { get; set; } = string.Empty;

    public int IntervalMinutes { get; set; } = 60;

    public bool AutoStartScheduler { get; set; }

    public List<string> SelectedTables { get; set; } = [];
}
