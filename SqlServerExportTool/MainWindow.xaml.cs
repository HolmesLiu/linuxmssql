using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using Microsoft.Data.SqlClient;
using SqlServerExportTool.Models;
using SqlServerExportTool.Services;
using Forms = System.Windows.Forms;

namespace SqlServerExportTool;

public partial class MainWindow : Window, INotifyPropertyChanged
{
    private readonly SettingsService _settingsService = new();
    private readonly SqlExportService _sqlExportService = new();
    private readonly DispatcherTimer _scheduler = new();
    private bool _isExporting;
    private string _serverHost = string.Empty;
    private string _serverPort = "1433";
    private string _userName = string.Empty;
    private string _password = string.Empty;
    private string _selectedDatabase = string.Empty;
    private string _selectedExportFormat = "sql";
    private string _selectedExportMode = "all";
    private string _selectedFilterDataType = "datetime";
    private string _filterColumn = string.Empty;
    private string _latestRowCountText = "1";
    private string _rangeStart = string.Empty;
    private string _rangeEnd = string.Empty;
    private string _outputDirectory = string.Empty;
    private string _intervalMinutesText = "60";
    private string _statusMessage = "等待配置。";
    private string _nextRunDisplay = "未启动";
    private bool _autoStartScheduler;

    public MainWindow()
    {
        InitializeComponent();
        DataContext = this;

        ExportFormats.Add("sql");
        ExportFormats.Add("json");
        ExportFormats.Add("csv");

        ExportModes.Add("all");
        ExportModes.Add("latest");
        ExportModes.Add("range");

        FilterDataTypes.Add("datetime");
        FilterDataTypes.Add("number");
        FilterDataTypes.Add("text");

        _scheduler.Tick += Scheduler_Tick;
        Loaded += MainWindow_Loaded;
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    public ObservableCollection<string> Databases { get; } = [];

    public ObservableCollection<string> ExportFormats { get; } = [];

    public ObservableCollection<string> ExportModes { get; } = [];

    public ObservableCollection<string> FilterDataTypes { get; } = [];

    public ObservableCollection<TableSelectionItem> Tables { get; } = [];

    public ObservableCollection<string> Logs { get; } = [];

    public string ServerHost
    {
        get => _serverHost;
        set => SetField(ref _serverHost, value);
    }

    public string ServerPort
    {
        get => _serverPort;
        set => SetField(ref _serverPort, value);
    }

    public string UserName
    {
        get => _userName;
        set => SetField(ref _userName, value);
    }

    public string Password
    {
        get => _password;
        set => SetField(ref _password, value);
    }

    public string SelectedDatabase
    {
        get => _selectedDatabase;
        set => SetField(ref _selectedDatabase, value);
    }

    public string SelectedExportFormat
    {
        get => _selectedExportFormat;
        set => SetField(ref _selectedExportFormat, value);
    }

    public string SelectedExportMode
    {
        get => _selectedExportMode;
        set => SetField(ref _selectedExportMode, value);
    }

    public string SelectedFilterDataType
    {
        get => _selectedFilterDataType;
        set => SetField(ref _selectedFilterDataType, value);
    }

    public string FilterColumn
    {
        get => _filterColumn;
        set => SetField(ref _filterColumn, value);
    }

    public string LatestRowCountText
    {
        get => _latestRowCountText;
        set => SetField(ref _latestRowCountText, value);
    }

    public string RangeStart
    {
        get => _rangeStart;
        set => SetField(ref _rangeStart, value);
    }

    public string RangeEnd
    {
        get => _rangeEnd;
        set => SetField(ref _rangeEnd, value);
    }

    public string OutputDirectory
    {
        get => _outputDirectory;
        set => SetField(ref _outputDirectory, value);
    }

    public string IntervalMinutesText
    {
        get => _intervalMinutesText;
        set => SetField(ref _intervalMinutesText, value);
    }

    public string StatusMessage
    {
        get => _statusMessage;
        set => SetField(ref _statusMessage, value);
    }

    public string NextRunDisplay
    {
        get => _nextRunDisplay;
        set => SetField(ref _nextRunDisplay, value);
    }

    public bool AutoStartScheduler
    {
        get => _autoStartScheduler;
        set => SetField(ref _autoStartScheduler, value);
    }

    private async void MainWindow_Loaded(object sender, RoutedEventArgs e)
    {
        await LoadSettingsAsync();

        if (AutoStartScheduler && TryGetIntervalMinutes(out _))
        {
            StartScheduler();
        }
    }

    private async Task LoadSettingsAsync()
    {
        AppSettings settings = await _settingsService.LoadAsync();

        ServerHost = settings.ServerHost;
        ServerPort = string.IsNullOrWhiteSpace(settings.ServerPort) ? "1433" : settings.ServerPort;
        UserName = settings.UserName;
        Password = settings.Password;
        PasswordInput.Password = settings.Password;
        SelectedDatabase = settings.SelectedDatabase;
        SelectedExportFormat = settings.ExportFormat;
        SelectedExportMode = settings.ExportMode;
        SelectedFilterDataType = settings.FilterDataType;
        FilterColumn = settings.FilterColumn;
        LatestRowCountText = settings.LatestCount.ToString();
        RangeStart = settings.RangeStart;
        RangeEnd = settings.RangeEnd;
        OutputDirectory = settings.OutputDirectory;
        IntervalMinutesText = settings.IntervalMinutes.ToString();
        AutoStartScheduler = settings.AutoStartScheduler;

        if (!string.IsNullOrWhiteSpace(ServerHost) && !string.IsNullOrWhiteSpace(UserName))
        {
            try
            {
                await LoadDatabasesAsync();
            }
            catch
            {
                AddLog("未能根据已保存配置加载数据库列表。");
            }
        }

        if (!string.IsNullOrWhiteSpace(SelectedDatabase))
        {
            await LoadTablesAsync(settings.SelectedTables);
        }

        AddLog("本地配置已加载。");
    }

    private async Task SaveSettingsAsync()
    {
        AppSettings settings = new()
        {
            ServerHost = ServerHost.Trim(),
            ServerPort = ServerPort.Trim(),
            UserName = UserName.Trim(),
            Password = Password,
            SelectedDatabase = SelectedDatabase,
            ExportFormat = SelectedExportFormat,
            ExportMode = SelectedExportMode,
            FilterColumn = FilterColumn.Trim(),
            LatestCount = TryGetLatestCount(out int latestCount) ? latestCount : 1,
            RangeStart = RangeStart.Trim(),
            RangeEnd = RangeEnd.Trim(),
            FilterDataType = SelectedFilterDataType,
            OutputDirectory = OutputDirectory.Trim(),
            IntervalMinutes = TryGetIntervalMinutes(out int minutes) ? minutes : 60,
            AutoStartScheduler = AutoStartScheduler,
            SelectedTables = Tables.Where(x => x.IsSelected).Select(x => x.DisplayName).ToList()
        };

        await _settingsService.SaveAsync(settings);
        AddLog("配置已保存。");
    }

    private async void LoadDatabases_Click(object sender, RoutedEventArgs e)
    {
        await LoadDatabasesAsync();
    }

    private async Task LoadDatabasesAsync()
    {
        if (!ValidateServerFields(requireDatabase: false))
        {
            return;
        }

        try
        {
            IReadOnlyList<string> names = await _sqlExportService.GetDatabaseNamesAsync(BuildMasterConnectionString());
            Databases.Clear();
            foreach (string name in names)
            {
                Databases.Add(name);
            }

            if (Databases.Count > 0 &&
                (string.IsNullOrWhiteSpace(SelectedDatabase) || !Databases.Contains(SelectedDatabase)))
            {
                SelectedDatabase = Databases[0];
            }

            SetStatus($"已加载 {Databases.Count} 个数据库。");
            AddLog($"数据库列表已加载，共 {Databases.Count} 项。");
        }
        catch (Exception ex)
        {
            SetStatus($"加载数据库失败：{ex.Message}");
            AddLog($"加载数据库失败：{ex.Message}");
        }
    }

    private async void TestConnection_Click(object sender, RoutedEventArgs e)
    {
        if (!ValidateServerFields(requireDatabase: true))
        {
            return;
        }

        try
        {
            await _sqlExportService.TestConnectionAsync(BuildDatabaseConnectionString());
            SetStatus("数据库连接成功。");
            AddLog("连接测试成功。");
        }
        catch (Exception ex)
        {
            SetStatus($"数据库连接失败：{ex.Message}");
            AddLog($"连接测试失败：{ex.Message}");
        }
    }

    private async void LoadTables_Click(object sender, RoutedEventArgs e)
    {
        await LoadTablesAsync();
    }

    private async Task LoadTablesAsync(IEnumerable<string>? selectedTables = null)
    {
        if (!ValidateServerFields(requireDatabase: true))
        {
            return;
        }

        try
        {
            IReadOnlyList<string> tableNames = await _sqlExportService.GetTableNamesAsync(BuildDatabaseConnectionString());
            HashSet<string> selectedLookup = selectedTables is null
                ? Tables.Where(x => x.IsSelected).Select(x => x.DisplayName).ToHashSet(StringComparer.OrdinalIgnoreCase)
                : selectedTables.ToHashSet(StringComparer.OrdinalIgnoreCase);

            Tables.Clear();
            foreach (string tableName in tableNames)
            {
                Tables.Add(new TableSelectionItem(tableName) { IsSelected = selectedLookup.Contains(tableName) });
            }

            SetStatus($"已加载 {tableNames.Count} 个表。");
            AddLog($"表列表已刷新，共 {tableNames.Count} 个表。");
        }
        catch (Exception ex)
        {
            SetStatus($"加载表失败：{ex.Message}");
            AddLog($"加载表失败：{ex.Message}");
        }
    }

    private void BrowseDirectory_Click(object sender, RoutedEventArgs e)
    {
        using Forms.FolderBrowserDialog dialog = new()
        {
            Description = "选择导出文件保存目录",
            UseDescriptionForTitle = true,
            InitialDirectory = Directory.Exists(OutputDirectory)
                ? OutputDirectory
                : Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory)
        };

        if (dialog.ShowDialog() == Forms.DialogResult.OK)
        {
            OutputDirectory = dialog.SelectedPath;
        }
    }

    private async void SaveSettings_Click(object sender, RoutedEventArgs e)
    {
        await SaveSettingsAsync();
        SetStatus("配置已保存。");
    }

    private async void ExportNow_Click(object sender, RoutedEventArgs e)
    {
        await RunExportAsync("手动");
    }

    private void StartSchedule_Click(object sender, RoutedEventArgs e)
    {
        StartScheduler();
    }

    private void StopSchedule_Click(object sender, RoutedEventArgs e)
    {
        StopScheduler();
    }

    private void SelectAllTables_Click(object sender, RoutedEventArgs e)
    {
        foreach (TableSelectionItem item in Tables)
        {
            item.IsSelected = true;
        }

        SetStatus("已全选所有表。");
    }

    private void ClearSelectedTables_Click(object sender, RoutedEventArgs e)
    {
        foreach (TableSelectionItem item in Tables)
        {
            item.IsSelected = false;
        }

        SetStatus("已清空表选择。");
    }

    private async void Scheduler_Tick(object? sender, EventArgs e)
    {
        await RunExportAsync("定时");
        if (_scheduler.IsEnabled)
        {
            ScheduleNextRun();
        }
    }

    private void StartScheduler()
    {
        if (!TryGetIntervalMinutes(out int minutes))
        {
            SetStatus("执行周期必须是大于 0 的整数。");
            return;
        }

        _scheduler.Interval = TimeSpan.FromMinutes(minutes);
        _scheduler.Start();
        ScheduleNextRun();
        SetStatus("定时任务已启动。");
        AddLog($"定时器已启动，周期为 {minutes} 分钟。");
    }

    private void StopScheduler()
    {
        _scheduler.Stop();
        NextRunDisplay = "未启动";
        SetStatus("定时任务已停止。");
        AddLog("定时器已停止。");
    }

    private async Task RunExportAsync(string trigger)
    {
        if (_isExporting)
        {
            AddLog($"{trigger}导出被跳过：上一轮任务仍在执行。");
            return;
        }

        if (!ValidateServerFields(requireDatabase: true) || !ValidateExportSettings())
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(OutputDirectory))
        {
            SetStatus("请选择导出目录。");
            return;
        }

        List<string> selectedTables = Tables.Where(x => x.IsSelected).Select(x => x.DisplayName).ToList();
        if (selectedTables.Count == 0)
        {
            SetStatus("请至少选择一个表。");
            return;
        }

        _isExporting = true;
        SetStatus($"{trigger}导出开始，共 {selectedTables.Count} 个表。");
        AddLog($"{trigger}导出开始。");

        try
        {
            await SaveSettingsAsync();
            ExportBatchResult result = await _sqlExportService.ExportTablesAsync(
                BuildDatabaseConnectionString(),
                OutputDirectory.Trim(),
                selectedTables,
                BuildExportRequest(),
                CancellationToken.None);

            SetStatus($"导出完成：{result.BatchDirectory}");
            AddLog($"导出完成，共生成 {result.ExportedFileCount} 个文件，目录：{result.BatchDirectory}");
        }
        catch (Exception ex)
        {
            SetStatus($"导出失败：{ex.Message}");
            AddLog($"导出失败：{ex}");
        }
        finally
        {
            _isExporting = false;
        }
    }

    private void PasswordBox_OnPasswordChanged(object sender, RoutedEventArgs e)
    {
        Password = ((PasswordBox)sender).Password;
    }

    private void ScheduleNextRun()
    {
        NextRunDisplay = DateTime.Now.Add(_scheduler.Interval).ToString("yyyy-MM-dd HH:mm:ss");
    }

    private bool TryGetIntervalMinutes(out int minutes)
    {
        if (int.TryParse(IntervalMinutesText, out minutes) && minutes > 0)
        {
            return true;
        }

        minutes = 0;
        return false;
    }

    private bool TryGetLatestCount(out int latestCount)
    {
        if (int.TryParse(LatestRowCountText, out latestCount) && latestCount > 0)
        {
            return true;
        }

        latestCount = 0;
        return false;
    }

    private bool ValidateServerFields(bool requireDatabase)
    {
        if (string.IsNullOrWhiteSpace(ServerHost))
        {
            SetStatus("请输入服务器 IP 或主机名。");
            return false;
        }

        if (string.IsNullOrWhiteSpace(ServerPort))
        {
            SetStatus("请输入 SQL Server 端口。");
            return false;
        }

        if (!int.TryParse(ServerPort, out int port) || port <= 0 || port > 65535)
        {
            SetStatus("端口必须是 1 到 65535 之间的整数。");
            return false;
        }

        if (string.IsNullOrWhiteSpace(UserName))
        {
            SetStatus("请输入登录账号。");
            return false;
        }

        if (requireDatabase && string.IsNullOrWhiteSpace(SelectedDatabase))
        {
            SetStatus("请先加载并选择数据库。");
            return false;
        }

        return true;
    }

    private bool ValidateExportSettings()
    {
        if (SelectedExportMode == "latest")
        {
            if (string.IsNullOrWhiteSpace(FilterColumn))
            {
                SetStatus("“导出最新”模式必须填写筛选 / 排序字段。");
                return false;
            }

            if (!TryGetLatestCount(out _))
            {
                SetStatus("最新记录数量必须是大于 0 的整数。");
                return false;
            }
        }

        if (SelectedExportMode == "range")
        {
            if (string.IsNullOrWhiteSpace(FilterColumn))
            {
                SetStatus("“指定范围”模式必须填写筛选字段。");
                return false;
            }

            if (string.IsNullOrWhiteSpace(RangeStart) || string.IsNullOrWhiteSpace(RangeEnd))
            {
                SetStatus("“指定范围”模式必须同时填写开始值和结束值。");
                return false;
            }
        }

        return true;
    }

    private ExportRequest BuildExportRequest()
    {
        return new ExportRequest
        {
            Format = SelectedExportFormat,
            Mode = SelectedExportMode,
            FilterColumn = FilterColumn.Trim(),
            LatestCount = TryGetLatestCount(out int latestCount) ? latestCount : 1,
            RangeStart = RangeStart.Trim(),
            RangeEnd = RangeEnd.Trim(),
            FilterDataType = SelectedFilterDataType
        };
    }

    private string BuildMasterConnectionString()
    {
        return BuildConnectionString("master");
    }

    private string BuildDatabaseConnectionString()
    {
        return BuildConnectionString(SelectedDatabase);
    }

    private string BuildConnectionString(string databaseName)
    {
        SqlConnectionStringBuilder builder = new()
        {
            DataSource = $"{ServerHost.Trim()},{ServerPort.Trim()}",
            InitialCatalog = databaseName,
            UserID = UserName.Trim(),
            Password = Password,
            Encrypt = false,
            TrustServerCertificate = true,
            IntegratedSecurity = false,
            ConnectTimeout = 15
        };

        return builder.ConnectionString;
    }

    private void SetStatus(string message)
    {
        StatusMessage = message;
    }

    private void AddLog(string message)
    {
        Logs.Insert(0, $"[{DateTime.Now:HH:mm:ss}] {message}");
        while (Logs.Count > 200)
        {
            Logs.RemoveAt(Logs.Count - 1);
        }
    }

    private void Window_Closing(object? sender, CancelEventArgs e)
    {
        _scheduler.Stop();
        _ = SaveSettingsAsync();
    }

    private void SetField<T>(ref T field, T value, [CallerMemberName] string? propertyName = null)
    {
        if (EqualityComparer<T>.Default.Equals(field, value))
        {
            return;
        }

        field = value;
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}
