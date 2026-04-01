using SqlServerTool.UbuntuService.Models;
using SqlServerTool.UbuntuService.Services;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<SqlTransferService>();

var app = builder.Build();

if (args.Length > 0)
{
    int code = await CliRunner.RunAsync(args, app.Services, CancellationToken.None);
    Environment.ExitCode = code;
    return;
}

app.MapGet("/api/health", () => Results.Ok(new { ok = true, time = DateTimeOffset.UtcNow }));

app.MapPost("/api/export", async (ExportRequest request, SqlTransferService service, CancellationToken cancellationToken) =>
{
    try
    {
        ExportResult result = await service.ExportAsync(request, cancellationToken);
        return Results.Ok(result);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { message = ex.Message });
    }
});

app.MapPost("/api/import", async (ImportRequest request, SqlTransferService service, CancellationToken cancellationToken) =>
{
    try
    {
        ImportResult result = await service.ImportAsync(request, cancellationToken);
        return Results.Ok(result);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { message = ex.Message });
    }
});

app.MapPost("/api/tables", async (TableListRequest request, SqlTransferService service, CancellationToken cancellationToken) =>
{
    try
    {
        IReadOnlyList<string> tables = await service.GetTableNamesAsync(request.ConnectionString, cancellationToken);
        return Results.Ok(new { tables });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { message = ex.Message });
    }
});

app.MapPost("/api/daily-backup", async (DailyBackupRequest request, SqlTransferService service, CancellationToken cancellationToken) =>
{
    try
    {
        DailyBackupResult result = await service.DailyBackupFromExcelAsync(request, cancellationToken);
        return Results.Ok(result);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { message = ex.Message });
    }
});

app.Run();