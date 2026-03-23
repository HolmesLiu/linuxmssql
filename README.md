# SQL Server Export Tool

Windows 桌面工具，支持定时从 SQL Server 2012 导出指定表的表结构与数据。

## 功能

- 中文图形界面
- 表单式数据库连接配置
- 选择数据库与指定表
- 导出格式：`sql`、`json`、`csv`
- 导出模式：全部、最新、指定范围
- 定时执行与手动执行
- 自动生成带时间戳的导出目录与文件名

## 运行环境

- Windows
- .NET 8
- SQL Server 2012 或兼容版本

## 构建

```powershell
dotnet build .\SqlServerExportTool\SqlServerExportTool.csproj
```

## 发布单文件 exe

```powershell
dotnet publish .\SqlServerExportTool\SqlServerExportTool.csproj -c Release -r win-x64 --self-contained true /p:PublishSingleFile=true /p:IncludeNativeLibrariesForSelfExtract=true
```

## 项目结构

- `SqlServerExportTool/`：桌面程序源码
- `SqlServerExportTool/Models/`：配置与导出模型
- `SqlServerExportTool/Services/`：数据库访问、导出、配置持久化
