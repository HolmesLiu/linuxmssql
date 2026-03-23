using System.ComponentModel;

namespace SqlServerExportTool.Models;

public sealed class TableSelectionItem : INotifyPropertyChanged
{
    private bool _isSelected;

    public TableSelectionItem(string displayName)
    {
        DisplayName = displayName;
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    public string DisplayName { get; }

    public bool IsSelected
    {
        get => _isSelected;
        set
        {
            if (_isSelected == value)
            {
                return;
            }

            _isSelected = value;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsSelected)));
        }
    }
}
