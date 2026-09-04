using System.Windows;
using System.Windows.Controls;
using WpfPanel = System.Windows.Controls.Panel;
using WpfRect = System.Windows.Rect;
using WpfSize = System.Windows.Size;

namespace CloudLight.CodexBridge.Controls;

/// <summary>
/// Wraps cards using the actual width offered by the page.  It deliberately
/// keeps one visual tree for narrow and wide windows so controls do not get
/// duplicated or squeezed into unusable fixed columns.
/// </summary>
public sealed class ResponsiveCardPanel : WpfPanel
{
    public static readonly DependencyProperty MinItemWidthProperty =
        DependencyProperty.Register(nameof(MinItemWidth), typeof(double), typeof(ResponsiveCardPanel),
            new FrameworkPropertyMetadata(240d, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public static readonly DependencyProperty MaxItemWidthProperty =
        DependencyProperty.Register(nameof(MaxItemWidth), typeof(double), typeof(ResponsiveCardPanel),
            new FrameworkPropertyMetadata(0d, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public static readonly DependencyProperty MaxColumnsProperty =
        DependencyProperty.Register(nameof(MaxColumns), typeof(int), typeof(ResponsiveCardPanel),
            new FrameworkPropertyMetadata(4, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public static readonly DependencyProperty HorizontalSpacingProperty =
        DependencyProperty.Register(nameof(HorizontalSpacing), typeof(double), typeof(ResponsiveCardPanel),
            new FrameworkPropertyMetadata(16d, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public static readonly DependencyProperty VerticalSpacingProperty =
        DependencyProperty.Register(nameof(VerticalSpacing), typeof(double), typeof(ResponsiveCardPanel),
            new FrameworkPropertyMetadata(16d, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public double MinItemWidth
    {
        get => (double)GetValue(MinItemWidthProperty);
        set => SetValue(MinItemWidthProperty, value);
    }

    public double MaxItemWidth
    {
        get => (double)GetValue(MaxItemWidthProperty);
        set => SetValue(MaxItemWidthProperty, value);
    }

    public int MaxColumns
    {
        get => (int)GetValue(MaxColumnsProperty);
        set => SetValue(MaxColumnsProperty, value);
    }

    public double HorizontalSpacing
    {
        get => (double)GetValue(HorizontalSpacingProperty);
        set => SetValue(HorizontalSpacingProperty, value);
    }

    public double VerticalSpacing
    {
        get => (double)GetValue(VerticalSpacingProperty);
        set => SetValue(VerticalSpacingProperty, value);
    }

    protected override WpfSize MeasureOverride(WpfSize availableSize)
    {
        var width = ResolveWidth(availableSize.Width);
        var columns = CalculateColumnCount(width, Children.Count, MinItemWidth, MaxItemWidth, MaxColumns, HorizontalSpacing);
        var itemWidth = ItemWidth(width, columns, HorizontalSpacing);
        var availableHeight = double.IsFinite(availableSize.Height) ? Math.Max(0, availableSize.Height) : double.PositiveInfinity;
        var rowHeights = new List<double>();

        for (var index = 0; index < Children.Count; index++)
        {
            var child = Children[index];
            child.Measure(new WpfSize(itemWidth, availableHeight));
            var row = index / columns;
            while (rowHeights.Count <= row) rowHeights.Add(0);
            rowHeights[row] = Math.Max(rowHeights[row], SafeLength(child.DesiredSize.Height));
        }

        var height = rowHeights.Count == 0
            ? 0
            : rowHeights.Sum() + Math.Max(0, rowHeights.Count - 1) * SafeLength(VerticalSpacing);
        return new WpfSize(width, SafeLength(height));
    }

    protected override WpfSize ArrangeOverride(WpfSize finalSize)
    {
        var width = ResolveWidth(finalSize.Width);
        var columns = CalculateColumnCount(width, Children.Count, MinItemWidth, MaxItemWidth, MaxColumns, HorizontalSpacing);
        var itemWidth = ItemWidth(width, columns, HorizontalSpacing);
        var rowHeights = new List<double>();

        for (var index = 0; index < Children.Count; index++)
        {
            var row = index / columns;
            while (rowHeights.Count <= row) rowHeights.Add(0);
            rowHeights[row] = Math.Max(rowHeights[row], SafeLength(Children[index].DesiredSize.Height));
        }

        var rowTop = 0d;
        for (var index = 0; index < Children.Count; index++)
        {
            var row = index / columns;
            var column = index % columns;
            var x = column * (itemWidth + SafeLength(HorizontalSpacing));
            var y = rowTop;
            Children[index].Arrange(new WpfRect(SafeLength(x), SafeLength(y), itemWidth, SafeLength(rowHeights[row])));
            if (column == columns - 1 || index == Children.Count - 1)
                rowTop += rowHeights[row] + SafeLength(VerticalSpacing);
        }

        return new WpfSize(width, SafeLength(Math.Max(0, rowTop - (rowHeights.Count == 0 ? 0 : VerticalSpacing))));
    }

    internal static int CalculateColumnCount(double availableWidth, int itemCount, double minItemWidth, double maxItemWidth, int maxColumns, double spacing)
    {
        if (itemCount <= 0) return 1;
        var width = SafeLength(availableWidth);
        var minimum = Math.Max(1, SafeLength(minItemWidth));
        var gap = SafeLength(spacing);
        var limit = Math.Max(1, Math.Min(itemCount, maxColumns));
        var columns = Math.Max(1, Math.Min(limit, (int)Math.Floor((width + gap) / (minimum + gap))));
        if (maxItemWidth > 0 && double.IsFinite(maxItemWidth))
        {
            while (columns < limit && ItemWidth(width, columns, gap) > maxItemWidth)
                columns++;
        }
        return columns;
    }

    private static double ItemWidth(double width, int columns, double spacing)
    {
        var safeColumns = Math.Max(1, columns);
        return SafeLength(Math.Max(0, (SafeLength(width) - Math.Max(0, safeColumns - 1) * SafeLength(spacing)) / safeColumns));
    }

    private double ResolveWidth(double value)
    {
        if (double.IsFinite(value)) return Math.Max(0, value);
        var count = Math.Max(1, Math.Min(Math.Max(1, Children.Count), Math.Max(1, MaxColumns)));
        var minimum = Math.Max(1, SafeLength(MinItemWidth));
        return minimum * count + SafeLength(HorizontalSpacing) * Math.Max(0, count - 1);
    }

    private static double SafeLength(double value) => double.IsFinite(value) ? Math.Max(0, value) : 0;
}
