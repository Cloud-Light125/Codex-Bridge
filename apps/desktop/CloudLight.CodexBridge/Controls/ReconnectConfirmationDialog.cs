using System.Windows;
using System.Windows.Controls;

namespace CloudLight.CodexBridge.Controls;

internal sealed class ReconnectConfirmationDialog : Window
{
    private ReconnectConfirmationDialog(string platformLabel)
    {
        Title = "需要重新连接机器人";
        Width = 520;
        SizeToContent = SizeToContent.Height;
        ResizeMode = ResizeMode.NoResize;
        ShowInTaskbar = false;
        WindowStartupLocation = WindowStartupLocation.CenterOwner;
        SetResourceReference(BackgroundProperty, "SurfaceBrush");
        SetResourceReference(ForegroundProperty, "PrimaryTextBrush");

        var reconnect = new Button { Content = "保存并重新连接", IsDefault = true, MinWidth = 128 };
        reconnect.SetResourceReference(FrameworkElement.StyleProperty, "PrimaryButtonStyle");
        reconnect.Click += (_, _) => DialogResult = true;
        var cancel = new Button { Content = "取消", IsCancel = true, MinWidth = 80 };
        cancel.Click += (_, _) => DialogResult = false;

        var buttons = new StackPanel { Orientation = System.Windows.Controls.Orientation.Horizontal, HorizontalAlignment = System.Windows.HorizontalAlignment.Right };
        buttons.Children.Add(cancel);
        buttons.Children.Add(reconnect);
        var content = new StackPanel();
        content.Children.Add(new TextBlock { Text = "需要重新连接机器人", FontSize = 19, FontWeight = FontWeights.SemiBold });
        content.Children.Add(new TextBlock
        {
            Text = "机器人 ID 或密钥已修改，需要重新连接后才能生效。是否保存并立即重新连接？",
            TextWrapping = TextWrapping.Wrap,
            Margin = new Thickness(0, 10, 0, 22)
        });
        content.Children.Add(buttons);
        Content = new Border { Padding = new Thickness(24), Child = content };
    }

    public static bool Show(string platformLabel)
    {
        var dialog = new ReconnectConfirmationDialog(platformLabel)
        {
            Owner = Application.Current?.MainWindow
        };
        return dialog.ShowDialog() == true;
    }
}
