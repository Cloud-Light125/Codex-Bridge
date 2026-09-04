using System.Diagnostics;
using System.Windows;
using System.Windows.Controls;
using CloudLight.CodexBridge.ViewModels;

namespace CloudLight.CodexBridge.Views;

public partial class ChannelProfilesView : UserControl
{
    public ChannelProfilesView() => InitializeComponent();

    private void SecretBox_PasswordChanged(object sender, RoutedEventArgs e)
    {
        if (sender is PasswordBox { DataContext: ChannelProfileViewModel profile } box) profile.SetPendingSecret(box.Password);
    }

    private void DeleteProfile_Click(object sender, RoutedEventArgs e)
    {
        if (DataContext is not ChannelProfilesViewModel viewModel || sender is not Button { Tag: ChannelProfileViewModel profile }) return;
        var platform = profile.IsQq ? "QQ" : "Telegram";
        var message = $"确定删除这个 {platform} 机器人吗？\n这会删除 Bridge 中的机器人配置和聊天关联，不会删除 {platform} 中的机器人。";
        if (MessageBox.Show(message, "删除机器人设置", MessageBoxButton.YesNo, MessageBoxImage.Warning) != MessageBoxResult.Yes) return;
        viewModel.DeleteProfileCommand.Execute(profile);
    }

    private void OpenOfficialUrl_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button { Tag: string url } || !IsOfficialUrl(url)) return;
        try
        {
            Process.Start(new ProcessStartInfo(url) { UseShellExecute = true });
        }
        catch
        {
            MessageBox.Show("无法打开官方页面，请稍后在浏览器中访问。", "打开官方页面", MessageBoxButton.OK, MessageBoxImage.Information);
        }
    }

    private static readonly HashSet<string> OfficialUrls = new(StringComparer.Ordinal)
    {
        "https://q.qq.com",
        "https://t.me/BotFather",
        "https://core.telegram.org/bots/tutorial"
    };

    internal static bool IsOfficialUrl(string? url) => !string.IsNullOrWhiteSpace(url) && OfficialUrls.Contains(url);
}
