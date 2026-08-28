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
        if (MessageBox.Show($"确定删除 {profile.Name} 吗？这会删除本机保存的凭据；已有聊天绑定需先解除。", "删除 Channel Profile", MessageBoxButton.YesNo, MessageBoxImage.Warning) != MessageBoxResult.Yes) return;
        viewModel.DeleteProfileCommand.Execute(profile);
    }
}
