namespace CloudLight.CodexBridge.Views;

public partial class SettingsView : UserControl
{
    public SettingsView() => InitializeComponent();

    private void OpenClawTokenBox_PasswordChanged(object sender, RoutedEventArgs e)
    {
        if (DataContext is ViewModels.SettingsViewModel settings) settings.SetOpenClawToken(OpenClawTokenBox.Password);
    }

    private void OpenClawPasswordBox_PasswordChanged(object sender, RoutedEventArgs e)
    {
        if (DataContext is ViewModels.SettingsViewModel settings) settings.SetOpenClawPassword(OpenClawPasswordBox.Password);
    }
}
