#ifndef MyAppVersion
  #define MyAppVersion "1.2.0"
#endif

#ifndef SourceDir
  #error SourceDir must point to a complete win-x64 publish directory.
#endif

#ifndef OutputDir
  #define OutputDir "..\artifacts\win-x64-1.2.0"
#endif

#ifndef AppIconFile
  #define AppIconFile "..\apps\desktop\CloudLight.CodexBridge\Resources\AppIcon.ico"
#endif

#define MyAppName "CloudLight Codex Bridge"
#define MyAppExeName "CloudLight.CodexBridge.exe"
#define MyPublisher "CloudLight"
#define MyRunValueName "CloudLight Codex Bridge"

[Setup]
AppId={{F21D152D-9330-4C2C-8AB0-18EF9037AE4B}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyPublisher}
VersionInfoVersion={#MyAppVersion}.0
VersionInfoCompany={#MyPublisher}
VersionInfoDescription={#MyAppName} Setup
VersionInfoProductName={#MyAppName}
VersionInfoProductVersion={#MyAppVersion}
VersionInfoOriginalFileName=CloudLight-CodexBridge-Setup-{#MyAppVersion}-win-x64.exe
DefaultDirName={localappdata}\Programs\CloudLight Codex Bridge
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
AllowNoIcons=yes
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0.17763
OutputDir={#OutputDir}
OutputBaseFilename=CloudLight-CodexBridge-Setup-{#MyAppVersion}-win-x64
SetupIconFile={#AppIconFile}
UninstallDisplayIcon={app}\{#MyAppExeName}
WizardStyle=modern
Compression=lzma2/ultra64
SolidCompression=yes
CloseApplications=yes
CloseApplicationsFilter={#MyAppExeName},bridge-daemon.exe
RestartApplications=no
SetupLogging=yes
UsedUserAreasWarning=no

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Languages\ChineseSimplified.isl"

[Tasks]
Name: "desktopicon"; Description: "创建桌面快捷方式"; GroupDescription: "附加快捷方式："; Flags: unchecked

[Files]
Source: "{#SourceDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; WorkingDir: "{app}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; WorkingDir: "{app}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "启动 {#MyAppName}"; Flags: nowait postinstall skipifsilent

[Code]
procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usUninstall then
    RegDeleteValue(
      HKCU,
      'Software\Microsoft\Windows\CurrentVersion\Run',
      '{#MyRunValueName}');
end;
