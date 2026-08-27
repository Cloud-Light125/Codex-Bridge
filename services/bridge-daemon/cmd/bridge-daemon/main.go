package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/api"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/bindings"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/config"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/mirror"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/openclaw"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/qqbot"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/telegram"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/threadregistry"
)

var version = "1.0.1"

func main() {
	options := config.Options{Version: version}
	flag.StringVar(&options.Listen, "listen", "127.0.0.1:0", "local API listen address (127.0.0.1 only)")
	flag.StringVar(&options.Token, "token", "", "local API bearer token")
	flag.StringVar(&options.CodexPath, "codex-path", "", "optional Codex CLI executable path")
	flag.StringVar(&options.SandboxMode, "sandbox", "workspace-write", "sandbox for new turns: read-only or workspace-write")
	flag.Parse()
	if err := options.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "bridge-daemon:", err)
		os.Exit(2)
	}

	paths, err := config.UserPaths()
	if err != nil {
		fmt.Fprintln(os.Stderr, "bridge-daemon:", err)
		os.Exit(1)
	}
	logger, err := bridgelog.New(paths.LogFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bridge-daemon: open log:", err)
		os.Exit(1)
	}
	defer logger.Close()
	bindingRepository, err := bindings.NewRepository(paths.BindingsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bridge-daemon: open bindings:", err)
		os.Exit(1)
	}
	threadRegistry, err := threadregistry.New(paths.ThreadNumbersFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bridge-daemon: open thread numbers:", err)
		os.Exit(1)
	}
	commandRegistry, err := commandregistry.New(paths.CommandsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bridge-daemon: open commands:", err)
		os.Exit(1)
	}

	listener, err := net.Listen("tcp4", options.Listen)
	if err != nil {
		logger.Printf("listen failed: %v", err)
		os.Exit(1)
	}
	address := "http://" + listener.Addr().String()
	broker := events.NewBroker()
	manager, err := bridgeruntime.NewManager(options.Version, address, options.CodexPath, options.SandboxMode, broker, logger, threadRegistry)
	if err != nil {
		logger.Printf("initialize runtime: %v", err)
		_ = listener.Close()
		os.Exit(1)
	}
	controlService := control.NewService(manager, manager, threadRegistry)
	telegramService := telegram.NewService(controlService, manager, bindingRepository, broker, logger, threadRegistry)
	qqbotService := qqbot.NewService(controlService, manager, bindingRepository, broker, logger, threadRegistry)
	openClawService := openclaw.NewService(logger, broker)
	telegramService.SetOpenClawBackend(openClawService)
	qqbotService.SetOpenClawBackend(openClawService)
	telegramService.SetCommandRegistry(commandRegistry)
	qqbotService.SetCommandRegistry(commandRegistry)
	mirrorService, err := mirror.New(paths.MirrorFile, controlService, manager, threadRegistry, broker, logger,
		mirror.Target{Status: func() (string, bool) {
			status := telegramService.Adapter().TelegramStatus()
			return status.BotID, status.Running && status.Connected
		}, Send: func(ctx context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
			return telegramService.Adapter().SendMessage(ctx, message)
		}},
		mirror.Target{Status: func() (string, bool) {
			status := qqbotService.Adapter().QQBotStatus()
			return status.AppID, status.Running && status.Connected
		}, Send: func(ctx context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
			return qqbotService.Adapter().SendMessage(ctx, message)
		}},
	)
	if err != nil {
		logger.Printf("initialize mirror: %v", err)
		_ = listener.Close()
		os.Exit(1)
	}
	server := api.New(options.Token, manager, controlService, bindingRepository, broker, logger, telegramService, qqbotService, mirrorService, commandRegistry)
	server.SetOpenClawBackend(openClawService)

	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()

	ready := map[string]any{
		"type":    "ready",
		"address": address,
		"token":   options.Token,
		"pid":     os.Getpid(),
	}
	if err := json.NewEncoder(os.Stdout).Encode(ready); err != nil {
		logger.Printf("write ready line: %v", err)
		os.Exit(1)
	}
	logger.Printf("bridge-daemon ready on %s (pid=%d)", address, os.Getpid())
	broker.Publish(events.DaemonStarted, map[string]any{"address": address, "pid": os.Getpid()})
	manager.Start()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	stdinClosed := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
		}
		close(stdinClosed)
	}()

	select {
	case received := <-stop:
		logger.Printf("shutdown signal received: %s", received)
	case err := <-serveErrors:
		if err != nil {
			logger.Printf("local API stopped unexpectedly: %v", err)
		}
	case <-stdinClosed:
		logger.Printf("parent process closed stdin; shutting down")
	}

	broker.Publish(events.DaemonStopped, map[string]any{"pid": os.Getpid()})
	mirrorService.Close()
	telegramContext, cancelTelegram := context.WithTimeout(context.Background(), 10*time.Second)
	if err := telegramService.Close(telegramContext); err != nil {
		logger.Printf("Telegram shutdown: %v", err)
	}
	cancelTelegram()
	qqContext, cancelQQ := context.WithTimeout(context.Background(), 10*time.Second)
	if err := qqbotService.Close(qqContext); err != nil {
		logger.Printf("QQ Official Bot shutdown: %v", err)
	}
	cancelQQ()
	if err := openClawService.Close(); err != nil {
		logger.Printf("OpenClaw shutdown: %v", err)
	}
	if err := manager.Close(); err != nil {
		logger.Printf("runtime shutdown: %v", err)
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		logger.Printf("HTTP shutdown: %v", err)
	}
	logger.Printf("bridge-daemon stopped")
}
