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
	"cloudlight.dev/codexbridge/bridge-daemon/internal/channelprofiles"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/config"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/mirror"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/openclaw"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/taskcenter"
)

var version = "1.3.2"

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
	conversationRegistry, err := conversationregistry.New(paths.ConversationNumbersFile, paths.ThreadNumbersFile, paths.OpenClawSessionNumbersFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bridge-daemon: open conversation numbers:", err)
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
	projectRegistry, err := taskcenter.NewProjectRegistry(paths.ProjectsFile)
	if err != nil {
		logger.Printf("initialize projects: %v", err)
		_ = listener.Close()
		os.Exit(1)
	}
	taskRegistry, err := taskcenter.NewTaskRegistry(paths.TasksFile)
	if err != nil {
		logger.Printf("initialize tasks: %v", err)
		_ = listener.Close()
		os.Exit(1)
	}
	if warning := projectRegistry.LoadWarning(); warning != "" {
		logger.Printf("[projects] %s", warning)
		broker.Publish(events.Error, map[string]any{"category": "projects", "message": warning})
	}
	if warning := taskRegistry.LoadWarning(); warning != "" {
		logger.Printf("[tasks] %s", warning)
		broker.Publish(events.Error, map[string]any{"category": "tasks", "message": warning})
	}
	manager, err := bridgeruntime.NewManager(options.Version, address, options.CodexPath, options.SandboxMode, broker, logger, conversationRegistry)
	if err != nil {
		logger.Printf("initialize runtime: %v", err)
		_ = listener.Close()
		os.Exit(1)
	}
	controlService := control.NewService(manager, manager, conversationRegistry)
	openClawService := openclaw.NewService(logger, broker, conversationRegistry)
	taskService := taskcenter.NewService(projectRegistry, taskRegistry, broker, logger,
		taskcenter.NewCodexTaskAdapter(controlService, manager, conversationRegistry),
		taskcenter.NewOpenClawTaskAdapter(openClawService, conversationRegistry),
	)
	profileManager := channelprofiles.NewManager(controlService, manager, bindingRepository, broker, logger, conversationRegistry, commandRegistry, openClawService)
	profileManager.SetTaskService(taskService)
	mirrorService, err := mirror.New(paths.MirrorFile, controlService, manager, conversationRegistry, broker, logger,
		mirror.Target{Status: func() (string, bool) {
			return profileManager.TelegramMirrorTarget()
		}, Send: func(ctx context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
			return profileManager.SendTelegramMirror(ctx, message)
		}},
		mirror.Target{Status: func() (string, bool) {
			return profileManager.QQMirrorTarget()
		}, Send: func(ctx context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
			return profileManager.SendQQMirror(ctx, message)
		}},
	)
	if err != nil {
		logger.Printf("initialize mirror: %v", err)
		_ = listener.Close()
		os.Exit(1)
	}
	server := api.New(options.Token, manager, controlService, bindingRepository, broker, logger, profileManager, mirrorService, commandRegistry)
	server.SetOpenClawBackend(openClawService)
	server.SetTaskService(taskService)

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
	taskService.Start()
	go func() {
		// Backend connection startup is asynchronous. Retry recovery for a
		// bounded window; adapters defer work while disconnected, and the
		// dispatch mutex makes overlapping connection-triggered passes idempotent.
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			taskService.Recover(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

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
	profilesContext, cancelProfiles := context.WithTimeout(context.Background(), 10*time.Second)
	if err := profileManager.Close(profilesContext); err != nil {
		logger.Printf("channel profiles shutdown: %v", err)
	}
	cancelProfiles()
	taskService.Close()
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
