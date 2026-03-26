package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/smit-p/pdrive/internal/rclonerc"
)

// RcloneManager manages the rclone RC daemon as a child process.
type RcloneManager struct {
	rcloneBin  string
	configPath string
	addr       string
	client     *rclonerc.Client

	cmd    *exec.Cmd
	mu     sync.Mutex
	cancel context.CancelFunc
}

// NewRcloneManager creates a new rclone manager.
func NewRcloneManager(rcloneBin, configPath, addr string) *RcloneManager {
	return &RcloneManager{
		rcloneBin:  rcloneBin,
		configPath: configPath,
		addr:       addr,
		client:     rclonerc.NewClient(addr),
	}
}

// Client returns the rclone RC client.
func (rm *RcloneManager) Client() *rclonerc.Client {
	return rm.client
}

// Start spawns rclone in RC daemon mode and waits for it to become healthy.
func (rm *RcloneManager) Start(ctx context.Context) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	monitorCtx, cancel := context.WithCancel(ctx)
	rm.cancel = cancel

	if err := rm.spawn(monitorCtx); err != nil {
		cancel()
		return err
	}

	// Wait for rclone to become healthy.
	if err := rm.waitHealthy(30 * time.Second); err != nil {
		rm.kill()
		cancel()
		return fmt.Errorf("rclone failed to start: %w", err)
	}

	slog.Info("rclone RC started", "addr", rm.addr)

	// Monitor and auto-restart in background.
	go rm.monitor(monitorCtx)

	return nil
}

// Stop gracefully shuts down the rclone process.
func (rm *RcloneManager) Stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.cancel != nil {
		rm.cancel()
	}
	rm.kill()
	slog.Info("rclone RC stopped")
}

func (rm *RcloneManager) spawn(ctx context.Context) error {
	rm.cmd = exec.CommandContext(ctx, rm.rcloneBin,
		"rcd",
		"--rc-addr", rm.addr,
		"--rc-no-auth",
		"--config", rm.configPath,
	)
	rm.cmd.Stdout = os.Stdout
	rm.cmd.Stderr = os.Stderr

	if err := rm.cmd.Start(); err != nil {
		return fmt.Errorf("starting rclone: %w", err)
	}
	return nil
}

func (rm *RcloneManager) kill() {
	if rm.cmd != nil && rm.cmd.Process != nil {
		rm.cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- rm.cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			rm.cmd.Process.Kill()
		}
		rm.cmd = nil
	}
}

func (rm *RcloneManager) waitHealthy(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := rm.client.Ping(); err == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("rclone RC did not respond within %v", timeout)
}

func (rm *RcloneManager) monitor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			if err := rm.client.Ping(); err != nil {
				slog.Warn("rclone health check failed, restarting", "error", err)
				rm.mu.Lock()
				rm.kill()
				if err := rm.spawn(ctx); err != nil {
					slog.Error("failed to restart rclone", "error", err)
				} else {
					rm.waitHealthy(30 * time.Second)
					slog.Info("rclone restarted successfully")
				}
				rm.mu.Unlock()
			}
		}
	}
}
