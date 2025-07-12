package internal

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// DockerManager manages Docker container lifecycle and health operations
// used for simulating node failures and recoveries in benchmarks.
type DockerManager struct {
	enabled        bool
	containerNames []string
	timeout        time.Duration
}

// NewDockerManager returns a configured DockerManager instance.
func NewDockerManager(enabled bool, containerNames []string, timeout time.Duration) *DockerManager {
	return &DockerManager{
		enabled:        enabled,
		containerNames: containerNames,
		timeout:        timeout,
	}
}

// VerifyDockerAvailable checks whether Docker is accessible and responsive.
func (dm *DockerManager) VerifyDockerAvailable(ctx context.Context) error {
	if !dm.enabled {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, dm.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker not available: %w (output: %s)", err, string(output))
	}

	return nil
}

// VerifyContainersExist ensures that all expected containers are present.
func (dm *DockerManager) VerifyContainersExist(ctx context.Context) error {
	if !dm.enabled {
		return nil
	}

	for _, containerName := range dm.containerNames {
		if err := dm.verifyContainerExists(ctx, containerName); err != nil {
			return fmt.Errorf("container %s verification failed: %w", containerName, err)
		}
	}

	return nil
}

// verifyContainerExists checks if a specific container exists and is inspectable.
func (dm *DockerManager) verifyContainerExists(ctx context.Context, containerName string) error {
	ctx, cancel := context.WithTimeout(ctx, dm.timeout)
	defer cancel()

	cmd := exec.CommandContext(
		ctx,
		"docker",
		"inspect",
		"--format",
		"{{.State.Status}}",
		containerName,
	)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("container %s not found or not accessible", containerName)
	}

	return nil
}

// GetContainerStatus retrieves the current status (e.g. running, exited) of a container.
func (dm *DockerManager) GetContainerStatus(
	ctx context.Context,
	containerName string,
) (string, error) {
	if !dm.enabled {
		return "unknown", nil
	}

	ctx, cancel := context.WithTimeout(ctx, dm.timeout)
	defer cancel()

	cmd := exec.CommandContext(
		ctx,
		"docker",
		"inspect",
		"--format",
		"{{.State.Status}}",
		containerName,
	)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get container status: %w", err)
	}

	return strings.TrimSpace(string(output)), nil
}

// StopContainer stops a running Docker container by name.
func (dm *DockerManager) StopContainer(ctx context.Context, containerName string) error {
	if !dm.enabled {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, dm.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "stop", containerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"failed to stop container %s: %w (output: %s)",
			containerName,
			err,
			string(output),
		)
	}

	return nil
}

// StartContainer starts a stopped Docker container by name.
func (dm *DockerManager) StartContainer(ctx context.Context, containerName string) error {
	if !dm.enabled {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, dm.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "start", containerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"failed to start container %s: %w (output: %s)",
			containerName,
			err,
			string(output),
		)
	}

	return nil
}

// WaitForContainerReady waits until the container is running and, optionally, passes a health check.
func (dm *DockerManager) WaitForContainerReady(
	ctx context.Context,
	containerName string,
	healthCheck func() error,
) error {
	if !dm.enabled {
		return nil
	}

	timeout := 30 * time.Second
	interval := 2 * time.Second

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for container %s to be ready", containerName)
		case <-ticker.C:
			status, err := dm.GetContainerStatus(ctx, containerName)
			if err != nil {
				continue
			}

			if status == "running" {
				if healthCheck != nil {
					if err := healthCheck(); err == nil {
						return nil
					}
				} else {
					return nil
				}
			}
		}
	}
}

// SimulateNodeFailure stops the container representing a specific node.
func (dm *DockerManager) SimulateNodeFailure(ctx context.Context, nodeIndex int) error {
	if !dm.enabled || nodeIndex >= len(dm.containerNames) {
		return fmt.Errorf("invalid node index or Docker not enabled")
	}

	containerName := dm.containerNames[nodeIndex]
	return dm.StopContainer(ctx, containerName)
}

// RecoverNode restarts a previously stopped container to simulate node recovery.
func (dm *DockerManager) RecoverNode(ctx context.Context, nodeIndex int) error {
	if !dm.enabled || nodeIndex >= len(dm.containerNames) {
		return fmt.Errorf("invalid node index or Docker not enabled")
	}

	containerName := dm.containerNames[nodeIndex]
	return dm.StartContainer(ctx, containerName)
}
