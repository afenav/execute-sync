package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/urfave/cli/v2"
)

// GitHub API response structures
type GithubRelease struct {
	TagName string        `json:"tag_name"`
	Assets  []GithubAsset `json:"assets"`
}

type GithubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	ContentType        string `json:"content_type"`
	Size               int    `json:"size"`
}

// UpgradeCommand creates a command to upgrade to the latest version
func UpgradeCommand() *cli.Command {
	return &cli.Command{
		Name:        "upgrade",
		Aliases:     []string{"u"},
		Usage:       "Upgrade to the latest version",
		Description: "Downloads and installs the latest version of execute-sync",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "force",
				Usage: "Force upgrade even if already on latest version",
			},
		},
		Action: func(cCtx *cli.Context) error {
			return performUpgrade(cCtx)
		},
	}
}

// performUpgrade handles the upgrade process
func performUpgrade(cCtx *cli.Context) error {
	// Get current executable path
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	// Get the latest release info
	release, err := getLatestRelease()
	if err != nil {
		return fmt.Errorf("failed to get latest release info: %w", err)
	}

	// Skip if already on latest version unless force flag is used
	latestVersion := strings.TrimPrefix(release.TagName, "v")
	currentVersion := strings.TrimPrefix(version, "v")

	if latestVersion == currentVersion && !cCtx.Bool("force") {
		log.Info("Already running the latest version", "version", version)
		return nil
	}

	// Find appropriate asset for current platform
	asset, found := findAssetForCurrentPlatform(release.Assets)
	if !found {
		return fmt.Errorf("no compatible binary found for %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	log.Info("Downloading latest version", "version", release.TagName, "asset", asset.Name)

	// Create temporary directory for download
	tempDir, err := os.MkdirTemp("", "execute-sync-upgrade")
	if err != nil {
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Download the asset
	assetPath := filepath.Join(tempDir, asset.Name)
	if err := downloadFile(asset.BrowserDownloadURL, assetPath); err != nil {
		return fmt.Errorf("failed to download release asset: %w", err)
	}

	// Extract binary from archive if needed
	binaryPath, err := extractBinary(assetPath, tempDir)
	if err != nil {
		return fmt.Errorf("failed to extract binary: %w", err)
	}

	// Make the binary executable
	if err := os.Chmod(binaryPath, 0755); err != nil {
		return fmt.Errorf("failed to make binary executable: %w", err)
	}

	// Create backup of current binary
	backupPath := execPath + ".bak"
	if err := os.Rename(execPath, backupPath); err != nil {
		return fmt.Errorf("failed to create backup of current binary: %w", err)
	}
	log.Info("Created backup of current binary", "path", backupPath)

	// Replace current binary with new one
	if err := os.Rename(binaryPath, execPath); err != nil {
		// Attempt to restore from backup
		os.Rename(backupPath, execPath)
		return fmt.Errorf("failed to replace binary: %w", err)
	}

	log.Info("Successfully upgraded to", "version", release.TagName)
	return nil
}

// getLatestRelease fetches info about the latest GitHub release
func getLatestRelease() (*GithubRelease, error) {
	resp, err := http.Get("https://api.github.com/repos/afenav/execute-sync/releases/latest")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub API returned status %d", resp.StatusCode)
	}

	var release GithubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, err
	}

	return &release, nil
}

// findAssetForCurrentPlatform finds the appropriate asset for current OS and architecture
func findAssetForCurrentPlatform(assets []GithubAsset) (GithubAsset, bool) {
	// Get target platform identifiers
	targetOS := runtime.GOOS
	targetArch := runtime.GOARCH

	// Look for the pattern <os>_<arch>.zip
	expectedPattern := fmt.Sprintf("%s_%s.zip", targetOS, targetArch)

	for _, asset := range assets {
		name := strings.ToLower(asset.Name)
		if strings.Contains(name, expectedPattern) {
			return asset, true
		}
	}

	return GithubAsset{}, false
}

// downloadFile downloads a file from URL to a local path
func downloadFile(url, filepath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed with status %d", resp.StatusCode)
	}

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

// extractBinary extracts the binary from the downloaded archive
func extractBinary(archivePath, destDir string) (string, error) {
	// We only handle zip files in this simplified version
	return extractFromZip(archivePath, destDir)
}

// extractFromZip extracts files from a .zip archive
func extractFromZip(archivePath, destDir string) (string, error) {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	executablePath := ""

	for _, file := range reader.File {
		// Skip directories
		if file.FileInfo().IsDir() {
			continue
		}

		// Look for the main executable
		filename := filepath.Base(file.Name)
		if strings.Contains(strings.ToLower(filename), "execute-sync") {
			outPath := filepath.Join(destDir, filename)
			outFile, err := os.Create(outPath)
			if err != nil {
				return "", err
			}

			rc, err := file.Open()
			if err != nil {
				outFile.Close()
				return "", err
			}

			_, err = io.Copy(outFile, rc)
			outFile.Close()
			rc.Close()

			if err != nil {
				return "", err
			}

			executablePath = outPath
		}
	}

	if executablePath == "" {
		return "", fmt.Errorf("no executable found in archive")
	}

	return executablePath, nil
}
