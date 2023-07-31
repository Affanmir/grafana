package assetpath

import (
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/grafana/grafana/pkg/plugins"
	"github.com/grafana/grafana/pkg/plugins/config"
	"github.com/grafana/grafana/pkg/plugins/pluginscdn"
)

// Service provides methods for constructing asset paths for plugins.
// It supports core plugins, external plugins stored on the local filesystem, and external plugins stored
// on the plugins CDN, and it will switch to the correct implementation depending on the plugin and the config.
type Service struct {
	cdn *pluginscdn.Service
}

func ProvideService(cdn *pluginscdn.Service) *Service {
	return &Service{cdn: cdn}
}

func DefaultService(cfg *config.Cfg) *Service {
	return &Service{cdn: pluginscdn.ProvideService(cfg)}
}

// Base returns the base path for the specified plugin.
func (s *Service) Base(pluginJSON plugins.JSONData, class plugins.Class, pluginDir string) (string, error) {
	if class == plugins.ClassCore {
		baseDir := getBaseDir(pluginDir, true)
		if isDecoupledPlugin(pluginDir) {
			return path.Join("public/plugins", baseDir), nil
		}
		return path.Join("public/app/plugins", string(pluginJSON.Type), baseDir), nil
	}
	if s.cdn.PluginSupported(pluginJSON.ID) {
		return s.cdn.SystemJSAssetPath(pluginJSON.ID, pluginJSON.Info.Version, "")
	}
	return path.Join("public/plugins", pluginJSON.ID), nil
}

// Module returns the module.js path for the specified plugin.
func (s *Service) Module(pluginJSON plugins.JSONData, class plugins.Class, pluginDir string) (string, error) {
	if class == plugins.ClassCore {
		baseDir := getBaseDir(pluginDir, false)
		return path.Join("app/plugins", string(pluginJSON.Type), baseDir, "module"), nil
	}
	if s.cdn.PluginSupported(pluginJSON.ID) {
		return s.cdn.SystemJSAssetPath(pluginJSON.ID, pluginJSON.Info.Version, "module")
	}
	return path.Join("plugins", pluginJSON.ID, "module"), nil
}

// RelativeURL returns the relative URL for an arbitrary plugin asset.
// If pathStr is an empty string, defaultStr is returned.
func (s *Service) RelativeURL(p *plugins.Plugin, pathStr, defaultStr string) (string, error) {
	if pathStr == "" {
		return defaultStr, nil
	}
	if s.cdn.PluginSupported(p.ID) {
		// CDN
		return s.cdn.NewCDNURLConstructor(p.ID, p.Info.Version).StringPath(pathStr)
	}
	// Local
	u, err := url.Parse(pathStr)
	if err != nil {
		return "", fmt.Errorf("url parse: %w", err)
	}
	if u.IsAbs() {
		return pathStr, nil
	}
	// is set as default or has already been prefixed with base path
	if pathStr == defaultStr || strings.HasPrefix(pathStr, p.BaseURL) {
		return pathStr, nil
	}
	return path.Join(p.BaseURL, pathStr), nil
}

func isDecoupledPlugin(pluginDir string) bool {
	return strings.Contains(filepath.ToSlash(pluginDir), "public/plugins")
}

func getBaseDir(pluginDir string, keepSrcDir bool) string {
	baseDir := filepath.Base(pluginDir)
	if isDecoupledPlugin(pluginDir) {
		// Decoupled core plugins will be suffixed with "dist" if they have been built or "src" if not.
		// e.g. public/plugins/testdata/src
		if baseDir == "dist" || baseDir == "src" {
			parentDir := filepath.Base(strings.TrimSuffix(pluginDir, baseDir))
			if keepSrcDir {
				return filepath.Join(parentDir, baseDir)
			}
			return parentDir
		}
	}
	return baseDir
}
