# Helios SDK Tools

VS Code extension for building and installing Helios node plugins.

## Commands
- Helios SDK: Build Plugin
- Helios SDK: Build && Install Plugin
- Helios SDK: Install Plugin From Path
- Helios SDK: Create Plugin Project

## Settings
- `heliosSdk.apiBase`: Base URL for the Helios API (default `http://127.0.0.1:5801`).
- `heliosSdk.buildCommand`: Shell command to build the plugin.
- `heliosSdk.buildWorkingDirectory`: Working directory for the build command.
- `heliosSdk.pluginOutputPath`: Default path to the built `.so`.
- `heliosSdk.sdkRoot`: Override path for the SDK root (defaults to `IDE_SDK_DIR`).
- `heliosSdk.projectsRoot`: Override path for the SDK projects directory (defaults to `IDE_PROJECTS_DIR`).
