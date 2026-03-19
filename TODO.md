# TODO

## Highest Priority

1. Packaging and install flow
- add a real installer for `codexbar-linux.py`, systemd user units, and the `runtime-governor` plugin
- add uninstall and upgrade paths
- document expected Ubuntu dependencies clearly

2. Background worker split
- move more non-UI work out of the GTK tray process
- isolate usage aggregation, smoke orchestration, alert evaluation, and event compaction
- keep the tray focused on rendering and operator actions

3. Test harness
- add repeatable checks for smoke tests, restore-point create/restore, alert dedupe, and event retention
- make it easy to run a “known good” local verification pass before release

## Runtime and Policy

4. Policy violation console
- show expected lane vs actual lane for top-level and sub-agent runs
- surface strict/advisory policy state clearly
- make route drift obvious when fallbacks or overrides happen

5. Fallback and failover analytics
- track fallback frequency
- show provider failover chains
- highlight degraded periods by provider

6. Sub-agent drilldown
- show parent lane, child lane, task type, and sidecar role
- make orchestrator vs worker behavior visible in one place

## Ops UX

7. Event browser expansion
- add search
- add date/time range filters
- allow copying raw event JSON
- allow exporting filtered event sets

8. Alert UX polish
- show visible mute countdown
- show acknowledged state in the UI
- separate warning vs critical styling more clearly
- make alert cooldown configurable

9. Daemon diagnostics
- expand structured local OpenClaw and NemoClaw fields
- show last successful action and last failure reason
- improve recovery guidance inside the tray

## Repo and Release

10. Example config templates
- ship sample daemon config
- ship sample pricing registry
- ship sample routing policy

11. Release hygiene
- add a changelog
- define version/tag strategy
- add release notes template

12. Public demo assets
- add more polished screenshots
- add a short demo GIF or video
- add a quick-start install section to the README
