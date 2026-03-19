# TODO

## Next Priority

1. Daemon diagnostics panel
Show structured NemoClaw and local OpenClaw fields directly:
- connected/running state
- server URL
- version
- autostart
- last successful action

2. Alert UX polish
Improve the operator flow around alerts:
- visible mute countdown
- explicit acknowledged state in UI
- separate warning vs critical tone rules
- configurable cooldown instead of fixed default

3. Event browser expansion
The event center now has filters and detail view. Next steps:
- search
- date/time range
- export selected events
- copy raw event JSON

4. Background worker split
Move more non-UI work out of the GTK process:
- alert evaluation
- event compaction
- smoke orchestration
- incident summarization

## Runtime / Policy

5. Policy violation console
Show when actual route != expected route, with:
- task type
- selected lane
- actual lane
- reason for drift
- strict vs advisory policy state

6. Fallback-hop analytics
Track and display:
- fallback frequency
- provider failover chains
- degraded periods by provider

7. Sub-agent runtime drilldown
Surface orchestrator vs worker behavior more clearly:
- parent lane
- child lane
- sidecar allowed/not allowed
- spawned tool count

## Packaging / Repo

8. Install script
Add a clean installer for:
- `codexbar-linux.py`
- systemd units
- runtime-governor plugin

9. Config templates
Ship example local config files instead of relying on live defaults:
- daemon config example
- pricing registry example
- policy example

10. Release hygiene
Add:
- license
- changelog
- version tag strategy
- release notes template

## Nice To Have

11. Public demo assets
Add screenshots and a short demo GIF/video.

12. Theming pass
Offer one cleaner light theme and one stronger dark ops theme.

13. Test harness
Add reproducible local checks for:
- smoke tests
- restore-point creation/restore
- event retention
- alert dedupe

