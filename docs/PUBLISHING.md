# Publishing Checklist

Before pushing this repo:

1. Run a secret scan:

```bash
rg -n "sk-proj-|nvapi-|AIza|NEMOCLAW_COOKIE|OPENAI_API_KEY=|NVIDIA_API_KEY=|GEMINI_API_KEY=|access_token|refresh_token" .
```

2. Confirm no local state files were added:

```bash
git status --short
```

3. Confirm no personal absolute paths remain:

```bash
rg -n "/home/|\\.docker/desktop/docker.sock|desktop-linux" .
```

4. Review systemd unit templates:

- `systemd/codexbar.service`
- `systemd/codexbar-smoke-tests.service`
- `systemd/codexbar-usage-snapshot.service`

5. If you use NemoClaw, keep your actual Docker socket and environment overrides local in `~/.openclaw/codexbar-daemons.json`, not in git.

## Recommended Backup

Keep two backups:

- the sanitized git repo in this folder
- a private local tarball of your real live setup

Example private backup:

```bash
tar -czf ~/codexbar-private-backup-$(date +%Y%m%d-%H%M%S).tgz \
  ~/.local/bin/codexbar-linux.py \
  ~/.openclaw/extensions/runtime-governor \
  ~/.config/systemd/user/codexbar.service \
  ~/.config/systemd/user/codexbar-smoke-tests.service \
  ~/.config/systemd/user/codexbar-smoke-tests.timer \
  ~/.config/systemd/user/codexbar-usage-snapshot.service \
  ~/.config/systemd/user/codexbar-usage-snapshot.timer
```
