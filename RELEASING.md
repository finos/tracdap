# Release Checklist

A short pre-release checklist for maintainers cutting a new TRAC D.A.P. version. This is not a
comprehensive release process - it's a running list of specific things that have gone wrong
before and are easy to miss. Add to it as new release issues are found.

## Before tagging

- [ ] **Runtime extension plugins' `tracdap-runtime` pin matches the new minor version.** Each
      plugin under `tracdap-runtime/python-ext/plugins/*/requirements.txt` pins an exact
      `tracdap-runtime == X.Y.*` range by hand - it is not derived from `dev/version.sh` and
      nothing currently checks it automatically. A stale pin makes the plugin wheel
      uninstallable alongside the new runtime (`pip install` reports a `ResolutionImpossible`
      dependency conflict between the plugin and the runtime it's meant to extend). Check with:

      grep -rn "tracdap-runtime ==" tracdap-runtime/python-ext/plugins/*/requirements.txt

      Every match should read `tracdap-runtime == <new-minor>.*`. See #754 for the bug this
      missed.
