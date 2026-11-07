# Surround Sound Fix (decky-pw-ac3-live)

Decky Loader plugin that gives Steam Deck real 5.1 surround over HDMI by encoding game audio to Dolby Digital (AC-3) in real time with `pw-ac3-live`. For TVs, soundbars and receivers that accept Dolby Digital but only stereo PCM.

Plugin releases are intended to bundle the `pw-ac3-live` executable so users do not need to install it separately.

## Overview

This repository contains the Decky plugin implementation for `pw-ac3-live`, including:

- Frontend UI (`src/`)
- Python backend (`main.py`)
- Decky metadata (`plugin.json`, `package.json`)
- Packaged defaults (`defaults/`)

## Features

- **One toggle** to turn the AC-3 (Dolby Digital 5.1) encoder on or off from the Quick Access menu.
- **Auto-start**: the plugin reads the connected display's ELD (`/proc/asound/card0/eld#*`) and starts the
  encoder automatically when the TV/receiver accepts AC-3 5.1 but only stereo PCM, which is exactly the case
  where 5.1 PCM would otherwise be cut down to stereo (centre channel/dialogue lost).
  Displays that play multichannel PCM directly don't trigger it.
- **Sleep-safe**: holds a logind *delay* inhibitor (no root needed). Before sleep the encoder is stopped and
  audio restored; after wake it restarts automatically once HDMI is back.
- **Hotplug-safe**: unplugging HDMI (or the Desktop/Game Mode switch, which looks like a hotplug) stops the
  encoder cleanly; it restarts when the display returns.
- **Crash recovery**: if the encoder exits unexpectedly it is restarted with backoff (5/15/30 s).
- **Latency presets**: Low (60 ms) / Normal (120 ms, default) / Safe (200 ms) ALSA buffer. Changing it while
  running restarts the encoder. Normal is the default because 60 ms stuttered on SteamOS 3.9, where neither
  PipeWire nor the encoder get realtime scheduling.
- **Persistent settings** stored in Decky's plugin settings directory (`settings.json`), including the
  advanced encoder parameters.

### Robustness notes

- After an HDMI hotplug, PipeWire can report the HDMI card profile as `off` while a stale sink node still
  holds `hw:0,8` open. ALSA's `HDMI.conf` locks `IEC958 Playback Default` while the device is open, so
  `iecset` fails with `Operation not permitted`. The plugin cycles the profile on -> off before start and
  waits for the PCM to close (`HDMI_DEVICE_BUSY` error if it doesn't).
- The pre-start audio snapshot never records `off` as the profile to restore, and leftover state from a
  crashed session is restored before a new snapshot is taken.
- The internal speaker card is matched by PCI address (`alsa_card.pci-0000_04_00.5-*`) since its name
  differs between Decks/SteamOS versions.

## About pw-ac3-live

`pw-ac3-live` is the core encoder daemon used by this plugin. It captures 5.1 LPCM, encodes it to AC-3 in real time, and outputs an IEC61937 stream for HDMI/SPDIF passthrough.

It supports two output paths:

- PipeWire native output (desktop Linux)
- Direct ALSA output (required on Steam Deck to avoid PipeWire ALSA scheduling jitter)

Links:

- Core repository: <https://github.com/pums974/pw-ac3-live>

## Distribution Model

- End-user plugin package: includes `pw-ac3-live` binary.
- Runtime expectation: no external `pw-ac3-live` installation required.
- Development: optional local override may be used for testing only.

## Development

### Prerequisites

- Node.js >= 16.14
- `pnpm` v9
- Decky Loader development environment

### Building the bundled encoder

The package expects the encoder at `bin/pw-ac3-live`. On SteamOS 3.9 (PipeWire 1.6) the encoder's
`libspa 0.8` crate fails to compile against the system PipeWire headers (`no field size on spa_pod_builder`).
Building against older headers works, and the resulting binary runs fine on PipeWire 1.6:

```bash
git clone https://github.com/pums974/pw-ac3-live && cd pw-ac3-live
podman run --rm --network=host -v "$PWD":/src:Z -w /src docker.io/library/ubuntu:24.04 bash -c '
  apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq curl build-essential \
    pkg-config clang libclang-dev libpipewire-0.3-dev libasound2-dev &&
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal && . $HOME/.cargo/env &&
  cargo build --release'
# then copy target/release/pw-ac3-live to <plugin>/bin/pw-ac3-live
```

### Install dependencies

```bash
pnpm install
```

### Build frontend

```bash
pnpm run build
```

### Watch mode

```bash
pnpm run watch
```

## Plugin Identity

- Plugin name: `Surround Sound Fix` (install folder and settings/logs dir stay `pw-ac3-live`)
- Package name: `decky-pw-ac3-live`
- Author: `Alexandre Poux`
- API version: `1`

## License

BSD-3-Clause. See `LICENSE`.
