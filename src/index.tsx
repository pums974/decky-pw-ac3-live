import {
  ButtonItem,
  DropdownItem,
  PanelSection,
  PanelSectionRow,
  TextField,
  ToggleField,
  staticClasses,
} from "@decky/ui";
import {
  addEventListener,
  callable,
  definePlugin,
  removeEventListener,
  toaster,
  useQuickAccessVisible,
} from "@decky/api";
import { useCallback, useEffect, useRef, useState } from "react";
import { FaVolumeUp } from "react-icons/fa";

type LifecycleState = "stopped" | "starting" | "running" | "stopping" | "error";
type LatencyPreset = "low" | "normal" | "safe";

interface BackendErrorPayload {
  code: string;
  message: string;
  detail: string;
  hint: string;
  timestamp: string;
}

interface EncoderStatusPayload {
  state: LifecycleState;
  pid: number | null;
  started_at: string | null;
  last_error: BackendErrorPayload | null;
}

interface DisplayInfo {
  connected: boolean;
  name: string | null;
  manufacture_id: string | null;
  product_id: string | null;
  lpcm_channels: number;
  ac3_channels: number;
  needs_encoder: boolean;
}

interface AutomationInfo {
  want_running: boolean;
  suspended: boolean;
  auto_retry_count: number;
}

type AdvancedSettings = Partial<{
  allow_external_binary_override: boolean;
  binary_path: string;
  alsa_device: string;
  latency: string;
  buffer_size: number;
  output_buffer_size: number;
  ffmpeg_thread_queue_size: number;
  ffmpeg_chunk_frames: number;
  alsa_iec_card: number;
  alsa_iec_index: number;
}>;

interface PluginSettings {
  version: number;
  auto_start: boolean;
  latency_preset: LatencyPreset;
  advanced: AdvancedSettings;
}

interface GetStatusResponse {
  status: EncoderStatusPayload;
  display: DisplayInfo;
  automation: AutomationInfo;
}

interface GetSettingsResponse {
  settings: PluginSettings;
  latency_presets_ms: Record<LatencyPreset, number>;
  display: DisplayInfo;
  automation: AutomationInfo;
}

interface MutationResponse {
  ok: boolean;
  status?: EncoderStatusPayload;
  settings?: PluginSettings;
  error: BackendErrorPayload | null;
}

const startEncoder = callable<[], MutationResponse>("start_encoder");
const stopEncoder = callable<[force?: boolean, timeoutMs?: number], MutationResponse>(
  "stop_encoder",
);
const getStatus = callable<[], GetStatusResponse>("get_status");
const getSettings = callable<[], GetSettingsResponse>("get_settings");
const setSettings = callable<[patch: Partial<PluginSettings>], MutationResponse>("set_settings");

const PRESET_LABELS: Record<LatencyPreset, string> = {
  low: "Low",
  normal: "Normal",
  safe: "Safe",
};

// Advanced fields: [key, label, default shown as placeholder, numeric]
const ADVANCED_FIELDS: Array<[keyof AdvancedSettings, string, string, boolean]> = [
  ["alsa_device", "ALSA device", "hw:0,8", false],
  ["latency", "PipeWire quantum", "1536/48000", false],
  ["ffmpeg_thread_queue_size", "FFmpeg thread queue size", "4", true],
  ["ffmpeg_chunk_frames", "FFmpeg chunk frames", "1536", true],
  ["alsa_iec_card", "ALSA IEC card", "0", true],
  ["alsa_iec_index", "ALSA IEC index", "2", true],
  ["buffer_size", "Input buffer frames", "4800", true],
  ["output_buffer_size", "Output buffer frames", "same as input", true],
];

function errorToMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function describeDisplay(display: DisplayInfo | null): string {
  if (!display) {
    return "-";
  }
  if (!display.connected) {
    return "No HDMI display connected";
  }
  const name = display.name ?? "Unknown display";
  if (display.needs_encoder) {
    return `${name}: needs Dolby Digital for 5.1 (PCM ${display.lpcm_channels}ch, AC-3 ${display.ac3_channels}ch)`;
  }
  if (display.lpcm_channels >= 6) {
    return `${name}: plays 5.1 PCM directly, encoder not needed`;
  }
  return `${name}: no AC-3 5.1 support`;
}

function describeState(state: LifecycleState | undefined, automation: AutomationInfo | null): string {
  if (automation?.suspended) {
    return "Paused for sleep";
  }
  switch (state) {
    case "running":
      return "On: Dolby Digital 5.1";
    case "starting":
      return "Starting…";
    case "stopping":
      return "Stopping…";
    case "error":
      return automation?.want_running ? "Error (retrying)" : "Error";
    case "stopped":
      return "Off";
    default:
      return "Loading…";
  }
}

function Content() {
  const [status, setStatus] = useState<EncoderStatusPayload | null>(null);
  const [display, setDisplay] = useState<DisplayInfo | null>(null);
  const [automation, setAutomation] = useState<AutomationInfo | null>(null);
  const [settings, setSettingsState] = useState<PluginSettings | null>(null);
  const [presetsMs, setPresetsMs] = useState<Record<LatencyPreset, number>>({
    low: 60,
    normal: 120,
    safe: 200,
  });
  const [advancedForm, setAdvancedForm] = useState<Record<string, string>>({});
  const [allowExternal, setAllowExternal] = useState(false);
  const [binaryPath, setBinaryPath] = useState("");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [isMutating, setIsMutating] = useState(false);
  const [frontendError, setFrontendError] = useState<string | null>(null);

  const quickAccessVisible = useQuickAccessVisible();
  const previousVisibleRef = useRef<boolean>(quickAccessVisible);

  const toastError = useCallback((error: BackendErrorPayload) => {
    toaster.toast({
      title: `Surround Sound Fix: ${error.code}`,
      body: error.message,
      critical: true,
    });
  }, []);

  const applySettings = useCallback((next: PluginSettings) => {
    setSettingsState(next);
    const form: Record<string, string> = {};
    for (const [key] of ADVANCED_FIELDS) {
      const value = next.advanced[key];
      form[key] = value === undefined || value === null ? "" : String(value);
    }
    setAdvancedForm(form);
    setAllowExternal(Boolean(next.advanced.allow_external_binary_override));
    setBinaryPath(next.advanced.binary_path ?? "");
  }, []);

  const refreshStatus = useCallback(async () => {
    try {
      const response = await getStatus();
      setStatus(response.status);
      setDisplay(response.display);
      setAutomation(response.automation);
      setFrontendError(null);
    } catch (error) {
      setFrontendError(`Failed to load status: ${errorToMessage(error)}`);
    }
  }, []);

  const loadSettings = useCallback(async () => {
    try {
      const response = await getSettings();
      applySettings(response.settings);
      setPresetsMs(response.latency_presets_ms);
      setDisplay(response.display);
      setAutomation(response.automation);
    } catch (error) {
      setFrontendError(`Failed to load settings: ${errorToMessage(error)}`);
    }
  }, [applySettings]);

  useEffect(() => {
    void loadSettings();
    void refreshStatus();

    const stateListener = addEventListener<[status: EncoderStatusPayload]>(
      "encoder_state_changed",
      (nextStatus) => {
        setStatus(nextStatus);
        void refreshStatus();
      },
    );
    const errorListener = addEventListener<[error: BackendErrorPayload]>(
      "encoder_error",
      (error) => {
        setFrontendError(`${error.code}: ${error.message}`);
        void refreshStatus();
      },
    );
    const displayListener = addEventListener<[info: DisplayInfo]>("display_changed", (info) => {
      setDisplay(info);
    });
    const settingsListener = addEventListener<[next: PluginSettings]>(
      "settings_changed",
      (next) => applySettings(next),
    );

    return () => {
      removeEventListener("encoder_state_changed", stateListener);
      removeEventListener("encoder_error", errorListener);
      removeEventListener("display_changed", displayListener);
      removeEventListener("settings_changed", settingsListener);
    };
  }, [applySettings, loadSettings, refreshStatus]);

  useEffect(() => {
    const wasVisible = previousVisibleRef.current;
    previousVisibleRef.current = quickAccessVisible;
    if (!wasVisible && quickAccessVisible) {
      void refreshStatus();
    }
  }, [quickAccessVisible, refreshStatus]);

  const state = status?.state;
  const isOn = state === "running" || state === "starting";
  const isTransitioning = state === "starting" || state === "stopping";

  const handleToggleEncoder = useCallback(
    async (enable: boolean) => {
      setIsMutating(true);
      try {
        const response = enable ? await startEncoder() : await stopEncoder(false, 5000);
        if (response.status) {
          setStatus(response.status);
        }
        if (!response.ok && response.error) {
          setFrontendError(`${response.error.code}: ${response.error.message}`);
          toastError(response.error);
        } else {
          setFrontendError(null);
        }
      } catch (error) {
        setFrontendError(`Request failed: ${errorToMessage(error)}`);
      } finally {
        setIsMutating(false);
        void refreshStatus();
      }
    },
    [refreshStatus, toastError],
  );

  const saveSettings = useCallback(
    async (patch: Partial<PluginSettings>): Promise<boolean> => {
      setIsMutating(true);
      try {
        const response = await setSettings(patch);
        if (response.settings) {
          applySettings(response.settings);
        }
        if (!response.ok && response.error) {
          setFrontendError(`${response.error.code}: ${response.error.message}`);
          toastError(response.error);
          return false;
        }
        setFrontendError(null);
        return true;
      } catch (error) {
        setFrontendError(`Saving settings failed: ${errorToMessage(error)}`);
        return false;
      } finally {
        setIsMutating(false);
        void refreshStatus();
      }
    },
    [applySettings, refreshStatus, toastError],
  );

  const handleSaveAdvanced = useCallback(async () => {
    const advanced: Record<string, unknown> = {
      allow_external_binary_override: allowExternal,
      binary_path: allowExternal ? binaryPath.trim() || null : null,
    };
    for (const [key, label, , numeric] of ADVANCED_FIELDS) {
      const raw = (advancedForm[key] ?? "").trim();
      if (!raw) {
        advanced[key] = null; // back to default
      } else if (numeric) {
        const parsed = Number.parseInt(raw, 10);
        if (!Number.isInteger(parsed) || String(parsed) !== raw) {
          setFrontendError(`${label} must be a whole number.`);
          return;
        }
        advanced[key] = parsed;
      } else {
        advanced[key] = raw;
      }
    }
    if (await saveSettings({ advanced: advanced as AdvancedSettings })) {
      toaster.toast({ title: "Surround Sound Fix", body: "Advanced settings saved." });
    }
  }, [advancedForm, allowExternal, binaryPath, saveSettings]);

  const handleResetAdvanced = useCallback(async () => {
    const advanced: Record<string, null> = {
      allow_external_binary_override: null,
      binary_path: null,
    };
    for (const [key] of ADVANCED_FIELDS) {
      advanced[key] = null;
    }
    await saveSettings({ advanced: advanced as unknown as AdvancedSettings });
  }, [saveSettings]);

  const presetOptions = (Object.keys(PRESET_LABELS) as LatencyPreset[]).map((preset) => ({
    data: preset,
    label: `${PRESET_LABELS[preset]} (${presetsMs[preset]} ms)`,
  }));

  const lastError = status?.last_error ?? null;
  const locked = isMutating || isTransitioning || status === null;

  return (
    <>
      <PanelSection title="Dolby Digital 5.1">
        <PanelSectionRow>
          <ToggleField
            label="AC-3 encoder"
            description={describeState(state, automation)}
            checked={isOn}
            disabled={locked}
            onChange={(value) => void handleToggleEncoder(value)}
          />
        </PanelSectionRow>
        <PanelSectionRow>
          <div style={{ fontSize: "0.85em" }}>TV: {describeDisplay(display)}</div>
        </PanelSectionRow>
      </PanelSection>

      <PanelSection title="Settings">
        <PanelSectionRow>
          <ToggleField
            label="Auto-start"
            description="Turn on automatically when a TV that needs Dolby Digital for 5.1 is connected. Stops before sleep and resumes after waking."
            checked={settings?.auto_start ?? false}
            disabled={settings === null || isMutating}
            onChange={(value) => void saveSettings({ auto_start: value })}
          />
        </PanelSectionRow>
        <PanelSectionRow>
          <DropdownItem
            label="Latency"
            description="Higher means fewer stutters but more audio delay. Changing it while on restarts the encoder (a short gap)."
            rgOptions={presetOptions}
            selectedOption={settings?.latency_preset ?? "normal"}
            disabled={settings === null || isMutating}
            onChange={(option) => void saveSettings({ latency_preset: option.data as LatencyPreset })}
          />
        </PanelSectionRow>
      </PanelSection>

      {(lastError || frontendError) && (
        <PanelSection title="Last Error">
          <PanelSectionRow>
            <div>{lastError ? `${lastError.code}: ${lastError.message}` : frontendError}</div>
          </PanelSectionRow>
          {lastError?.detail && (
            <PanelSectionRow>
              <div style={{ fontSize: "0.8em" }}>{lastError.detail}</div>
            </PanelSectionRow>
          )}
          {lastError?.hint && (
            <PanelSectionRow>
              <div style={{ fontSize: "0.8em" }}>Hint: {lastError.hint}</div>
            </PanelSectionRow>
          )}
        </PanelSection>
      )}

      <PanelSection title="Advanced">
        <PanelSectionRow>
          <ToggleField
            label="Show advanced settings"
            description="Leave blank to use the default. Saved; applied on next start."
            checked={showAdvanced}
            onChange={(value) => setShowAdvanced(value)}
          />
        </PanelSectionRow>

        {showAdvanced && (
          <>
            {ADVANCED_FIELDS.map(([key, label, placeholder, numeric]) => (
              <PanelSectionRow key={key}>
                <TextField
                  label={`${label} (default ${placeholder})`}
                  value={advancedForm[key] ?? ""}
                  mustBeNumeric={numeric}
                  disabled={isMutating}
                  onChange={(event) => {
                    const value = event.target.value;
                    setAdvancedForm((current) => ({ ...current, [key]: value }));
                  }}
                />
              </PanelSectionRow>
            ))}
            <PanelSectionRow>
              <ToggleField
                label="External encoder binary"
                description="Development only: use a pw-ac3-live binary outside the plugin"
                checked={allowExternal}
                disabled={isMutating}
                onChange={(value) => setAllowExternal(value)}
              />
            </PanelSectionRow>
            {allowExternal && (
              <PanelSectionRow>
                <TextField
                  label="External binary path"
                  value={binaryPath}
                  disabled={isMutating}
                  onChange={(event) => setBinaryPath(event.target.value)}
                />
              </PanelSectionRow>
            )}
            <PanelSectionRow>
              <ButtonItem layout="below" disabled={isMutating} onClick={() => void handleSaveAdvanced()}>
                Save advanced settings
              </ButtonItem>
            </PanelSectionRow>
            <PanelSectionRow>
              <ButtonItem layout="below" disabled={isMutating} onClick={() => void handleResetAdvanced()}>
                Reset advanced to defaults
              </ButtonItem>
            </PanelSectionRow>
            <PanelSectionRow>
              <div style={{ fontSize: "0.8em" }}>
                Encoder PID: {status?.pid ?? "-"} · State: {state ?? "-"}
              </div>
            </PanelSectionRow>
          </>
        )}
      </PanelSection>
    </>
  );
}

export default definePlugin(() => {
  // Global listener so automatic actions (TV detected, wake from sleep) show a toast even
  // when the plugin panel is closed.
  const noticeListener = addEventListener<[message: string]>("automation_notice", (message) => {
    toaster.toast({ title: "Surround Sound Fix", body: message });
  });

  return {
    name: "Surround Sound Fix",
    titleView: <div className={staticClasses.Title}>Surround Sound Fix</div>,
    content: <Content />,
    icon: <FaVolumeUp />,
    onDismount() {
      removeEventListener("automation_notice", noticeListener);
    },
  };
});
