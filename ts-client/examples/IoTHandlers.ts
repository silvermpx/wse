/**
 * Example: IoT/monitoring event handlers for WSE.
 */

export interface SensorReading {
  deviceId: string;
  metric: string;
  value: number;
  unit: string;
  timestamp: string;
}

export function registerIoTHandlers() {
  window.addEventListener('sensor_reading', ((e: CustomEvent<SensorReading>) => {
    console.log('Sensor:', e.detail.deviceId, e.detail.metric, e.detail.value);
  }) as EventListener);

  window.addEventListener('alert', ((e: CustomEvent<{ deviceId: string; severity: string; message: string }>) => {
    console.warn('Alert:', e.detail.severity, e.detail.message);
  }) as EventListener);

  window.addEventListener('device_status', ((e: CustomEvent<{ deviceId: string; online: boolean }>) => {
    console.log('Device', e.detail.deviceId, e.detail.online ? 'online' : 'offline');
  }) as EventListener);
}
