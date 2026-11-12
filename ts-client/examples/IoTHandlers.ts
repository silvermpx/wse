/**
 * Example: IoT/monitoring handlers for WSE.
 *
 *   const handlers = useMemo(() => createIoTHandlers({ onReading: store.addReading }), []);
 *   useWSE('cookie', ['sensors'], { registerHandlers: handlers });
 */

import type { HandlerRegistry } from '../services/WSESession.js';
import type { WSMessage } from '../types.js';

export interface SensorReading {
  deviceId: string;
  metric: string;
  value: number;
  unit: string;
  timestamp: string;
}

export interface IoTCallbacks {
  onReading: (reading: SensorReading) => void;
  onAlert?: (alert: { deviceId: string; severity: string; message: string }) => void;
  onDeviceStatus?: (status: { deviceId: string; online: boolean }) => void;
}

export function createIoTHandlers(callbacks: IoTCallbacks) {
  return (processor: HandlerRegistry): void => {
    processor.registerHandler('sensor_reading', (msg: WSMessage) => callbacks.onReading(msg.p as SensorReading));
    processor.registerHandler('alert', (msg: WSMessage) => callbacks.onAlert?.(msg.p));
    processor.registerHandler('device_status', (msg: WSMessage) => callbacks.onDeviceStatus?.(msg.p));
  };
}
