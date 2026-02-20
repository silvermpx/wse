// =============================================================================
// WebSocket Engine - Event Transformer
// =============================================================================

import { WSMessage } from '../types';
import { INTERNAL_TO_WS_EVENT_TYPE_MAP } from '../constants';

export class EventTransformer {
  // ---------------------------------------------------------------------------
  // Main Transformation
  // ---------------------------------------------------------------------------

  transformForWS(event: any, sequence: number = 0): WSMessage {
    const eventType = event.event_type || event.type;

    // Map internal event type to WebSocket event type
    const wsEventType = INTERNAL_TO_WS_EVENT_TYPE_MAP[eventType] || eventType?.toLowerCase() || 'unknown';

    // Extract metadata
    const metadata = event._metadata || {};

    const payload = this.transformPayload(wsEventType, event);

    const wsEvent: WSMessage = {
      v: 2,
      id: metadata.event_id || event.event_id || crypto.randomUUID(),
      t: wsEventType,
      ts: metadata.timestamp || event.timestamp || new Date().toISOString(),
      seq: sequence,
      p: payload,
    };

    return wsEvent;
  }

  private transformPayload(wsEventType: string, event: any): any {
    // Generic payload extraction for all event types.
    // Application-specific transformations should be registered
    // via custom handlers rather than hardcoded here.
    return this.extractGenericPayload(event);
  }

  private extractGenericPayload(event: any): any {
    const payload = event.payload || event;

    if ('_metadata' in payload) {
      const { _metadata, ...rest } = payload;
      return rest;
    }

    if ('type' in payload && !('event_type' in payload)) {
      payload.event_type = payload.type;
    }

    return payload;
  }

  // ---------------------------------------------------------------------------
  // Reverse Transformation (WS to Internal)
  // ---------------------------------------------------------------------------

  transformFromWS(wsMessage: WSMessage): any {
    const internalEvent: any = {
      event_type: this.mapWSEventTypeToInternal(wsMessage.t),
      timestamp: wsMessage.ts,
      event_id: wsMessage.id,
      sequence: wsMessage.seq,
      ...wsMessage.p,
    };

    return internalEvent;
  }

  private mapWSEventTypeToInternal(wsType: string): string {
    for (const [internal, ws] of Object.entries(INTERNAL_TO_WS_EVENT_TYPE_MAP)) {
      if (ws === wsType) {
        return internal;
      }
    }

    return wsType.charAt(0).toUpperCase() + wsType.slice(1);
  }

  // ---------------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------------

  isKnownEventType(eventType: string): boolean {
    return eventType in INTERNAL_TO_WS_EVENT_TYPE_MAP;
  }

  getWSEventType(internalType: string): string {
    return INTERNAL_TO_WS_EVENT_TYPE_MAP[internalType] || internalType.toLowerCase();
  }
}

// Singleton instance
export const eventTransformer = new EventTransformer();
