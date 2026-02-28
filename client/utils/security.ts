// =============================================================================
// WebSocket Engine - Security Manager
// =============================================================================

import { logger } from './logger';

// Check for Web Crypto API availability
if (typeof window !== 'undefined' && !window.crypto?.subtle) {
  logger.warn('Web Crypto API not available. HTTPS required for crypto.subtle');
}

// Security error types
export class SecurityError extends Error {
  constructor(
    message: string,
    public readonly code: 'ENCRYPTION_FAILED' | 'DECRYPTION_FAILED' |
           'SIGNING_FAILED' | 'VERIFICATION_FAILED' | 'KEY_GENERATION_FAILED' |
           'KEY_EXCHANGE_FAILED' | 'REPLAY_ATTACK' | 'NEGOTIATION_FAILED',
    public readonly details?: any
  ) {
    super(message);
    this.name = 'SecurityError';
  }
}

export class SecurityManager {
  private encryptionEnabled = false;
  private messageSigningEnabled = false;
  private encryptionKey: CryptoKey | null = null;
  private signingKey: CryptoKey | null = null;
  private keyRotationInterval: number | null = null;
  private lastKeyRotation: number | null = null;
  private keyRotationTimer: NodeJS.Timeout | null = null;
  private keyRotationFailures = 0;

  private sessionKeyPair: CryptoKeyPair | null = null;
  private sharedSecret: CryptoKey | null = null;
  private serverPublicKey: CryptoKey | null = null;

  private usedIVs = new Set<string>();

  private backendCompatibilityMode = false;
  private readonly supportedAlgorithms = {
    encryption: 'AES-GCM',
    signing: 'HMAC-SHA256',
    keyExchange: 'ECDH-P256'
  };

  async initialize(config: {
    encryptionEnabled: boolean;
    messageSigningEnabled: boolean;
    keyRotationInterval?: number;
    backendCompatibilityMode?: boolean;
  }) {
    try {
      this.encryptionEnabled = config.encryptionEnabled;
      this.messageSigningEnabled = config.messageSigningEnabled;
      this.keyRotationInterval = config.keyRotationInterval || 3600000;
      this.backendCompatibilityMode = config.backendCompatibilityMode || false;

      if (this.encryptionEnabled) {
        await this.generateEncryptionKey();
        logger.info('Encryption enabled for WebSocket messages with AES-GCM');
      }

      if (this.messageSigningEnabled) {
        await this.generateSigningKey();
        logger.info('Message signing enabled for WebSocket messages with HMAC-SHA256');
      }

      await this.generateKeyPair();

      if (this.encryptionEnabled || this.messageSigningEnabled) {
        this.scheduleKeyRotation();
      }
    } catch (error) {
      throw new SecurityError(
        'Failed to initialize security manager',
        'KEY_GENERATION_FAILED',
        error
      );
    }
  }

  private async generateEncryptionKey(): Promise<void> {
    try {
      this.encryptionKey = await crypto.subtle.generateKey(
        { name: 'AES-GCM', length: 256 },
        true,
        ['encrypt', 'decrypt']
      );
    } catch (error) {
      throw new SecurityError('Failed to generate encryption key', 'KEY_GENERATION_FAILED', error);
    }
  }

  private async generateSigningKey(): Promise<void> {
    try {
      this.signingKey = await crypto.subtle.generateKey(
        { name: 'HMAC', hash: 'SHA-256' },
        true,
        ['sign', 'verify']
      );
    } catch (error) {
      throw new SecurityError('Failed to generate signing key', 'KEY_GENERATION_FAILED', error);
    }
  }

  private async generateKeyPair(): Promise<void> {
    try {
      this.sessionKeyPair = await crypto.subtle.generateKey(
        { name: 'ECDH', namedCurve: 'P-256' },
        true,
        ['deriveKey', 'deriveBits']
      );
    } catch (error) {
      throw new SecurityError('Failed to generate key pair', 'KEY_GENERATION_FAILED', error);
    }
  }

  async getPublicKey(): Promise<JsonWebKey | null> {
    if (!this.sessionKeyPair) return null;
    try {
      return await crypto.subtle.exportKey('jwk', this.sessionKeyPair.publicKey);
    } catch (error) {
      logger.error('Failed to export public key:', error);
      return null;
    }
  }

  async setServerPublicKey(publicKeyJwk: JsonWebKey): Promise<void> {
    try {
      if (!publicKeyJwk.kty || publicKeyJwk.kty !== 'EC') {
        throw new Error('Invalid key type for ECDH');
      }
      if (!publicKeyJwk.crv || publicKeyJwk.crv !== 'P-256') {
        throw new Error('Invalid curve for ECDH');
      }

      this.serverPublicKey = await crypto.subtle.importKey(
        'jwk', publicKeyJwk,
        { name: 'ECDH', namedCurve: 'P-256' },
        false, []
      );

      if (this.sessionKeyPair && this.serverPublicKey) {
        await this.deriveSharedSecret();
      }
    } catch (error) {
      this.serverPublicKey = null;
      throw new SecurityError('Failed to import server public key', 'KEY_EXCHANGE_FAILED', error);
    }
  }

  private async deriveSharedSecret(): Promise<void> {
    if (!this.sessionKeyPair || !this.serverPublicKey) return;

    try {
      const sharedSecretBits = await crypto.subtle.deriveBits(
        { name: 'ECDH', public: this.serverPublicKey },
        this.sessionKeyPair.privateKey,
        256
      );

      this.sharedSecret = await crypto.subtle.importKey(
        'raw', sharedSecretBits, 'HKDF', false, ['deriveKey']
      );

      this.encryptionKey = await crypto.subtle.deriveKey(
        {
          name: 'HKDF',
          hash: 'SHA-256',
          salt: new TextEncoder().encode('wse-encryption'),
          info: new TextEncoder().encode('aes-gcm-key')
        },
        this.sharedSecret,
        { name: 'AES-GCM', length: 256 },
        true,
        ['encrypt', 'decrypt']
      );

      logger.info('Derived shared encryption key from ECDH exchange');
    } catch (error) {
      throw new SecurityError('Failed to derive shared secret', 'KEY_EXCHANGE_FAILED', error);
    }
  }

  async encryptMessage(data: string): Promise<ArrayBuffer | null> {
    if (!this.encryptionEnabled || !this.encryptionKey) return null;

    try {
      const encoder = new TextEncoder();
      const encoded = encoder.encode(data);

      const iv = crypto.getRandomValues(new Uint8Array(12));
      const ivHex = Array.from(iv).map(b => b.toString(16).padStart(2, '0')).join('');

      // Track used IVs; evict oldest when cache is full
      this.usedIVs.add(ivHex);
      if (this.usedIVs.size > 10000) {
        const first = this.usedIVs.values().next().value;
        if (first) this.usedIVs.delete(first);
      }

      const encrypted = await crypto.subtle.encrypt(
        { name: 'AES-GCM', iv: iv as BufferSource },
        this.encryptionKey,
        encoded
      );

      const combined = new Uint8Array(iv.length + encrypted.byteLength);
      combined.set(iv, 0);
      combined.set(new Uint8Array(encrypted), iv.length);

      return combined.buffer;
    } catch (error) {
      throw new SecurityError('Encryption failed', 'ENCRYPTION_FAILED', error);
    }
  }

  async decryptMessage(data: ArrayBuffer): Promise<string | null> {
    if (!this.encryptionEnabled || !this.encryptionKey) return null;

    try {
      const dataArray = new Uint8Array(data);
      const iv = dataArray.slice(0, 12);
      const encrypted = dataArray.slice(12);

      const decrypted = await crypto.subtle.decrypt(
        { name: 'AES-GCM', iv: iv },
        this.encryptionKey,
        encrypted
      );

      return new TextDecoder().decode(decrypted);
    } catch (error) {
      throw new SecurityError('Decryption failed', 'DECRYPTION_FAILED', error);
    }
  }

  async encryptForTransport(data: string): Promise<ArrayBuffer> {
    const encrypted = await this.encryptMessage(data);
    if (!encrypted) {
      const encoded = new TextEncoder().encode(data);
      return encoded.buffer.slice(encoded.byteOffset, encoded.byteOffset + encoded.byteLength) as ArrayBuffer;
    }

    const prefix = new TextEncoder().encode('E:');
    const combined = new Uint8Array(prefix.length + encrypted.byteLength);
    combined.set(prefix, 0);
    combined.set(new Uint8Array(encrypted), prefix.length);

    return combined.buffer;
  }

  async decryptFromTransport(data: ArrayBuffer): Promise<string | null> {
    const view = new Uint8Array(data);
    if (view.length >= 2 && view[0] === 69 && view[1] === 58) {
      return this.decryptMessage(data.slice(2));
    }
    return this.decryptMessage(data);
  }

  async signMessage(data: any): Promise<string | null> {
    if (!this.messageSigningEnabled || !this.signingKey) return null;

    try {
      const encoder = new TextEncoder();
      const encoded = encoder.encode(JSON.stringify(data));
      const signature = await crypto.subtle.sign('HMAC', this.signingKey, encoded);
      return this.arrayBufferToBase64(signature);
    } catch (error) {
      logger.error('Message signing failed:', error);
      return null;
    }
  }

  async verifySignature(data: any, signature: string): Promise<boolean> {
    if (!this.messageSigningEnabled || !this.signingKey) return true;

    try {
      const encoder = new TextEncoder();
      const encoded = encoder.encode(JSON.stringify(data));
      const signatureBuffer = this.base64ToArrayBuffer(signature);

      return await crypto.subtle.verify('HMAC', this.signingKey, signatureBuffer, encoded);
    } catch (error) {
      logger.error('Signature verification failed:', error);
      return false;
    }
  }

  async negotiateSecurity(serverCapabilities: {
    encryption?: boolean;
    signing?: boolean;
    algorithms?: string[];
    publicKey?: JsonWebKey;
  }): Promise<{
    encryption: boolean;
    signing: boolean;
    keyExchange: boolean;
    algorithms: { encryption: string; signing: string; keyExchange: string; };
  }> {
    try {
      this.encryptionEnabled = this.encryptionEnabled && (serverCapabilities.encryption ?? false);
      this.messageSigningEnabled = this.messageSigningEnabled && (serverCapabilities.signing ?? false);

      if (serverCapabilities.publicKey && this.encryptionEnabled) {
        await this.setServerPublicKey(serverCapabilities.publicKey);
      }

      const negotiated = {
        encryption: this.encryptionEnabled,
        signing: this.messageSigningEnabled,
        keyExchange: !!serverCapabilities.publicKey,
        algorithms: this.supportedAlgorithms
      };

      logger.info('Security negotiated:', negotiated);
      return negotiated;
    } catch (error) {
      throw new SecurityError('Security negotiation failed', 'NEGOTIATION_FAILED', error);
    }
  }

  private scheduleKeyRotation(): void {
    if (!this.keyRotationInterval || this.keyRotationTimer) return;

    this.keyRotationTimer = setInterval(async () => {
      try {
        await this.rotateKeys();
        this.keyRotationFailures = 0;
      } catch (error) {
        logger.error('Key rotation failed:', error);
        this.keyRotationFailures++;
        if (this.keyRotationFailures >= 3) {
          logger.error('Key rotation disabled after 3 consecutive failures');
          if (this.keyRotationTimer) {
            clearInterval(this.keyRotationTimer);
            this.keyRotationTimer = null;
          }
        }
      }
    }, this.keyRotationInterval);
  }

  async rotateKeys(): Promise<void> {
    // Skip rotation if ECDH shared secret is active -- rotating would
    // replace the ECDH-derived key with a random one, breaking encryption
    // since the server still has the old shared secret.
    if (this.sharedSecret) {
      logger.debug('Skipping key rotation (ECDH session active)');
      return;
    }

    logger.info('Rotating cryptographic keys');

    try {
      if (this.encryptionEnabled) {
        await this.generateEncryptionKey();
        this.usedIVs.clear();
      }
      if (this.messageSigningEnabled) {
        await this.generateSigningKey();
      }
      await this.generateKeyPair();
      this.lastKeyRotation = Date.now();
      logger.info('Keys rotated successfully');
    } catch (error) {
      logger.error('Key rotation failed:', error);
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------------

  private arrayBufferToBase64(buffer: ArrayBuffer): string {
    const bytes = new Uint8Array(buffer);
    let binary = '';
    for (let i = 0; i < bytes.byteLength; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  }

  private base64ToArrayBuffer(base64: string): ArrayBuffer {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  isEncryptionEnabled(): boolean {
    return this.encryptionEnabled;
  }

  isMessageSigningEnabled(): boolean {
    return this.messageSigningEnabled;
  }

  getSecurityInfo(): {
    encryptionEnabled: boolean;
    encryptionAlgorithm: string | null;
    messageSigningEnabled: boolean;
    signatureAlgorithm: string | null;
    keyRotationInterval: number | null;
    lastKeyRotation: number | null;
    keyExchangeEnabled: boolean;
    backendCompatibilityMode: boolean;
  } {
    return {
      encryptionEnabled: this.encryptionEnabled,
      encryptionAlgorithm: this.encryptionEnabled ? 'AES-GCM-256' : null,
      messageSigningEnabled: this.messageSigningEnabled,
      signatureAlgorithm: this.messageSigningEnabled ? 'HMAC-SHA256' : null,
      keyRotationInterval: this.keyRotationInterval,
      lastKeyRotation: this.lastKeyRotation,
      keyExchangeEnabled: !!this.sessionKeyPair,
      backendCompatibilityMode: this.backendCompatibilityMode,
    };
  }

  destroy(): void {
    if (this.keyRotationTimer) {
      clearInterval(this.keyRotationTimer);
      this.keyRotationTimer = null;
    }
    if (this.usedIVs) {
      this.usedIVs.clear();
    }
    this.encryptionKey = null;
    this.signingKey = null;
    this.sessionKeyPair = null;
    this.sharedSecret = null;
    this.serverPublicKey = null;
    this.encryptionEnabled = false;
    this.messageSigningEnabled = false;
    this.keyRotationFailures = 0;

    logger.info('Security manager destroyed, all keys and caches cleared');
  }
}

export const securityManager = new SecurityManager();
