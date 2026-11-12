// =============================================================================
// WebSocket Engine - Security Manager
// =============================================================================

import { logger } from './logger.js';

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
           'KEY_EXCHANGE_FAILED' | 'REPLAY_ATTACK' | 'NEGOTIATION_FAILED' |
           'IV_REUSE',
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

  // In-flight ECDH derivation (resolves true once the AES key exists, false on
  // failure) and a generation counter bumped by every reset, so a derivation
  // started for a connection that has since closed can never install its key
  // for the next connection.
  private keyDerivation: Promise<boolean> | null = null;
  private keyGeneration = 0;

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

      // The transport AES key is NOT generated here -- it is derived from the
      // ECDH exchange (deriveSharedSecret) after server_hello. Generating a
      // standalone random key here and using it before the exchange encrypted
      // client_hello with a key the server does not have, so the server dropped
      // the frame and the handshake deadlocked. Only the ECDH keypair is
      // generated (below); transport encryption activates once the shared
      // secret exists (isTransportEncryptionReady()).
      if (this.encryptionEnabled) {
        logger.info('Encryption requested; AES key derived via ECDH after server_hello');
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

  async generateKeyPair(): Promise<void> {
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

  async getPublicKeyRaw(): Promise<string | null> {
    if (!this.sessionKeyPair) return null;
    try {
      const raw = await crypto.subtle.exportKey('raw', this.sessionKeyPair.publicKey);
      return this.arrayBufferToBase64(raw);
    } catch (error) {
      logger.error('Failed to export raw public key:', error);
      return null;
    }
  }

  async setServerPublicKeyRaw(b64key: string): Promise<void> {
    const generation = this.keyGeneration;
    const run = (async () => {
      const keyData = this.base64ToArrayBuffer(b64key);
      const serverKey = await crypto.subtle.importKey(
        'raw', keyData,
        { name: 'ECDH', namedCurve: 'P-256' },
        false, []
      );
      if (generation !== this.keyGeneration) {
        throw new SecurityError('Key exchange superseded by a newer connection', 'KEY_EXCHANGE_FAILED');
      }
      if (!this.sessionKeyPair) {
        throw new SecurityError('No local ECDH key pair', 'KEY_EXCHANGE_FAILED');
      }
      this.serverPublicKey = serverKey;
      await this.deriveSharedSecret(generation);
      if (generation !== this.keyGeneration) {
        throw new SecurityError('Key exchange superseded by a newer connection', 'KEY_EXCHANGE_FAILED');
      }
      this.encryptionEnabled = true;
      logger.info('E2E encryption enabled via raw key exchange (ECDH P-256 + AES-GCM-256)');
    })();

    const derivation = run.then(() => true, () => false);
    this.keyDerivation = derivation;
    try {
      await run;
    } catch (error) {
      if (generation === this.keyGeneration) {
        this.serverPublicKey = null;
      }
      if (error instanceof SecurityError) throw error;
      throw new SecurityError('Failed to import raw server public key', 'KEY_EXCHANGE_FAILED', error);
    } finally {
      if (this.keyDerivation === derivation) {
        this.keyDerivation = null;
      }
    }
  }

  /** True while a server key has been received and the AES key is being derived. */
  hasPendingKeyDerivation(): boolean {
    return this.keyDerivation !== null;
  }

  /**
   * Resolve true once the transport key exists: immediately when it already
   * does, after the in-flight derivation otherwise. Resolves false when there
   * is no key and none is being derived, or when the derivation fails.
   */
  async waitForTransportKey(): Promise<boolean> {
    if (this.isTransportEncryptionReady()) return true;
    const pending = this.keyDerivation;
    if (!pending) return false;
    const ok = await pending;
    return ok && this.isTransportEncryptionReady();
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

  private async deriveSharedSecret(generation: number = this.keyGeneration): Promise<void> {
    if (!this.sessionKeyPair || !this.serverPublicKey) return;

    try {
      const sharedSecretBits = await crypto.subtle.deriveBits(
        { name: 'ECDH', public: this.serverPublicKey },
        this.sessionKeyPair.privateKey,
        256
      );

      const sharedSecret = await crypto.subtle.importKey(
        'raw', sharedSecretBits, 'HKDF', false, ['deriveKey']
      );

      // RFC 5869: salt = empty (zero-salt default), info = context label
      // Must match server.rs, security.rs, and Python client exactly
      const encryptionKey = await crypto.subtle.deriveKey(
        {
          name: 'HKDF',
          hash: 'SHA-256',
          salt: new Uint8Array(0),
          info: new TextEncoder().encode('wse-encryption/aes-gcm-key')
        },
        sharedSecret,
        { name: 'AES-GCM', length: 256 },
        true,
        ['encrypt', 'decrypt']
      );

      // Install both keys together, and only for the connection that asked.
      if (generation !== this.keyGeneration) return;
      this.sharedSecret = sharedSecret;
      this.encryptionKey = encryptionKey;

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

      // Draw a 96-bit IV and guard against reuse under the same key. IV reuse
      // is catastrophic for AES-GCM (it leaks the GHASH auth key), so we CHECK
      // the recent-IV set before using one rather than only recording it. With
      // a CSPRNG the redraw effectively never fires (birthday bound ~2^48), but
      // consulting the set is what makes the guard real instead of write-only.
      let iv = crypto.getRandomValues(new Uint8Array(12));
      let ivHex = Array.from(iv).map(b => b.toString(16).padStart(2, '0')).join('');
      for (let attempt = 0; this.usedIVs.has(ivHex) && attempt < 8; attempt++) {
        iv = crypto.getRandomValues(new Uint8Array(12));
        ivHex = Array.from(iv).map(b => b.toString(16).padStart(2, '0')).join('');
      }
      if (this.usedIVs.has(ivHex)) {
        // Could not obtain a fresh IV — refuse to encrypt rather than reuse one.
        throw new SecurityError('Unable to generate a unique IV', 'IV_REUSE');
      }

      // Record it; evict oldest when the window is full.
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
      throw new SecurityError('Cannot send: encryption key not ready', 'ENCRYPTION_FAILED');
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
    // Not an E:-prefixed encrypted frame
    return null;
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
    // Transport keys are never rotated here: the ECDH key pair and the derived
    // AES key belong to one connection (a fresh pair is generated for every
    // client_hello and dropped on close). Replacing them on a timer could swap
    // the key pair between client_hello and server_hello, or install a random
    // AES key the server does not have.
    logger.debug('Rotating signing key');

    try {
      if (this.messageSigningEnabled) {
        await this.generateSigningKey();
      }
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

  /**
   * True only once the ECDH shared secret has been derived (after server_hello),
   * i.e. when outbound transport encryption is actually safe. The send path
   * gates on THIS, not isEncryptionEnabled(), so client_hello (sent before the
   * key exchange) goes plaintext instead of being encrypted with a key the
   * server does not yet have -- which would deadlock the handshake.
   */
  isTransportEncryptionReady(): boolean {
    return this.encryptionEnabled && this.sharedSecret !== null;
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

  resetEncryption(): void {
    this.keyGeneration++;
    this.keyDerivation = null;
    this.encryptionKey = null;
    this.sharedSecret = null;
    this.serverPublicKey = null;
    this.sessionKeyPair = null;
    if (this.usedIVs) {
      this.usedIVs.clear();
    }
    // Keep encryptionEnabled=true so sendClientHello knows to include pubkey
    // A fresh keypair will be generated in sendClientHello via getPublicKeyRaw -> generateKeyPair
    logger.debug('Encryption state reset for reconnect');
  }

  destroy(): void {
    this.keyGeneration++;
    this.keyDerivation = null;
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
