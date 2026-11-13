// =============================================================================
// WebSocket Engine - Security Manager
// =============================================================================

import { E2eOffer, type E2eSession, type E2eTrust } from './e2eSession.js';
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
           'KEY_GENERATION_FAILED' | 'KEY_EXCHANGE_FAILED' | 'REPLAY_ATTACK' |
           'IV_REUSE',
    public readonly details?: any
  ) {
    super(message);
    this.name = 'SecurityError';
  }
}

export class SecurityManager {
  private encryptionEnabled = false;
  private encryptionKey: CryptoKey | null = null;

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

  // E2E sessions (with encryption, unless the first design is asked for): a
  // fresh offer per connection, and the session it becomes.
  private e2eTrust: E2eTrust | null = null;
  private e2eOffer: E2eOffer | null = null;
  private e2eSession: E2eSession | null = null;

  async initialize(config: { encryptionEnabled: boolean }) {
    try {
      this.encryptionEnabled = config.encryptionEnabled;

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

      await this.generateKeyPair();
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

  /** Use E2E sessions with this trust; null for the first design. */
  configureE2e(trust: E2eTrust | null): void {
    this.e2eTrust = trust;
  }

  isSessionMode(): boolean {
    return this.e2eTrust !== null;
  }

  /** client_hello.p.e2e: a fresh offer for this connection. */
  async createOffer(): Promise<Record<string, unknown>> {
    this.e2eSession = null;
    const generation = this.keyGeneration;
    const offer = await E2eOffer.create();
    if (generation === this.keyGeneration) this.e2eOffer = offer;
    return offer.message();
  }

  /** Complete the handshake from server_hello.p; rejects when it does not check. */
  async acceptServerHello(payload: any): Promise<void> {
    const offer = this.e2eOffer;
    if (!offer || !this.e2eTrust || typeof payload?.e2e !== 'object' || payload.e2e === null) {
      throw new SecurityError('the server did not answer the E2E offer', 'KEY_EXCHANGE_FAILED');
    }
    const generation = this.keyGeneration;
    const session = await offer.accept(payload.e2e, String(payload.connection_id ?? ''), this.e2eTrust);
    if (generation !== this.keyGeneration) return; // reset meanwhile: another connection
    this.e2eSession = session;
    this.e2eOffer = null;
  }

  /**
   * The plaintext of an E:-prefixed frame as bytes (JSON, or M: msgpack).
   * In a session a frame that is not the next one rejects with an E2eError:
   * the connection must end.
   */
  async decryptBytesFromTransport(data: ArrayBuffer): Promise<Uint8Array | null> {
    const view = new Uint8Array(data);
    if (view.length < 2 || view[0] !== 69 || view[1] !== 58) return null;
    if (this.e2eSession) {
      return this.e2eSession.open(new Uint8Array(view));
    }
    if (!this.encryptionEnabled || !this.encryptionKey) return null;
    try {
      const decrypted = await crypto.subtle.decrypt(
        { name: 'AES-GCM', iv: view.slice(2, 14) },
        this.encryptionKey,
        view.slice(14),
      );
      return new Uint8Array(decrypted);
    } catch (error) {
      throw new SecurityError('Decryption failed', 'DECRYPTION_FAILED', error);
    }
  }

  async encryptForTransport(data: string): Promise<ArrayBuffer> {
    if (this.e2eSession) {
      const frame = await this.e2eSession.seal(new TextEncoder().encode(data));
      return frame.buffer;
    }
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
    if (this.e2eTrust) return this.e2eSession !== null;
    return this.encryptionEnabled && this.sharedSecret !== null;
  }

  resetEncryption(): void {
    this.keyGeneration++;
    this.keyDerivation = null;
    this.e2eOffer = null;
    this.e2eSession = null;
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
    this.e2eOffer = null;
    this.e2eSession = null;
    if (this.usedIVs) {
      this.usedIVs.clear();
    }
    this.encryptionKey = null;
    this.sessionKeyPair = null;
    this.sharedSecret = null;
    this.serverPublicKey = null;
    this.encryptionEnabled = false;

    logger.info('Security manager destroyed, all keys and caches cleared');
  }
}

export const securityManager = new SecurityManager();
