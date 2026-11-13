"""Regression tests for the Rust utility classes and functions in wse_server."""

import datetime
import decimal
import uuid

import pytest

from wse_server import (
    EventFilter,
    EventPriority,
    RustCompressionManager,
    RustEventSequencer,
    RustPriorityMessageQueue,
    RustPriorityQueue,
    RustSequencer,
    RustTokenBucket,
    RustWSEServer,
    rust_aes_gcm_decrypt,
    rust_aes_gcm_encrypt,
    rust_compress,
    rust_decompress,
    rust_ecdh_derive_shared_secret,
    rust_ecdh_generate_keypair,
    rust_hmac_sha256,
    rust_jwt_decode,
    rust_jwt_encode,
    rust_match_event,
    rust_should_compress,
    rust_transform_event,
)

# ---------------------------------------------------------------------------
# Event filters
# ---------------------------------------------------------------------------


class TestFilters:
    def test_or_does_not_hide_sibling_conditions(self):
        """{"account": "A", "$or": [...]} requires both the account and the $or."""
        criteria = {"account": "A", "$or": [{"t": "fill"}, {"t": "cancel"}]}
        assert rust_match_event({"account": "A", "t": "fill"}, criteria)
        assert not rust_match_event({"account": "B", "t": "fill"}, criteria)
        assert not EventFilter.matches({"account": "B", "t": "fill"}, criteria)

    def test_and_and_or_both_apply(self):
        criteria = {"$and": [{"a": 1}], "$or": [{"b": 1}, {"b": 2}]}
        assert rust_match_event({"a": 1, "b": 2}, criteria)
        assert not rust_match_event({"a": 1, "b": 3}, criteria)
        assert not rust_match_event({"a": 2, "b": 1}, criteria)

    def test_tuple_operand_is_accepted(self):
        assert rust_match_event({"a": 1}, {"$or": ({"a": 1},)})
        assert not rust_match_event({"a": 2}, {"$or": ({"a": 1},)})

    def test_malformed_logical_operand_raises(self):
        with pytest.raises(TypeError):
            rust_match_event({"a": 1}, {"$or": {"a": 1}})
        with pytest.raises(TypeError):
            rust_match_event({"a": 1}, {"$and": ["not a dict"]})

    def test_regex_still_anchored_and_cached(self):
        for _ in range(3):
            assert rust_match_event({"s": "AAPL"}, {"s": {"$regex": "AA"}})
            assert not rust_match_event({"s": "XAAPL"}, {"s": {"$regex": "AA"}})


# ---------------------------------------------------------------------------
# Compression and msgpack
# ---------------------------------------------------------------------------


class TestCompression:
    def test_cyclic_payload_raises_instead_of_aborting(self):
        cm = RustCompressionManager()
        d = {}
        d["self"] = d
        with pytest.raises(ValueError, match="cycle"):
            cm.pack_msgpack(d)

    def test_too_deep_payload_raises(self):
        cm = RustCompressionManager()
        deep = {}
        cur = deep
        for _ in range(200):
            cur["x"] = {}
            cur = cur["x"]
        with pytest.raises(ValueError, match="deeper"):
            cm.pack_msgpack(deep)

    def test_shared_but_acyclic_values_are_fine(self):
        cm = RustCompressionManager()
        shared = {"k": 1}
        packed = cm.pack_msgpack({"a": shared, "b": shared})
        assert cm.unpack_msgpack(packed) == {"a": {"k": 1}, "b": {"k": 1}}

    def test_unpack_event_falls_back_to_json(self):
        cm = RustCompressionManager()
        assert cm.unpack_event(b'{"t":"x"}') == {"t": "x"}
        assert cm.unpack_event(b"[1, 2]", is_msgpack=False) == [1, 2]

    def test_unpack_msgpack_rejects_trailing_bytes(self):
        cm = RustCompressionManager()
        packed = cm.pack_msgpack({"a": 1})
        with pytest.raises(RuntimeError):
            cm.unpack_msgpack(packed + b"junk")

    def test_invalid_compression_level_is_a_value_error(self):
        with pytest.raises(ValueError):
            RustCompressionManager(compression_level=10)
        cm = RustCompressionManager()
        with pytest.raises(ValueError):
            cm.compress(b"x" * 2000, level=0)

    def test_large_payload_roundtrip(self):
        cm = RustCompressionManager()
        data = b"wse " * 100_000  # above the detach threshold
        assert cm.decompress(cm.compress(data)) == data


# ---------------------------------------------------------------------------
# Event transformer
# ---------------------------------------------------------------------------


class TestTransformer:
    def test_converts_values_at_any_depth(self):
        uid = uuid.uuid4()
        event = {"event_type": "OrderPlaced", "order": {"customer": {"address": {"id": uid}}}}
        out = rust_transform_event(event, 1, {})
        assert out["p"]["order"]["customer"]["address"]["id"] == str(uid)

    def test_decimal_becomes_exact_string(self):
        out = rust_transform_event({"event_type": "x", "price": decimal.Decimal("10.10")}, 1, {})
        assert out["p"]["price"] == "10.10"

    def test_already_transformed_event_is_not_mutated(self):
        event = {"v": 1, "t": "x", "p": {}, "id": "1", "ts": "2026-01-01T00:00:00Z"}
        out = rust_transform_event(event, 7, {})
        assert out["seq"] == 7
        assert "seq" not in event

    def test_cycle_raises(self):
        payload = {"event_type": "x", "items": []}
        payload["items"].append(payload["items"])
        payload["items"].append(uuid.uuid4())  # forces conversion
        with pytest.raises(ValueError, match="cycle"):
            rust_transform_event(payload, 1, {})


# ---------------------------------------------------------------------------
# Priority queues and sequencers
# ---------------------------------------------------------------------------


class TestQueues:
    def test_low_priority_burst_cannot_evict_normal_work(self):
        q = RustPriorityMessageQueue(max_size=3, batch_size=10)
        for i in range(3):
            assert q.enqueue({"i": i}, 5)
        assert not q.enqueue({"bg": 1}, 1)  # BACKGROUND rejected, NORMAL kept
        stats = q.get_stats()
        assert stats["priority_distribution"][5] == 3
        assert stats["dropped_by_priority"][1] == 1
        assert stats["dropped_by_priority"][5] == 0

    def test_higher_priority_evicts_the_lowest_first(self):
        q = RustPriorityMessageQueue(max_size=2, batch_size=10)
        assert q.enqueue({"n": 1}, 5)
        assert q.enqueue({"bg": 1}, 1)
        assert q.enqueue({"hi": 1}, 8)  # evicts BACKGROUND, not NORMAL
        batch = q.dequeue_batch()
        assert [p for p, _ in batch] == [8, 5]

    def test_critical_never_evicts_high(self):
        q = RustPriorityMessageQueue(max_size=1, batch_size=10)
        assert q.enqueue({"h": 1}, 8)
        assert not q.enqueue({"c": 1}, 10)
        assert q.get_stats()["dropped_by_priority"][10] == 1

    def test_event_priority_members_are_accepted_as_priorities(self):
        q = RustPriorityMessageQueue(max_size=10, batch_size=10)
        assert q.enqueue({"a": 1}, EventPriority.HIGH)
        assert q.enqueue({"b": 1}, EventPriority.BACKGROUND)
        assert [p for p, _ in q.dequeue_batch()] == [8, 1]

    def test_processing_rate_measures_dequeued_messages(self):
        q = RustPriorityMessageQueue(max_size=100, batch_size=10)
        assert q.get_stats()["processing_rate"] == 0.0
        for i in range(25):
            q.enqueue({"i": i})
        while q.dequeue_batch():
            pass
        # 25 messages averaged over at least one second.
        assert 0.0 < q.get_stats()["processing_rate"] <= 25.0
        q.clear()
        assert q.get_stats()["processing_rate"] == 0.0

    def test_clear_resets_drop_statistics(self):
        q = RustPriorityMessageQueue(max_size=1, batch_size=10)
        q.enqueue({"a": 1}, 5)
        q.enqueue({"b": 1}, 3)
        q.clear()
        assert q.get_stats()["total_dropped"] == 0
        assert q.size == 0

    def test_sizes_are_validated_and_not_preallocated(self):
        with pytest.raises(ValueError):
            RustPriorityQueue(0)
        with pytest.raises(ValueError):
            RustPriorityMessageQueue(max_size=0)
        with pytest.raises(ValueError):
            RustSequencer(0)
        with pytest.raises(ValueError):
            RustEventSequencer(window_size=0)
        # A huge bound must not reserve its capacity up front.
        q = RustPriorityQueue(10**12)
        assert q.push(1, "x")
        assert RustSequencer(10**12).next_seq() == 1


# ---------------------------------------------------------------------------
# JWT claim encoding
# ---------------------------------------------------------------------------

SECRET = b"a-test-secret-that-is-long-enough!!"


class TestJwtEncode:
    def test_datetime_claims_become_timestamps(self):
        exp = datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC)
        token = rust_jwt_encode({"sub": "u", "exp": exp}, SECRET)
        claims = rust_jwt_decode(token, SECRET)
        assert claims["exp"] == int(exp.timestamp())

    def test_tuples_and_sets_become_arrays(self):
        exp = int(datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC).timestamp())
        claims = {"sub": "u", "exp": exp, "wse_topics": {"allow": ("chat.*",)}, "roles": {"a"}}
        decoded = rust_jwt_decode(rust_jwt_encode(claims, SECRET), SECRET)
        assert decoded["wse_topics"] == {"allow": ["chat.*"]}
        assert decoded["roles"] == ["a"]

    def test_unsupported_values_raise(self):
        with pytest.raises(TypeError):
            rust_jwt_encode({"sub": "u", "obj": object()}, SECRET)
        with pytest.raises(TypeError):
            rust_jwt_encode({"sub": "u", "big": 2**70}, SECRET)
        with pytest.raises(TypeError):
            rust_jwt_encode({"sub": "u", "x": float("nan")}, SECRET)


# ---------------------------------------------------------------------------
# Crypto helpers: byte-for-byte interop with the `cryptography` package
# ---------------------------------------------------------------------------


class TestCryptoInterop:
    def test_ecdh_hkdf_key_matches_cryptography(self):
        ec = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.ec")
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF

        ours_private, ours_public = rust_ecdh_generate_keypair()
        assert len(ours_private) == 32 and len(ours_public) == 65
        theirs = ec.generate_private_key(ec.SECP256R1())
        theirs_public = theirs.public_key().public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )

        key_rust = rust_ecdh_derive_shared_secret(ours_private, theirs_public)

        shared = theirs.exchange(
            ec.ECDH(), ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), ours_public)
        )
        key_py = HKDF(
            algorithm=hashes.SHA256(), length=32, salt=None, info=b"wse-encryption/aes-gcm-key"
        ).derive(shared)
        assert key_rust == key_py

    def test_aes_gcm_interop_both_ways(self):
        aead = pytest.importorskip("cryptography.hazmat.primitives.ciphers.aead")
        key = bytes(range(32))
        blob = rust_aes_gcm_encrypt(key, b"hello")
        assert aead.AESGCM(key).decrypt(blob[:12], blob[12:], None) == b"hello"
        nonce = b"\x01" * 12
        theirs = nonce + aead.AESGCM(key).encrypt(nonce, b"world", None)
        assert rust_aes_gcm_decrypt(key, theirs) == b"world"

    def test_aes_gcm_rejects_tampering_and_bad_input(self):
        key = bytes(32)
        blob = bytearray(rust_aes_gcm_encrypt(key, b"data"))
        blob[-1] ^= 1
        with pytest.raises(RuntimeError):
            rust_aes_gcm_decrypt(key, bytes(blob))
        with pytest.raises(ValueError):
            rust_aes_gcm_encrypt(b"short", b"x")
        with pytest.raises(ValueError):
            rust_aes_gcm_decrypt(key, b"too short")

    def test_invalid_peer_key_is_a_value_error(self):
        private, _ = rust_ecdh_generate_keypair()
        with pytest.raises(ValueError):
            rust_ecdh_derive_shared_secret(private, b"\x04" + bytes(64))

    def test_hmac_sha256_known_answer(self):
        tag = rust_hmac_sha256(b"Jefe", b"what do ya want for nothing?")
        assert tag.hex() == "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"


# ---------------------------------------------------------------------------
# Defaults and argument checks at the Python boundary
# ---------------------------------------------------------------------------


class TestBoundary:
    def test_documented_defaults_exist(self):
        data = b"x" * 2048
        assert rust_decompress(rust_compress(data)) == data
        assert rust_should_compress(data)
        assert not rust_should_compress(b"tiny")
        assert RustTokenBucket(2.0, 0.0).acquire()

    @pytest.mark.parametrize(
        "args",
        [(0.0, 1.0), (-1.0, 1.0), (float("nan"), 1.0), (10.0, -1.0), (10.0, float("inf"))],
    )
    def test_token_bucket_rejects_bad_configuration(self, args):
        with pytest.raises(ValueError):
            RustTokenBucket(*args)

    def test_token_bucket_rejects_initial_tokens_out_of_range(self):
        with pytest.raises(ValueError):
            RustTokenBucket(10.0, 1.0, initial_tokens=11.0)
        with pytest.raises(ValueError):
            RustTokenBucket(10.0, 1.0, initial_tokens=-1.0)

    @pytest.mark.parametrize("tokens", [-1.0, float("nan"), float("inf")])
    def test_acquire_rejects_tokens_that_would_corrupt_the_bucket(self, tokens):
        bucket = RustTokenBucket(10.0, 0.0)
        with pytest.raises(ValueError):
            bucket.acquire(tokens)
        assert bucket.tokens == 10.0


# ---------------------------------------------------------------------------
# JWT algorithms: tokens from any auth service
# ---------------------------------------------------------------------------

ALGORITHMS = [
    "HS256",
    "HS384",
    "HS512",
    "RS256",
    "RS384",
    "RS512",
    "PS256",
    "PS384",
    "PS512",
    "ES256",
    "ES384",
    "EdDSA",
]


@pytest.fixture(scope="module")
def key_pairs():
    """PEM key pairs made by the `cryptography` package, one per key kind."""
    pytest.importorskip("cryptography")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec, ed25519, rsa

    def pem(private):
        return (
            private.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ),
            private.public_key().public_bytes(
                serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
            ),
        )

    return {
        "RSA": pem(rsa.generate_private_key(public_exponent=65537, key_size=2048)),
        "P-256": pem(ec.generate_private_key(ec.SECP256R1())),
        "P-384": pem(ec.generate_private_key(ec.SECP384R1())),
        "Ed25519": pem(ed25519.Ed25519PrivateKey.generate()),
    }


def keys_for(alg, key_pairs):
    """(signing key, verification key) for `alg`."""
    if alg.startswith("HS"):
        secret = bytes(range(64))[: {"HS256": 32, "HS384": 48, "HS512": 64}[alg]]
        return secret, secret
    kind = {"RS": "RSA", "PS": "RSA", "ES256": "P-256", "ES384": "P-384", "EdDSA": "Ed25519"}
    pair = key_pairs[kind.get(alg, kind.get(alg[:2]))]
    return pair


class TestJwtAlgorithms:
    @pytest.mark.parametrize("alg", ALGORITHMS)
    def test_every_standard_algorithm_round_trips(self, alg, key_pairs):
        signing, verifying = keys_for(alg, key_pairs)
        token = rust_jwt_encode({"sub": "u1", "exp": 4_000_000_000}, signing, algorithm=alg)
        claims = rust_jwt_decode(token, verifying, algorithm=alg)
        assert claims is not None and claims["sub"] == "u1"

    @pytest.mark.parametrize("alg", ["RS256", "ES256", "EdDSA"])
    def test_the_configured_algorithm_decides_not_the_token(self, alg, key_pairs):
        signing, verifying = keys_for(alg, key_pairs)
        token = rust_jwt_encode({"sub": "u1", "exp": 4_000_000_000}, signing, algorithm=alg)
        # The same public key used as an HMAC secret must not verify anything.
        assert rust_jwt_decode(token, verifying, algorithm="HS256") is None
        other = {"RS256": "PS256", "ES256": "ES384", "EdDSA": "ES256"}[alg]
        assert rust_jwt_decode(token, verifying, algorithm=other) is None

    def test_hmac_secrets_must_match_the_hash_size(self):
        with pytest.raises(RuntimeError):
            rust_jwt_encode({"sub": "u", "exp": 4_000_000_000}, b"x" * 47, algorithm="HS384")

    def test_server_accepts_any_standard_algorithm_and_checks_its_keys(self, key_pairs):
        _, ed_public = key_pairs["Ed25519"]
        RustWSEServer("127.0.0.1", 1, jwt_secret=ed_public, jwt_algorithm="EdDSA")
        with pytest.raises(ValueError, match="jwt_secret"):
            RustWSEServer("127.0.0.1", 1, jwt_secret=b"not a key", jwt_algorithm="EdDSA")
        with pytest.raises(ValueError, match="Supported"):
            RustWSEServer("127.0.0.1", 1, jwt_secret=ed_public, jwt_algorithm="none")

    def test_jwt_options_without_a_secret_are_refused(self):
        with pytest.raises(ValueError, match="jwt_issuer, jwt_audience"):
            RustWSEServer("127.0.0.1", 1, jwt_issuer="auth", jwt_audience="api")
