import pytest

from aiodynamo.models import KeySchema, KeySpec, KeyType


class TestKeySchema:
    """Tests for KeySchema multi-attribute key support."""

    def test_single_hash_key(self) -> None:
        """Test backward compatible single hash key."""
        schema = KeySchema(KeySpec("h", KeyType.string))
        assert schema.encode() == [{"AttributeName": "h", "KeyType": "HASH"}]

    def test_single_hash_and_range_key(self) -> None:
        """Test backward compatible single hash and range key."""
        schema = KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string))
        assert schema.encode() == [
            {"AttributeName": "h", "KeyType": "HASH"},
            {"AttributeName": "r", "KeyType": "RANGE"},
        ]

    def test_multi_hash_keys(self) -> None:
        """Test multi-attribute partition key."""
        schema = KeySchema(
            hash_key=(KeySpec("pk1", KeyType.string), KeySpec("pk2", KeyType.number))
        )
        assert schema.encode() == [
            {"AttributeName": "pk1", "KeyType": "HASH"},
            {"AttributeName": "pk2", "KeyType": "HASH"},
        ]

    def test_multi_range_keys(self) -> None:
        """Test multi-attribute sort key."""
        schema = KeySchema(
            hash_key=KeySpec("h", KeyType.string),
            range_key=(KeySpec("sk1", KeyType.string), KeySpec("sk2", KeyType.number)),
        )
        assert schema.encode() == [
            {"AttributeName": "h", "KeyType": "HASH"},
            {"AttributeName": "sk1", "KeyType": "RANGE"},
            {"AttributeName": "sk2", "KeyType": "RANGE"},
        ]

    def test_multi_hash_and_range_keys(self) -> None:
        """Test multi-attribute partition and sort keys."""
        schema = KeySchema(
            hash_key=(KeySpec("pk1", KeyType.string), KeySpec("pk2", KeyType.string)),
            range_key=(KeySpec("sk1", KeyType.string), KeySpec("sk2", KeyType.number)),
        )
        assert schema.encode() == [
            {"AttributeName": "pk1", "KeyType": "HASH"},
            {"AttributeName": "pk2", "KeyType": "HASH"},
            {"AttributeName": "sk1", "KeyType": "RANGE"},
            {"AttributeName": "sk2", "KeyType": "RANGE"},
        ]

    def test_max_hash_keys(self) -> None:
        """Test maximum 4 hash key attributes."""
        schema = KeySchema(
            hash_key=(
                KeySpec("pk1", KeyType.string),
                KeySpec("pk2", KeyType.string),
                KeySpec("pk3", KeyType.string),
                KeySpec("pk4", KeyType.string),
            )
        )
        assert len([k for k in schema.encode() if k["KeyType"] == "HASH"]) == 4

    def test_max_range_keys(self) -> None:
        """Test maximum 4 range key attributes."""
        schema = KeySchema(
            hash_key=KeySpec("h", KeyType.string),
            range_key=(
                KeySpec("sk1", KeyType.string),
                KeySpec("sk2", KeyType.string),
                KeySpec("sk3", KeyType.string),
                KeySpec("sk4", KeyType.string),
            ),
        )
        assert len([k for k in schema.encode() if k["KeyType"] == "RANGE"]) == 4

    def test_to_attributes(self) -> None:
        """Test to_attributes returns all key attributes."""
        schema = KeySchema(
            hash_key=(KeySpec("pk1", KeyType.string), KeySpec("pk2", KeyType.number)),
            range_key=(KeySpec("sk1", KeyType.binary),),
        )
        assert schema.to_attributes() == {"pk1": "S", "pk2": "N", "sk1": "B"}

    def test_iter(self) -> None:
        """Test iteration yields all key specs."""
        schema = KeySchema(
            hash_key=(KeySpec("pk1", KeyType.string), KeySpec("pk2", KeyType.number)),
            range_key=(KeySpec("sk1", KeyType.string),),
        )
        keys = list(schema)
        assert len(keys) == 3
        assert keys[0].name == "pk1"
        assert keys[1].name == "pk2"
        assert keys[2].name == "sk1"


class TestKeySchemaValidation:
    """Tests for KeySchema validation."""

    def test_empty_hash_keys_raises(self) -> None:
        """Test that empty hash_keys tuple raises ValueError."""
        with pytest.raises(ValueError, match="hash_key must have 1-4 attributes"):
            KeySchema(hash_key=())

    def test_too_many_hash_keys_raises(self) -> None:
        """Test that more than 4 hash keys raises ValueError."""
        with pytest.raises(ValueError, match="hash_key must have 1-4 attributes"):
            KeySchema(
                hash_key=(
                    KeySpec("pk1", KeyType.string),
                    KeySpec("pk2", KeyType.string),
                    KeySpec("pk3", KeyType.string),
                    KeySpec("pk4", KeyType.string),
                    KeySpec("pk5", KeyType.string),
                )
            )

    def test_too_many_range_keys_raises(self) -> None:
        """Test that more than 4 range keys raises ValueError."""
        with pytest.raises(ValueError, match="range_key must have 0-4 attributes"):
            KeySchema(
                hash_key=KeySpec("h", KeyType.string),
                range_key=(
                    KeySpec("sk1", KeyType.string),
                    KeySpec("sk2", KeyType.string),
                    KeySpec("sk3", KeyType.string),
                    KeySpec("sk4", KeyType.string),
                    KeySpec("sk5", KeyType.string),
                ),
            )
