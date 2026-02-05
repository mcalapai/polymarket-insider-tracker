"""Tests for storage repositories."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from polymarket_insider_tracker.storage.models import Base
from polymarket_insider_tracker.storage.repos import (
    ERC20TransferDTO,
    ERC20TransferRepository,
    RelationshipRepository,
    WalletRelationshipDTO,
    WalletSnapshotDTO,
    WalletSnapshotRepository,
)

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
async def async_engine():
    """Create an async SQLite engine for testing."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def async_session(async_engine) -> AsyncSession:
    """Create an async session for testing."""
    session_factory = async_sessionmaker(bind=async_engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest.fixture
def sample_wallet_dto() -> WalletSnapshotDTO:
    """Create a sample wallet snapshot DTO."""
    as_of = datetime.now(UTC)
    return WalletSnapshotDTO(
        address="0x1234567890abcdef1234567890abcdef12345678",
        as_of_block_number=123,
        as_of=as_of,
        nonce_as_of=5,
        first_funding_at=as_of - timedelta(hours=24),
        age_hours_as_of=Decimal("24.0"),
        matic_balance_wei_as_of=Decimal("1000000000000000000"),
        usdc_balance_units_as_of=Decimal("1000000000"),
        computed_at=as_of,
    )


@pytest.fixture
def sample_transfer_dto() -> ERC20TransferDTO:
    """Create a sample ERC20 transfer DTO."""
    return ERC20TransferDTO(
        token_address="0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        from_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        to_address="0x1234567890abcdef1234567890abcdef12345678",
        amount_units=Decimal("5000000"),
        tx_hash="0x" + "a" * 64,
        log_index=0,
        block_number=12345678,
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_relationship_dto() -> WalletRelationshipDTO:
    """Create a sample wallet relationship DTO."""
    return WalletRelationshipDTO(
        wallet_a="0x1234567890abcdef1234567890abcdef12345678",
        wallet_b="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        relationship_type="funded_by",
        confidence=Decimal("0.95"),
    )


# ============================================================================
# WalletSnapshotRepository Tests
# ============================================================================


class TestWalletSnapshotRepository:
    """Tests for WalletSnapshotRepository."""

    @pytest.mark.asyncio
    async def test_get_by_address_and_block_not_found(self, async_session: AsyncSession) -> None:
        repo = WalletSnapshotRepository(async_session)
        result = await repo.get_by_address_and_block("0xnonexistent", as_of_block_number=1)
        assert result is None

    @pytest.mark.asyncio
    async def test_upsert_creates_new(
        self, async_session: AsyncSession, sample_wallet_dto: WalletSnapshotDTO
    ) -> None:
        repo = WalletSnapshotRepository(async_session)
        await repo.upsert(sample_wallet_dto)
        await async_session.commit()

        result = await repo.get_by_address_and_block(
            sample_wallet_dto.address,
            as_of_block_number=sample_wallet_dto.as_of_block_number,
        )
        assert result is not None
        assert result.address == sample_wallet_dto.address.lower()
        assert result.nonce_as_of == sample_wallet_dto.nonce_as_of

    @pytest.mark.asyncio
    async def test_upsert_updates_existing(
        self, async_session: AsyncSession, sample_wallet_dto: WalletSnapshotDTO
    ) -> None:
        repo = WalletSnapshotRepository(async_session)
        await repo.upsert(sample_wallet_dto)
        await async_session.commit()

        updated_dto = WalletSnapshotDTO(
            address=sample_wallet_dto.address,
            as_of_block_number=sample_wallet_dto.as_of_block_number,
            as_of=sample_wallet_dto.as_of,
            nonce_as_of=10,
            first_funding_at=sample_wallet_dto.first_funding_at,
            age_hours_as_of=sample_wallet_dto.age_hours_as_of,
            matic_balance_wei_as_of=sample_wallet_dto.matic_balance_wei_as_of,
            usdc_balance_units_as_of=Decimal("2000000000"),
            computed_at=datetime.now(UTC),
        )
        await repo.upsert(updated_dto)
        await async_session.commit()

        result = await repo.get_by_address_and_block(
            sample_wallet_dto.address,
            as_of_block_number=sample_wallet_dto.as_of_block_number,
        )
        assert result is not None
        assert result.nonce_as_of == 10
        assert result.usdc_balance_units_as_of == Decimal("2000000000")

    @pytest.mark.asyncio
    async def test_get_latest_by_address(self, async_session: AsyncSession) -> None:
        repo = WalletSnapshotRepository(async_session)
        addr = "0xdddddddddddddddddddddddddddddddddddddddd"
        base_ts = datetime.now(UTC)
        await repo.upsert(
            WalletSnapshotDTO(
                address=addr,
                as_of_block_number=1,
                as_of=base_ts,
                nonce_as_of=1,
                first_funding_at=base_ts - timedelta(hours=1),
                age_hours_as_of=Decimal("1.0"),
                matic_balance_wei_as_of=Decimal("1"),
                usdc_balance_units_as_of=Decimal("2"),
                computed_at=base_ts,
            )
        )
        await repo.upsert(
            WalletSnapshotDTO(
                address=addr,
                as_of_block_number=2,
                as_of=base_ts + timedelta(minutes=1),
                nonce_as_of=2,
                first_funding_at=base_ts - timedelta(hours=1),
                age_hours_as_of=Decimal("1.016666"),
                matic_balance_wei_as_of=Decimal("1"),
                usdc_balance_units_as_of=Decimal("2"),
                computed_at=base_ts,
            )
        )
        await async_session.commit()

        latest = await repo.get_latest_by_address(addr)
        assert latest is not None
        assert latest.as_of_block_number == 2


# ============================================================================
# ERC20TransferRepository Tests
# ============================================================================


class TestERC20TransferRepository:
    """Tests for ERC20TransferRepository."""

    @pytest.mark.asyncio
    async def test_insert(
        self, async_session: AsyncSession, sample_transfer_dto: ERC20TransferDTO
    ) -> None:
        """Test inserting an ERC20 transfer."""
        repo = ERC20TransferRepository(async_session)
        await repo.insert(sample_transfer_dto)
        await async_session.commit()

        result = await repo.get_by_tx_hash(sample_transfer_dto.tx_hash)
        assert result is not None
        assert result.amount_units == sample_transfer_dto.amount_units

    @pytest.mark.asyncio
    async def test_get_transfers_to(
        self, async_session: AsyncSession, sample_transfer_dto: ERC20TransferDTO
    ) -> None:
        """Test getting transfers to an address."""
        repo = ERC20TransferRepository(async_session)
        await repo.insert(sample_transfer_dto)
        await async_session.commit()

        results = await repo.get_transfers_to(sample_transfer_dto.to_address)
        assert len(results) == 1
        assert results[0].from_address == sample_transfer_dto.from_address.lower()

    @pytest.mark.asyncio
    async def test_get_transfers_from(
        self, async_session: AsyncSession, sample_transfer_dto: ERC20TransferDTO
    ) -> None:
        """Test getting transfers from an address."""
        repo = ERC20TransferRepository(async_session)
        await repo.insert(sample_transfer_dto)
        await async_session.commit()

        results = await repo.get_transfers_from(sample_transfer_dto.from_address)
        assert len(results) == 1
        assert results[0].to_address == sample_transfer_dto.to_address.lower()

    @pytest.mark.asyncio
    async def test_get_first_transfer_to(
        self, async_session: AsyncSession, sample_transfer_dto: ERC20TransferDTO
    ) -> None:
        """Test getting first transfer to an address."""
        repo = ERC20TransferRepository(async_session)

        # Insert multiple transfers with different timestamps
        earlier = ERC20TransferDTO(
            token_address=sample_transfer_dto.token_address,
            from_address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            to_address=sample_transfer_dto.to_address,
            amount_units=Decimal("100000"),
            tx_hash="0x" + "b" * 64,
            log_index=0,
            block_number=12345670,
            timestamp=datetime.now(UTC) - timedelta(hours=2),
        )
        await repo.insert(earlier)
        await repo.insert(sample_transfer_dto)
        await async_session.commit()

        result = await repo.get_first_transfer_to(
            sample_transfer_dto.to_address,
            token_addresses=[sample_transfer_dto.token_address],
        )
        assert result is not None
        assert result.tx_hash == earlier.tx_hash.lower()

    @pytest.mark.asyncio
    async def test_insert_many(self, async_session: AsyncSession) -> None:
        """Test inserting multiple transfers."""
        repo = ERC20TransferRepository(async_session)

        transfers = [
            ERC20TransferDTO(
                token_address="0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
                from_address=f"0x{'a' * 40}",
                to_address=f"0x{'b' * 40}",
                amount_units=Decimal(str(i * 100_000)),
                tx_hash=f"0x{str(i) * 64}"[:66],
                log_index=0,
                block_number=12345678 + i,
                timestamp=datetime.now(UTC),
            )
            for i in range(1, 4)
        ]

        count = await repo.insert_many(transfers)
        await async_session.commit()
        assert count == 3


# ============================================================================
# RelationshipRepository Tests
# ============================================================================


class TestRelationshipRepository:
    """Tests for RelationshipRepository."""

    @pytest.mark.asyncio
    async def test_upsert(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test upserting a relationship."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)
        await async_session.commit()

        results = await repo.get_relationships(sample_relationship_dto.wallet_a)
        assert len(results) == 1
        assert results[0].confidence == sample_relationship_dto.confidence

    @pytest.mark.asyncio
    async def test_get_relationships_filter_type(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test getting relationships with type filter."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)

        same_entity = WalletRelationshipDTO(
            wallet_a=sample_relationship_dto.wallet_a,
            wallet_b="0xdddddddddddddddddddddddddddddddddddddddd",
            relationship_type="same_entity",
            confidence=Decimal("0.80"),
        )
        await repo.upsert(same_entity)
        await async_session.commit()

        funded_results = await repo.get_relationships(
            sample_relationship_dto.wallet_a, relationship_type="funded_by"
        )
        assert len(funded_results) == 1
        assert funded_results[0].relationship_type == "funded_by"

    @pytest.mark.asyncio
    async def test_get_related_wallets(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test getting related wallet addresses."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)
        await async_session.commit()

        related = await repo.get_related_wallets(sample_relationship_dto.wallet_a)
        assert len(related) == 1
        assert sample_relationship_dto.wallet_b.lower() in related

    @pytest.mark.asyncio
    async def test_delete_relationship(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test deleting a relationship."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)
        await async_session.commit()

        deleted = await repo.delete(
            sample_relationship_dto.wallet_a,
            sample_relationship_dto.wallet_b,
            sample_relationship_dto.relationship_type,
        )
        await async_session.commit()
        assert deleted is True

        results = await repo.get_relationships(sample_relationship_dto.wallet_a)
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_upsert_updates_confidence(
        self, async_session: AsyncSession, sample_relationship_dto: WalletRelationshipDTO
    ) -> None:
        """Test that upserting updates the confidence."""
        repo = RelationshipRepository(async_session)
        await repo.upsert(sample_relationship_dto)
        await async_session.commit()

        updated = WalletRelationshipDTO(
            wallet_a=sample_relationship_dto.wallet_a,
            wallet_b=sample_relationship_dto.wallet_b,
            relationship_type=sample_relationship_dto.relationship_type,
            confidence=Decimal("0.99"),
        )
        await repo.upsert(updated)
        await async_session.commit()

        results = await repo.get_relationships(sample_relationship_dto.wallet_a)
        assert len(results) == 1
        assert results[0].confidence == Decimal("0.99")


# ============================================================================
# DTO Tests
# ============================================================================


class TestDTOs:
    """Tests for Data Transfer Objects."""

    def test_wallet_snapshot_dto_from_model(self) -> None:
        """Test WalletSnapshotDTO.from_model works correctly."""
        from polymarket_insider_tracker.storage.models import WalletSnapshotModel

        now = datetime.now(UTC)
        model = WalletSnapshotModel(
            address="0x1234",
            as_of_block_number=123,
            as_of=now,
            nonce_as_of=5,
            first_funding_at=now - timedelta(hours=1),
            age_hours_as_of=Decimal("1.0"),
            matic_balance_wei_as_of=Decimal("100"),
            usdc_balance_units_as_of=Decimal("50000000"),
            computed_at=now,
            created_at=now,
        )

        dto = WalletSnapshotDTO.from_model(model)
        assert dto.address == "0x1234"
        assert dto.as_of_block_number == 123
        assert dto.nonce_as_of == 5

    def test_erc20_transfer_dto_from_model(self) -> None:
        """Test ERC20TransferDTO.from_model works correctly."""
        from polymarket_insider_tracker.storage.models import ERC20TransferModel

        now = datetime.now(UTC)
        model = ERC20TransferModel(
            id=1,
            token_address="0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
            from_address="0xaaa",
            to_address="0xbbb",
            amount_units=Decimal("100000"),
            tx_hash="0x123",
            log_index=0,
            block_number=12345,
            timestamp=now,
            created_at=now,
        )

        dto = ERC20TransferDTO.from_model(model)
        assert dto.from_address == "0xaaa"
        assert dto.amount_units == Decimal("100000")

    def test_wallet_relationship_dto_from_model(self) -> None:
        """Test WalletRelationshipDTO.from_model works correctly."""
        from polymarket_insider_tracker.storage.models import WalletRelationshipModel

        now = datetime.now(UTC)
        model = WalletRelationshipModel(
            id=1,
            wallet_a="0xaaa",
            wallet_b="0xbbb",
            relationship_type="funded_by",
            confidence=Decimal("0.95"),
            created_at=now,
        )

        dto = WalletRelationshipDTO.from_model(model)
        assert dto.wallet_a == "0xaaa"
        assert dto.relationship_type == "funded_by"
        assert dto.confidence == Decimal("0.95")
