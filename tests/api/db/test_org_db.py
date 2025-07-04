import pytest
import json
from unittest.mock import patch, AsyncMock, MagicMock, ANY, call
from datetime import datetime, timezone
from src.api.db.org import (
    create_organization_with_user,
    get_org_by_id,
    get_org_by_slug,
    update_org,
    convert_org_db_to_dict,
    generate_api_key,
    get_all_orgs,
    create_org_api_key,
    get_org_id_from_api_key,
    get_hva_org_id,
    get_hva_cohort_ids,
    is_user_hva_learner,
    get_hva_openai_api_key,
    add_users_to_org_by_email,
    remove_members_from_org,
    convert_user_organization_db_to_dict,
    get_org_members,
    drop_user_organizations_table,
    drop_organizations_table,
    update_org_openai_api_key,
    clear_org_openai_api_key,
    add_user_to_org_by_user_id,
)


@pytest.mark.asyncio
class TestOrganizationOperations:
    """Test organization-related database operations."""

    @patch("src.api.db.org.get_user_by_id")
    @patch("src.api.db.org.get_new_db_connection")
    @patch("src.api.db.org.send_slack_notification_for_new_org")
    async def test_create_organization_with_user_success(
        self, mock_slack, mock_db_conn, mock_get_user
    ):
        """Test successful organization creation with user."""
        # Mock user data
        mock_user = {
            "id": 1,
            "email": "admin@example.com",
            "first_name": "Admin",
            "middle_name": None,
            "last_name": "User",
            "default_dp_color": "#FF5733",
            "created_at": "2023-01-01 12:00:00",
        }
        mock_get_user.return_value = mock_user

        # Mock database connection and cursor
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123
        mock_cursor.fetchone.return_value = None  # No existing org with this slug
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        result = await create_organization_with_user("Test Org", "test-org", 1)

        assert result == 123
        mock_get_user.assert_called_once_with(1)
        mock_slack.assert_called_once_with("Test Org", 123, mock_user)

    @patch("src.api.db.org.get_user_by_id")
    async def test_create_organization_with_user_invalid_user(self, mock_get_user):
        """Test organization creation with invalid user ID."""
        mock_get_user.return_value = None

        with pytest.raises(Exception, match="User with id '999' not found"):
            await create_organization_with_user("Test Org", "test-org", 999)

    @patch("src.api.db.org.get_user_by_id")
    @patch("src.api.db.org.get_new_db_connection")
    async def test_create_organization_with_user_duplicate_slug(
        self, mock_db_conn, mock_get_user
    ):
        """Test organization creation with duplicate slug."""
        mock_user = {"id": 1, "email": "admin@example.com"}
        mock_get_user.return_value = mock_user

        # Mock database connection and cursor
        mock_cursor = AsyncMock()
        mock_cursor.fetchone.return_value = (1,)  # Existing org with this slug
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        with pytest.raises(
            Exception, match="Organization with slug 'test-org' already exists"
        ):
            await create_organization_with_user("Test Org", "test-org", 1)

    @patch("src.api.db.org.get_new_db_connection")
    async def test_get_all_orgs(self, mock_db_conn):
        """Test retrieving all organizations."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = [
            (1, "Org One", "org-one"),
            (2, "Org Two", "org-two"),
            (3, "Org Three", "org-three"),
        ]
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        result = await get_all_orgs()

        expected = [
            {"id": 1, "name": "Org One", "slug": "org-one"},
            {"id": 2, "name": "Org Two", "slug": "org-two"},
            {"id": 3, "name": "Org Three", "slug": "org-three"},
        ]

        assert result == expected

    @patch("src.api.db.org.execute_db_operation")
    async def test_get_org_by_id_success(self, mock_execute):
        """Test successful organization retrieval by ID."""
        mock_org_tuple = (
            1,  # id
            "test-org",  # slug
            "Test Organization",  # name
            "#FF5733",  # logo_color
            "2023-01-01 12:00:00",  # created_at
            "sk-test123",  # openai_api_key
            True,  # openai_free_trial
        )
        mock_execute.return_value = mock_org_tuple

        result = await get_org_by_id(1)

        expected = {
            "id": 1,
            "slug": "test-org",
            "name": "Test Organization",
            "logo_color": "#FF5733",
            "openai_api_key": "sk-test123",
            "openai_free_trial": True,
        }

        assert result == expected
        mock_execute.assert_called_once_with(
            "SELECT * FROM organizations WHERE id = ?", (1,), fetch_one=True
        )

    @patch("src.api.db.org.execute_db_operation")
    async def test_get_org_by_id_not_found(self, mock_execute):
        """Test organization retrieval when org doesn't exist."""
        mock_execute.return_value = None

        result = await get_org_by_id(999)

        assert result is None

    @patch("src.api.db.org.execute_db_operation")
    async def test_get_org_by_slug_success(self, mock_execute):
        """Test successful organization retrieval by slug."""
        mock_org_tuple = (
            1,  # id
            "test-org",  # slug
            "Test Organization",  # name
            "#FF5733",  # logo_color
            "2023-01-01 12:00:00",  # created_at
            "sk-test123",  # openai_api_key
            False,  # openai_free_trial
        )
        mock_execute.return_value = mock_org_tuple

        result = await get_org_by_slug("test-org")

        expected = {
            "id": 1,
            "slug": "test-org",
            "name": "Test Organization",
            "logo_color": "#FF5733",
            "openai_api_key": "sk-test123",
            "openai_free_trial": False,
        }

        assert result == expected

    @patch("src.api.db.org.execute_db_operation")
    async def test_update_org_success(self, mock_execute):
        """Test successful organization update."""
        await update_org(1, "Updated Org Name")

        mock_execute.assert_called_once_with(
            "UPDATE organizations SET name = ? WHERE id = ?", ("Updated Org Name", 1)
        )

    @patch("src.api.db.org.get_new_db_connection")
    @patch("src.api.db.org.generate_api_key")
    async def test_create_org_api_key(self, mock_generate, mock_db_conn):
        """Test creating an API key for an organization."""
        mock_generate.return_value = ("org__123__abc123", "hashed_key_value")

        mock_cursor = AsyncMock()
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        result = await create_org_api_key(123)

        assert result == "org__123__abc123"
        mock_generate.assert_called_once_with(123)
        mock_cursor.execute.assert_called_once()
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.org.execute_db_operation")
    async def test_get_org_id_from_api_key_success(self, mock_execute):
        """Test successful org ID retrieval from API key."""
        # Mock the hash comparison
        mock_execute.return_value = [("expected_hash",)]

        with patch("src.api.db.org.hashlib.sha256") as mock_sha256:
            mock_hash = MagicMock()
            mock_hash.hexdigest.return_value = "expected_hash"
            mock_sha256.return_value = mock_hash

            result = await get_org_id_from_api_key("org__123__validkey")

            assert result == 123

    async def test_get_org_id_from_api_key_invalid_format(self):
        """Test API key with invalid format."""
        with pytest.raises(ValueError, match="Invalid API key"):
            await get_org_id_from_api_key("invalid_key_format")

    async def test_get_org_id_from_api_key_invalid_org_id(self):
        """Test API key with invalid org ID."""
        with pytest.raises(ValueError, match="Invalid API key"):
            await get_org_id_from_api_key("org__invalid__key")

    @patch("src.api.db.org.execute_db_operation")
    async def test_get_org_id_from_api_key_no_keys_found(self, mock_execute):
        """Test API key when no keys exist for org."""
        mock_execute.return_value = []

        with pytest.raises(ValueError, match="Invalid API key"):
            await get_org_id_from_api_key("org__123__invalidkey")

    @patch("src.api.db.org.execute_db_operation")
    async def test_get_org_id_from_api_key_hash_mismatch(self, mock_execute):
        """Test API key with mismatched hash."""
        mock_execute.return_value = [("different_hash",)]

        with patch("src.api.db.org.hashlib.sha256") as mock_sha256:
            mock_hash = MagicMock()
            mock_hash.hexdigest.return_value = "actual_hash"
            mock_sha256.return_value = mock_hash

            with pytest.raises(ValueError, match="Invalid API key"):
                await get_org_id_from_api_key("org__123__invalidkey")

    @patch("src.api.db.org.execute_db_operation")
    async def test_get_hva_org_id_success(self, mock_execute):
        """Test successful HVA org ID retrieval."""
        mock_execute.return_value = (456,)

        result = await get_hva_org_id()

        assert result == 456
        mock_execute.assert_called_once_with(
            "SELECT id FROM organizations WHERE name = ?",
            ("HyperVerge Academy",),
            fetch_one=True,
        )

    @patch("src.api.db.org.execute_db_operation")
    async def test_get_hva_org_id_not_found(self, mock_execute):
        """Test HVA org ID when not found."""
        mock_execute.return_value = None

        result = await get_hva_org_id()

        assert result is None

    @patch("src.api.db.org.get_hva_org_id")
    @patch("src.api.db.org.execute_db_operation")
    async def test_get_hva_cohort_ids_success(self, mock_execute, mock_get_hva_org):
        """Test successful HVA cohort IDs retrieval."""
        mock_get_hva_org.return_value = 456
        mock_execute.return_value = [(1,), (2,), (3,)]

        result = await get_hva_cohort_ids()

        assert result == [1, 2, 3]
        mock_execute.assert_called_once_with(
            "SELECT id FROM cohorts WHERE org_id = ?", (456,), fetch_all=True
        )

    @patch("src.api.db.org.get_hva_org_id")
    async def test_get_hva_cohort_ids_no_org(self, mock_get_hva_org):
        """Test HVA cohort IDs when org doesn't exist."""
        mock_get_hva_org.return_value = None

        result = await get_hva_cohort_ids()

        assert result == []

    @patch("src.api.db.org.get_hva_cohort_ids")
    @patch("src.api.db.org.execute_db_operation")
    async def test_is_user_hva_learner_true(self, mock_execute, mock_get_cohorts):
        """Test user is HVA learner."""
        mock_get_cohorts.return_value = [1, 2, 3]
        mock_execute.return_value = (1,)

        result = await is_user_hva_learner(123)

        assert result is True

    @patch("src.api.db.org.get_hva_cohort_ids")
    async def test_is_user_hva_learner_no_cohorts(self, mock_get_cohorts):
        """Test user HVA learner check when no cohorts exist."""
        mock_get_cohorts.return_value = []

        result = await is_user_hva_learner(123)

        assert result is False

    @patch("src.api.db.org.get_hva_cohort_ids")
    @patch("src.api.db.org.execute_db_operation")
    async def test_is_user_hva_learner_false(self, mock_execute, mock_get_cohorts):
        """Test user is not HVA learner."""
        mock_get_cohorts.return_value = [1, 2, 3]
        mock_execute.return_value = (0,)

        result = await is_user_hva_learner(123)

        assert result is False

    @patch("src.api.db.org.get_hva_org_id")
    @patch("src.api.db.org.get_org_by_id")
    async def test_get_hva_openai_api_key(self, mock_get_org, mock_get_hva_org):
        """Test getting HVA OpenAI API key."""
        mock_get_hva_org.return_value = 456
        mock_get_org.return_value = {"openai_api_key": "sk-hva-key"}

        result = await get_hva_openai_api_key()

        assert result == "sk-hva-key"
        mock_get_org.assert_called_once_with(456)

    @patch("src.api.db.org.get_org_by_id")
    @patch("src.api.db.org.get_new_db_connection")
    @patch("src.api.db.org.insert_or_return_user")
    @patch("src.api.db.org.send_slack_notification_for_member_added_to_org")
    async def test_add_users_to_org_by_email_success(
        self, mock_slack, mock_insert_user, mock_db_conn, mock_get_org
    ):
        """Test successful addition of users to org by email."""
        mock_get_org.return_value = {"id": 1, "slug": "test-org", "name": "Test Org"}

        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = []  # No existing users
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        mock_user1 = {"id": 1, "email": "user1@example.com"}
        mock_user2 = {"id": 2, "email": "user2@example.com"}
        mock_insert_user.side_effect = [mock_user1, mock_user2]

        await add_users_to_org_by_email(1, ["user1@example.com", "user2@example.com"])

        assert mock_insert_user.call_count == 2
        mock_cursor.executemany.assert_called_once()
        mock_conn_instance.commit.assert_called_once()

    @patch("src.api.db.org.get_org_by_id")
    async def test_add_users_to_org_by_email_org_not_found(self, mock_get_org):
        """Test adding users to non-existent org."""
        mock_get_org.return_value = None

        with pytest.raises(Exception, match="Organization not found"):
            await add_users_to_org_by_email(999, ["user@example.com"])

    @patch("src.api.db.org.get_org_by_id")
    @patch("src.api.db.org.get_new_db_connection")
    @patch("src.api.db.org.insert_or_return_user")
    async def test_add_users_to_org_by_email_existing_users(
        self, mock_insert_user, mock_db_conn, mock_get_org
    ):
        """Test adding users that already exist in org."""
        mock_get_org.return_value = {"id": 1, "slug": "test-org", "name": "Test Org"}

        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = [(1,)]  # Existing user
        mock_conn_instance = AsyncMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn_instance.__aenter__.return_value = mock_conn_instance
        mock_db_conn.return_value = mock_conn_instance

        mock_user = {"id": 1, "email": "user@example.com"}
        mock_insert_user.return_value = mock_user

        with pytest.raises(Exception, match="Some users already exist in organization"):
            await add_users_to_org_by_email(1, ["user@example.com"])

    @patch("src.api.db.org.execute_db_operation")
    async def test_remove_members_from_org(self, mock_execute):
        """Test removing members from org."""
        await remove_members_from_org(1, [123, 456])

        mock_execute.assert_called_once_with(
            "DELETE FROM user_organizations WHERE org_id = ? AND user_id IN (123, 456)",
            (1,),
        )

    @patch("src.api.db.org.execute_db_operation")
    async def test_get_org_members(self, mock_execute):
        """Test getting org members."""
        mock_execute.return_value = [
            (1, "user1@example.com", "admin"),
            (2, "user2@example.com", "member"),
        ]

        result = await get_org_members(1)

        expected = [
            {"id": 1, "email": "user1@example.com", "role": "admin"},
            {"id": 2, "email": "user2@example.com", "role": "member"},
        ]

        assert result == expected

    @patch("src.api.db.org.execute_db_operation")
    async def test_update_org_openai_api_key(self, mock_execute):
        """Test updating org OpenAI API key."""
        await update_org_openai_api_key(1, "encrypted_key", True)

        mock_execute.assert_called_once_with(
            "UPDATE organizations SET openai_api_key = ?, openai_free_trial = ? WHERE id = ?",
            ("encrypted_key", True, 1),
        )

    @patch("src.api.db.org.execute_db_operation")
    async def test_clear_org_openai_api_key(self, mock_execute):
        """Test clearing org OpenAI API key."""
        await clear_org_openai_api_key(1)

        mock_execute.assert_called_once_with(
            "UPDATE organizations SET openai_api_key = NULL WHERE id = ?", (1,)
        )

    async def test_add_user_to_org_by_user_id(self):
        """Test adding user to org by user ID."""
        mock_cursor = AsyncMock()
        mock_cursor.lastrowid = 123

        result = await add_user_to_org_by_user_id(mock_cursor, 1, 2, "admin")

        assert result == 123
        mock_cursor.execute.assert_called_once_with(
            """INSERT INTO user_organizations
            (user_id, org_id, role)
            VALUES (?, ?, ?)""",
            (1, 2, "admin"),
        )

    @patch("src.api.db.org.execute_multiple_db_operations")
    def test_drop_user_organizations_table(self, mock_execute_multiple):
        """Test dropping user organizations table."""
        drop_user_organizations_table()

        mock_execute_multiple.assert_called_once()

    @patch("src.api.db.org.drop_user_organizations_table")
    @patch("src.api.db.org.execute_multiple_db_operations")
    def test_drop_organizations_table(self, mock_execute_multiple, mock_drop_user_orgs):
        """Test dropping organizations table."""
        drop_organizations_table()

        mock_drop_user_orgs.assert_called_once()
        mock_execute_multiple.assert_called_once()


class TestOrganizationUtilityFunctions:
    """Test organization utility and conversion functions."""

    def test_convert_org_db_to_dict_complete(self):
        """Test converting complete organization tuple to dictionary."""
        # Tuple order: (id, slug, name, logo_color, created_at, openai_api_key, openai_free_trial)
        org_tuple = (
            1,  # id
            "test-org",  # slug
            "Test Organization",  # name
            "#FF5733",  # logo_color
            "2023-01-01 12:00:00",  # created_at
            "sk-test123",  # openai_api_key
            True,  # openai_free_trial
        )

        result = convert_org_db_to_dict(org_tuple)

        expected = {
            "id": 1,
            "slug": "test-org",
            "name": "Test Organization",
            "logo_color": "#FF5733",
            "openai_api_key": "sk-test123",
            "openai_free_trial": True,
        }

        assert result == expected

    def test_convert_org_db_to_dict_none(self):
        """Test converting None organization."""
        result = convert_org_db_to_dict(None)
        assert result is None

    def test_convert_org_db_to_dict_extended(self):
        """Test organization conversion with extended tuple."""
        # Tuple order: (id, slug, name, logo_color, created_at, openai_api_key, openai_free_trial)
        org_tuple = (
            1,  # id
            "test-org",  # slug
            "Test Org",  # name
            "#000000",  # logo_color
            "2024-01-01 12:00:00",  # created_at
            "encrypted_key",  # openai_api_key
            True,  # openai_free_trial
        )

        result = convert_org_db_to_dict(org_tuple)

        assert result["id"] == 1
        assert result["slug"] == "test-org"
        assert result["name"] == "Test Org"

    def test_convert_org_db_to_dict_minimal(self):
        """Test organization conversion with minimal data."""
        # Tuple order: (id, slug, name, logo_color, created_at, openai_api_key, openai_free_trial)
        org_tuple = (1, "test-org", "Test Org", None, None, None, False)

        result = convert_org_db_to_dict(org_tuple)

        assert result["id"] == 1
        assert result["slug"] == "test-org"
        assert result["name"] == "Test Org"

    def test_convert_user_organization_db_to_dict(self):
        """Test converting user organization tuple to dictionary."""
        user_org_tuple = (1, 123, 456, "admin")

        result = convert_user_organization_db_to_dict(user_org_tuple)

        expected = {
            "id": 1,
            "user_id": 123,
            "org_id": 456,
            "role": "admin",
        }

        assert result == expected

    def test_generate_api_key(self):
        """Test API key generation."""
        org_id = 123
        api_key, hashed_key = generate_api_key(org_id)

        assert isinstance(api_key, str)
        assert isinstance(hashed_key, str)
        assert len(api_key) > 0
        assert len(hashed_key) > 0
        # API key should contain org ID
        assert str(org_id) in api_key

    def test_generate_api_key_different_orgs(self):
        """Test API key generation for different organizations."""
        api_key_1, hashed_key_1 = generate_api_key(1)
        api_key_2, hashed_key_2 = generate_api_key(2)

        assert isinstance(api_key_1, str)
        assert isinstance(api_key_2, str)
        assert isinstance(hashed_key_1, str)
        assert isinstance(hashed_key_2, str)
        # Different orgs should get different keys
        assert api_key_1 != api_key_2
        assert hashed_key_1 != hashed_key_2
