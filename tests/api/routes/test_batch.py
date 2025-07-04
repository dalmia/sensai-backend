import pytest
from fastapi import status
from unittest.mock import patch


@pytest.mark.asyncio
async def test_get_all_batches_for_cohort(client, mock_db):
    """Test getting all batches for a cohort with members"""
    with patch(
        "api.routes.batch.get_all_batches_for_cohort_from_db"
    ) as mock_get_batches:
        cohort_id = 1
        expected_batches = [
            {
                "id": 1,
                "name": "Batch 1",
                "members": [
                    {"id": 1, "email": "user1@example.com", "role": "learner"},
                    {"id": 2, "email": "user2@example.com", "role": "mentor"},
                ],
            },
            {
                "id": 2,
                "name": "Batch 2",
                "members": [
                    {"id": 3, "email": "user3@example.com", "role": "learner"},
                ],
            },
        ]

        # Test successful retrieval
        mock_get_batches.return_value = expected_batches

        response = client.get(f"/batches/?cohort_id={cohort_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_batches
        mock_get_batches.assert_called_with(cohort_id)


@pytest.mark.asyncio
async def test_get_all_batches_for_cohort_empty(client, mock_db):
    """Test getting all batches for a cohort when no batches exist"""
    with patch(
        "api.routes.batch.get_all_batches_for_cohort_from_db"
    ) as mock_get_batches:
        cohort_id = 1
        mock_get_batches.return_value = []

        response = client.get(f"/batches/?cohort_id={cohort_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == []
        mock_get_batches.assert_called_with(cohort_id)


@pytest.mark.asyncio
async def test_create_batch(client, mock_db):
    """Test creating a batch"""
    with patch("api.routes.batch.create_batch_with_members_in_db") as mock_create:
        request_body = {"name": "New Batch", "cohort_id": 1}
        batch_id = 5

        # Test successful creation
        mock_create.return_value = batch_id

        response = client.post("/batches/", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"id": batch_id}
        mock_create.assert_called_with(
            request_body["name"], request_body["cohort_id"], []
        )


@pytest.mark.asyncio
async def test_create_batch_with_members(client, mock_db):
    """Test creating a batch with initial members"""
    with patch("api.routes.batch.create_batch_with_members_in_db") as mock_create:
        request_body = {
            "name": "New Batch",
            "cohort_id": 1,
            "user_ids": [1, 2],
        }
        batch_id = 5

        # Test successful creation with members
        mock_create.return_value = batch_id

        response = client.post("/batches/", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"id": batch_id}
        mock_create.assert_called_with(
            request_body["name"],
            request_body["cohort_id"],
            request_body["user_ids"],
        )


@pytest.mark.asyncio
async def test_create_batch_with_members_error(client, mock_db):
    """Test creating a batch with members that causes an error"""
    with patch("api.routes.batch.create_batch_with_members_in_db") as mock_create:
        request_body = {
            "name": "New Batch",
            "cohort_id": 1,
            "user_ids": [1, 2],
        }

        # Test user already in batch error
        mock_create.side_effect = Exception(
            "One or more users are already in the batch"
        )

        response = client.post("/batches/", json=request_body)

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert response.json() == {
            "detail": "One or more users are already in the batch"
        }

        # Test organization permission error
        mock_create.reset_mock()
        mock_create.side_effect = Exception("User does not belong to this organization")

        response = client.post("/batches/", json=request_body)

        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert response.json() == {
            "detail": "User does not belong to this organization"
        }

        # Test other error
        mock_create.reset_mock()
        mock_create.side_effect = Exception("Some other error")

        response = client.post("/batches/", json=request_body)

        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "Some other error"}


@pytest.mark.asyncio
async def test_get_batch_by_id(client, mock_db):
    """Test getting a batch by ID"""
    with patch("api.routes.batch.get_batch_by_id_from_db") as mock_get_batch:
        batch_id = 1
        expected_batch = {
            "id": batch_id,
            "name": "Test Batch",
            "cohort_id": 1,
            "members": [
                {"id": 1, "email": "user1@example.com", "role": "learner"},
                {"id": 2, "email": "user2@example.com", "role": "mentor"},
            ],
        }

        # Test successful retrieval
        mock_get_batch.return_value = expected_batch

        response = client.get(f"/batches/{batch_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_batch
        mock_get_batch.assert_called_with(batch_id)

        # Test batch not found
        mock_get_batch.reset_mock()
        mock_get_batch.return_value = None

        response = client.get(f"/batches/{batch_id}")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Batch not found"}
        mock_get_batch.assert_called_with(batch_id)


@pytest.mark.asyncio
async def test_delete_batch(client, mock_db):
    """Test deleting a batch"""
    with patch("api.routes.batch.delete_batch_from_db") as mock_delete:
        batch_id = 1

        response = client.delete(f"/batches/{batch_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_delete.assert_called_with(batch_id)


@pytest.mark.asyncio
async def test_update_batch_name(client, mock_db):
    """Test updating a batch's name and members"""
    with patch("api.routes.batch.update_batch_name_and_members") as mock_update:
        batch_id = 1
        request_body = {
            "name": "Updated Batch Name",
            "members_added": [3, 4],
            "members_removed": [1],
        }

        mock_update.return_value = {
            "id": 1,
            "name": "Updated Batch Name",
            "cohort_id": 1,
            "members": [
                {"id": 1, "email": "user1@example.com", "role": "learner"},
                {"id": 2, "email": "user2@example.com", "role": "mentor"},
                {"id": 3, "email": "user3@example.com", "role": "learner"},
                {"id": 4, "email": "user4@example.com", "role": "mentor"},
            ],
        }

        response = client.put(f"/batches/{batch_id}", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == mock_update.return_value
        mock_update.assert_called_with(
            batch_id,
            request_body["name"],
            request_body["members_added"],
            request_body["members_removed"],
        )


@pytest.mark.asyncio
async def test_get_batches_for_user_in_cohort(client, mock_db):
    """Test getting batches for a user in a cohort"""
    with patch(
        "api.routes.batch.get_batches_for_user_in_cohort_from_db"
    ) as mock_get_batches:
        user_id = 1
        cohort_id = 1
        expected_batches = [
            {"id": 1, "name": "Batch 1", "role": "learner"},
            {"id": 2, "name": "Batch 2", "role": "mentor"},
        ]

        # Test successful retrieval
        mock_get_batches.return_value = expected_batches

        response = client.get(f"/batches/user/{user_id}/cohort/{cohort_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_batches
        mock_get_batches.assert_called_with(user_id, cohort_id)

        # Test user not in cohort
        mock_get_batches.reset_mock()
        mock_get_batches.side_effect = Exception(
            "User is not a member of the specified cohort"
        )

        response = client.get(f"/batches/user/{user_id}/cohort/{cohort_id}")

        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert response.json() == {
            "detail": "User is not a member of the specified cohort"
        }

        # Test cohort not found
        mock_get_batches.reset_mock()
        mock_get_batches.side_effect = Exception("Cohort not found")

        response = client.get(f"/batches/user/{user_id}/cohort/{cohort_id}")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Cohort not found"}

        # Test other exception
        mock_get_batches.reset_mock()
        mock_get_batches.side_effect = Exception("Some other error")

        response = client.get(f"/batches/user/{user_id}/cohort/{cohort_id}")

        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "Some other error"}


@pytest.mark.asyncio
async def test_get_batches_for_user_in_cohort_empty(client, mock_db):
    """Test getting batches for a user in a cohort when no batches exist"""
    with patch(
        "api.routes.batch.get_batches_for_user_in_cohort_from_db"
    ) as mock_get_batches:
        user_id = 1
        cohort_id = 1
        mock_get_batches.return_value = []

        response = client.get(f"/batches/user/{user_id}/cohort/{cohort_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == []
        mock_get_batches.assert_called_with(user_id, cohort_id)


@pytest.mark.asyncio
async def test_create_batch_with_empty_name(client, mock_db):
    """Test creating a batch with empty name"""
    with patch("api.routes.batch.create_batch_with_members_in_db") as mock_create:
        request_body = {"name": "", "cohort_id": 1}
        batch_id = 6

        mock_create.return_value = batch_id

        response = client.post("/batches/", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"id": batch_id}
        mock_create.assert_called_with("", 1, [])
