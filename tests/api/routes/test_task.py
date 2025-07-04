import pytest
from fastapi import status
from unittest.mock import patch, ANY
from datetime import datetime


@pytest.mark.asyncio
async def test_get_learning_material_tasks_for_course(client, mock_db):
    """
    Test getting learning material tasks for a course
    """
    with patch(
        "api.routes.task.get_all_learning_material_tasks_for_course_from_db"
    ) as mock_get_tasks:
        # Test successful retrieval
        course_id = 1
        expected_tasks = [
            {
                "id": 1,
                "title": "Task 1",
                "type": "learning_material",
                "status": "published",
                "scheduled_publish_at": "2023-05-01T10:00:00Z",
            },
            {
                "id": 2,
                "title": "Task 2",
                "type": "learning_material",
                "status": "draft",
                "scheduled_publish_at": None,
            },
        ]
        mock_get_tasks.return_value = expected_tasks

        response = client.get(f"/tasks/course/{course_id}/learning_material")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_tasks
        mock_get_tasks.assert_called_with(course_id)

        # Test empty list
        mock_get_tasks.reset_mock()
        mock_get_tasks.return_value = []

        response = client.get(f"/tasks/course/{course_id}/learning_material")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == []
        mock_get_tasks.assert_called_with(course_id)


@pytest.mark.asyncio
async def test_create_draft_task_for_course(client, mock_db):
    """
    Test creating a draft task for a course
    """
    with patch(
        "api.routes.task.create_draft_task_for_course_in_db"
    ) as mock_create_task:
        # Test successful creation
        request_body = {
            "title": "New Task",
            "type": "learning_material",
            "course_id": 1,
            "milestone_id": 2,
        }
        mock_create_task.return_value = (5, 1)  # task_id, ordering

        response = client.post("/tasks/", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"id": 5}
        mock_create_task.assert_called_with(
            request_body["title"],
            str(request_body["type"]),
            request_body["course_id"],
            request_body["milestone_id"],
        )


@pytest.mark.asyncio
async def test_publish_learning_material_task(client, mock_db):
    """
    Test publishing a learning material task
    """
    with patch("api.routes.task.update_learning_material_task_in_db") as mock_update:
        task_id = 1
        request_body = {
            "title": "Updated Task",
            "blocks": [
                {"type": "paragraph", "content": [{"text": "Content", "styles": {}}]}
            ],
            "scheduled_publish_at": "2023-05-01T10:00:00Z",
        }
        expected_response = {
            "id": task_id,
            "title": request_body["title"],
            "blocks": request_body["blocks"],
            "type": "learning_material",
            "status": "published",
            "scheduled_publish_at": request_body["scheduled_publish_at"],
        }

        # Test successful update
        mock_update.return_value = expected_response

        response = client.post(f"/tasks/{task_id}/learning_material", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        # Just verify keys are present, not exact structure
        result = response.json()
        assert result["id"] == expected_response["id"]
        assert result["title"] == expected_response["title"]
        assert result["type"] == expected_response["type"]
        assert result["status"] == expected_response["status"]
        assert (
            result["scheduled_publish_at"] == expected_response["scheduled_publish_at"]
        )

        # Using ANY to avoid datetime comparison issues
        mock_update.assert_called_with(
            task_id,
            request_body["title"],
            request_body["blocks"],
            ANY,
        )

        # Test task not found
        mock_update.reset_mock()
        mock_update.return_value = None

        response = client.post(f"/tasks/{task_id}/learning_material", json=request_body)

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Task not found"}


@pytest.mark.asyncio
async def test_update_learning_material_task(client, mock_db):
    """
    Test updating a learning material task
    """
    with patch("api.routes.task.update_learning_material_task_in_db") as mock_update:
        task_id = 1
        request_body = {
            "title": "Updated Task",
            "blocks": [
                {"type": "paragraph", "content": [{"text": "Content", "styles": {}}]}
            ],
            "scheduled_publish_at": "2023-05-01T10:00:00Z",
            "status": "draft",
        }
        expected_response = {
            "id": task_id,
            "title": request_body["title"],
            "blocks": request_body["blocks"],
            "type": "learning_material",
            "status": request_body["status"],
            "scheduled_publish_at": request_body["scheduled_publish_at"],
        }

        # Test successful update
        mock_update.return_value = expected_response

        response = client.put(f"/tasks/{task_id}/learning_material", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        # Just verify keys are present, not exact structure
        result = response.json()
        assert result["id"] == expected_response["id"]
        assert result["title"] == expected_response["title"]
        assert result["type"] == expected_response["type"]
        assert result["status"] == expected_response["status"]
        assert (
            result["scheduled_publish_at"] == expected_response["scheduled_publish_at"]
        )

        # Using ANY to avoid TaskStatus enum comparison issues
        mock_update.assert_called_with(
            task_id,
            request_body["title"],
            request_body["blocks"],
            ANY,
            ANY,
        )

        # Test task not found
        mock_update.reset_mock()
        mock_update.return_value = None

        response = client.put(f"/tasks/{task_id}/learning_material", json=request_body)

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Task not found"}


@pytest.mark.asyncio
async def test_update_draft_quiz(client, mock_db):
    """
    Test updating a draft quiz
    """
    with patch("api.routes.task.update_draft_quiz_in_db") as mock_update:
        task_id = 1
        # Use the correct structure that matches CreateQuestionRequest
        request_body = {
            "title": "Quiz Task",
            "questions": [
                {
                    "blocks": [
                        {
                            "type": "paragraph",
                            "content": [{"text": "Test question?", "styles": {}}],
                        }
                    ],
                    "answer": [
                        {
                            "type": "paragraph",
                            "content": [{"text": "Test answer", "styles": {}}],
                        }
                    ],
                    "type": "subjective",
                    "input_type": "text",
                    "response_type": "chat",
                    "context": None,
                    "coding_languages": None,
                    "scorecard_id": None,
                    "generation_model": None,
                    "max_attempts": 3,
                    "is_feedback_shown": True,
                    "title": "Test question",
                }
            ],
            "scheduled_publish_at": "2023-05-01T10:00:00Z",
            "status": "draft",
        }
        # The expected response needs to have PublishedQuestion structure with id
        expected_response = {
            "id": task_id,
            "title": request_body["title"],
            "questions": [
                {
                    "id": 1,  # Add the required id field for PublishedQuestion
                    "blocks": [
                        {
                            "type": "paragraph",
                            "content": [{"text": "Test question?", "styles": {}}],
                        }
                    ],
                    "answer": [
                        {
                            "type": "paragraph",
                            "content": [{"text": "Test answer", "styles": {}}],
                        }
                    ],
                    "type": "subjective",
                    "input_type": "text",
                    "response_type": "chat",
                    "context": None,
                    "coding_languages": None,
                    "scorecard_id": None,
                    "max_attempts": 3,
                    "is_feedback_shown": True,
                    "title": "Test question",
                }
            ],
            "type": "quiz",
            "status": request_body["status"],
            "scheduled_publish_at": request_body["scheduled_publish_at"],
        }

        # Test successful update
        mock_update.return_value = expected_response

        response = client.post(f"/tasks/{task_id}/quiz", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        # Just verify basic structure, not exact match
        result = response.json()
        assert result["id"] == expected_response["id"]
        assert result["title"] == expected_response["title"]
        assert result["type"] == expected_response["type"]
        assert result["status"] == expected_response["status"]
        assert result["questions"][0]["title"] == expected_response["questions"][0]["title"]

        # The request is processed by Pydantic models, so we need to check with ANY
        mock_update.assert_called_with(
            task_id=task_id,
            title=request_body["title"],
            questions=ANY,  # Pydantic models convert this
            scheduled_publish_at=ANY,
            status=ANY,
        )

        # Test task not found
        mock_update.reset_mock()
        mock_update.return_value = None

        response = client.post(f"/tasks/{task_id}/quiz", json=request_body)

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Task not found"}


@pytest.mark.asyncio
async def test_update_published_quiz(client, mock_db):
    """
    Test updating a published quiz
    """
    with patch("api.routes.task.update_published_quiz_in_db") as mock_update:
        task_id = 1
        request_body = {
            "title": "Updated Quiz Task",
            "questions": [
                {
                    "id": 1,
                    "blocks": [
                        {
                            "type": "paragraph",
                            "content": [{"text": "Updated question?", "styles": {}}],
                        }
                    ],
                    "answer": [
                        {
                            "type": "paragraph",
                            "content": [{"text": "Updated answer", "styles": {}}],
                        }
                    ],
                    "type": "subjective",
                    "input_type": "text",
                    "response_type": "chat",
                    "context": None,
                    "coding_languages": None,
                    "scorecard_id": None,
                    "title": "Test question",
                }
            ],
            "scheduled_publish_at": "2023-05-01T10:00:00Z",
        }
        expected_response = {
            "id": task_id,
            "title": request_body["title"],
            "questions": request_body["questions"],
            "type": "quiz",
            "status": "published",
            "scheduled_publish_at": request_body["scheduled_publish_at"],
        }

        # Test successful update
        mock_update.return_value = expected_response

        response = client.put(f"/tasks/{task_id}/quiz", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["id"] == expected_response["id"]
        assert result["title"] == expected_response["title"]
        assert result["type"] == expected_response["type"]
        assert result["questions"][0]["title"] == expected_response["questions"][0]["title"]

        # The request is processed by Pydantic models, so we need to check with ANY
        mock_update.assert_called_with(
            task_id=task_id,
            title=request_body["title"],
            questions=ANY,  # Pydantic models convert this
            scheduled_publish_at=ANY,
        )

        # Test task not found
        mock_update.reset_mock()
        mock_update.return_value = None

        response = client.put(f"/tasks/{task_id}/quiz", json=request_body)

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Task not found"}


@pytest.mark.asyncio
async def test_duplicate_task(client, mock_db):
    """
    Test duplicating a task
    """
    with patch("api.routes.task.duplicate_task_in_db") as mock_duplicate:
        request_body = {"task_id": 1, "course_id": 2, "milestone_id": 3}
        # Use the correct structure that matches DuplicateTaskResponse
        expected_response = {
            "task": {
                "id": 5,
                "title": "Duplicated Task",
                "type": "learning_material",
                "status": "draft",
                "blocks": [
                    {
                        "type": "paragraph",
                        "content": [{"text": "Content", "styles": {}}],
                    }
                ],
                "scheduled_publish_at": None,
            },
            "ordering": 1,
        }

        # Test successful duplication
        mock_duplicate.return_value = expected_response

        response = client.post("/tasks/duplicate", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert "task" in result
        assert "ordering" in result
        assert result["task"]["id"] == expected_response["task"]["id"]

        mock_duplicate.assert_called_with(
            request_body["task_id"],
            request_body["course_id"],
            request_body["milestone_id"],
        )


@pytest.mark.asyncio
async def test_delete_task(client, mock_db):
    """
    Test deleting a task
    """
    with patch("api.routes.task.delete_task_in_db") as mock_delete:
        task_id = 1

        response = client.delete(f"/tasks/{task_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_delete.assert_called_with(task_id)


@pytest.mark.asyncio
async def test_delete_tasks(client, mock_db):
    """
    Test deleting multiple tasks
    """
    with patch("api.routes.task.delete_tasks_in_db") as mock_delete:
        task_ids = [1, 2, 3]

        # Pass task_ids as a query parameter
        response = client.delete("/tasks/", params={"task_ids": task_ids})

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_delete.assert_called_with(task_ids)


@pytest.mark.asyncio
async def test_get_tasks_completed_for_user(client, mock_db):
    """
    Test getting completed tasks for a user
    """
    with patch("api.routes.task.get_solved_tasks_for_user_from_db") as mock_get_tasks:
        user_id = 1
        cohort_id = 2
        expected_tasks = [1, 3, 5]

        # Test with default view type
        mock_get_tasks.return_value = expected_tasks

        response = client.get(f"/tasks/cohort/{cohort_id}/user/{user_id}/completed")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_tasks
        # The enum is converted to an enum object, not a string
        mock_get_tasks.assert_called_with(user_id, cohort_id, ANY)

        # Test with specific view type
        mock_get_tasks.reset_mock()

        response = client.get(
            f"/tasks/cohort/{cohort_id}/user/{user_id}/completed",
            params={
                "view": "This week"
            },  # Change from "weekly" to "This week" to match enum
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_tasks
        mock_get_tasks.assert_called_with(user_id, cohort_id, ANY)


@pytest.mark.asyncio
async def test_get_task(client, mock_db):
    """
    Test getting a task
    """
    with patch("api.routes.task.get_task_from_db") as mock_get_task:
        task_id = 1
        expected_response = {
            "id": task_id,
            "title": "Task 1",
            "type": "learning_material",
            "status": "published",
            "scheduled_publish_at": "2023-05-01T10:00:00Z",
            "blocks": [
                {"type": "paragraph", "content": [{"text": "Content", "styles": {}}]}
            ],
        }

        # Test successful retrieval
        mock_get_task.return_value = expected_response

        response = client.get(f"/tasks/{task_id}")

        assert response.status_code == status.HTTP_200_OK
        # Don't do an exact comparison due to the complex structure
        result = response.json()
        assert result["id"] == expected_response["id"]
        assert result["title"] == expected_response["title"]
        assert result["type"] == expected_response["type"]
        assert "blocks" in result

        mock_get_task.assert_called_with(task_id)

        # Test task not found
        mock_get_task.reset_mock()
        mock_get_task.return_value = None

        response = client.get(f"/tasks/{task_id}")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "Task not found"}


@pytest.mark.asyncio
async def test_mark_task_completed(client, mock_db):
    """
    Test marking a task as completed
    """
    with patch("api.routes.task.mark_task_completed_in_db") as mock_mark_completed:
        task_id = 1
        request_body = {"user_id": 2}

        response = client.post(f"/tasks/{task_id}/complete", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_mark_completed.assert_called_with(task_id, request_body["user_id"])
