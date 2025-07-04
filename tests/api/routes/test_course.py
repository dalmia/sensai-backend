import pytest
from fastapi import status
from unittest.mock import patch


@pytest.mark.asyncio
async def test_create_course(client, mock_db):
    """
    Test creating a course
    """
    with patch("api.routes.course.create_course_in_db") as mock_create:
        request_body = {"name": "New Course", "org_id": 1}
        course_id = 5

        # Test successful creation
        mock_create.return_value = course_id

        response = client.post("/courses/", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"id": course_id}
        mock_create.assert_called_with(request_body["name"], request_body["org_id"])


@pytest.mark.asyncio
async def test_get_all_courses_for_org(client, mock_db):
    """
    Test getting all courses for an organization
    """
    with patch("api.routes.course.get_all_courses_for_org_from_db") as mock_get_courses:
        org_id = 1
        expected_courses = [
            {"id": 1, "name": "Course 1"},
            {"id": 2, "name": "Course 2"},
        ]

        # Test successful retrieval
        mock_get_courses.return_value = expected_courses

        response = client.get(f"/courses/?org_id={org_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_courses
        mock_get_courses.assert_called_with(org_id)


@pytest.mark.asyncio
async def test_get_course(client, mock_db):
    """
    Test getting a course by ID
    """
    with patch("api.routes.course.get_course_from_db") as mock_get_course:
        course_id = 1
        expected_course = {
            "id": course_id,
            "name": "Course 1",
            "milestones": [
                {
                    "id": 1,
                    "name": "Module 1",
                    "color": "#2d3748",
                    "ordering": 1,
                    "unlock_at": None,
                    "tasks": [
                        {
                            "id": 1,
                            "title": "Task 1",
                            "type": "learning_material",
                            "status": "published",
                            "scheduled_publish_at": None,
                            "ordering": 1,
                            "num_questions": None,
                            "is_generating": False,
                        }
                    ],
                }
            ],
            "course_generation_status": None,
        }

        # Test successful retrieval
        mock_get_course.return_value = expected_course

        response = client.get(f"/courses/{course_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_course
        mock_get_course.assert_called_with(course_id, True)

        # Test with only_published=False
        mock_get_course.reset_mock()
        mock_get_course.return_value = expected_course

        response = client.get(f"/courses/{course_id}?only_published=false")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_course
        mock_get_course.assert_called_with(course_id, False)


@pytest.mark.asyncio
async def test_add_tasks_to_courses(client, mock_db):
    """
    Test adding tasks to courses
    """
    with patch("api.routes.course.add_tasks_to_courses_in_db") as mock_add_tasks:
        request_body = {"course_tasks": [(1, 2, None), (1, 3, None)]}

        response = client.post("/courses/tasks", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_add_tasks.assert_called_with(request_body["course_tasks"])


@pytest.mark.asyncio
async def test_remove_tasks_from_courses(client, mock_db):
    """
    Test removing tasks from courses
    """
    with patch("api.routes.course.remove_tasks_from_courses_in_db") as mock_remove:
        request_body = {"course_tasks": [(1, 1), (2, 2)]}

        # Use client.request instead of client.delete to send JSON data
        response = client.request("DELETE", "/courses/tasks", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_remove.assert_called_with(request_body["course_tasks"])


@pytest.mark.asyncio
async def test_update_task_orders(client, mock_db):
    """
    Test updating task orders
    """
    with patch("api.routes.course.update_task_orders_in_db") as mock_update_orders:
        request_body = {"task_orders": [(1, 1), (2, 2)]}

        response = client.put("/courses/tasks/order", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_update_orders.assert_called_with(request_body["task_orders"])


@pytest.mark.asyncio
async def test_add_milestone_to_course(client, mock_db):
    """
    Test adding a milestone to a course
    """
    with patch("api.routes.course.add_milestone_to_course_in_db") as mock_add_milestone:
        course_id = 1
        request_body = {"name": "New Module", "color": "#2d3748"}
        milestone_id = 5

        # Test successful addition
        mock_add_milestone.return_value = (milestone_id, 3)  # milestone_id, ordering

        response = client.post(f"/courses/{course_id}/milestones", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"id": milestone_id}
        mock_add_milestone.assert_called_with(
            course_id, request_body["name"], request_body["color"]
        )


@pytest.mark.asyncio
async def test_update_milestone_orders(client, mock_db):
    """
    Test updating milestone orders
    """
    with patch("api.routes.course.update_milestone_orders_in_db") as mock_update_orders:
        request_body = {"milestone_orders": [(1, 1), (2, 2)]}

        response = client.put("/courses/milestones/order", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_update_orders.assert_called_with(request_body["milestone_orders"])


@pytest.mark.asyncio
async def test_delete_course(client, mock_db):
    """
    Test deleting a course
    """
    with patch("api.routes.course.delete_course_in_db") as mock_delete:
        course_id = 1

        response = client.delete(f"/courses/{course_id}")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_delete.assert_called_with(course_id)


@pytest.mark.asyncio
async def test_add_course_to_cohorts(client, mock_db):
    """
    Test adding a course to cohorts
    """
    with patch("api.routes.course.add_course_to_cohorts_in_db") as mock_add_to_cohorts:
        course_id = 1
        request_body = {"cohort_ids": [1, 2, 3]}

        response = client.post(f"/courses/{course_id}/cohorts", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_add_to_cohorts.assert_called_with(
            course_id,
            request_body["cohort_ids"],
            is_drip_enabled=False,
            frequency_value=None,
            frequency_unit=None,
            publish_at=None,
        )


@pytest.mark.asyncio
async def test_remove_course_from_cohorts(client, mock_db):
    """
    Test removing a course from cohorts
    """
    with patch("api.routes.course.remove_course_from_cohorts_from_db") as mock_remove:
        course_id = 1
        request_body = {"cohort_ids": [1, 2, 3]}

        # Use client.request instead of client.delete to send JSON data
        response = client.request(
            "DELETE", f"/courses/{course_id}/cohorts", json=request_body
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_remove.assert_called_with(course_id, request_body["cohort_ids"])


@pytest.mark.asyncio
async def test_get_cohorts_for_course(client, mock_db):
    """
    Test getting cohorts for a course
    """
    with patch("api.routes.course.get_cohorts_for_course_from_db") as mock_get_cohorts:
        course_id = 1
        expected_cohorts = [
            {
                "id": 1,
                "name": "Cohort 1",
                "drip_config": {
                    "is_drip_enabled": False,
                    "frequency_value": None,
                    "frequency_unit": None,
                    "publish_at": None,
                },
            },
            {
                "id": 2,
                "name": "Cohort 2",
                "drip_config": {
                    "is_drip_enabled": False,
                    "frequency_value": None,
                    "frequency_unit": None,
                    "publish_at": None,
                },
            },
        ]

        # Test successful retrieval
        mock_get_cohorts.return_value = expected_cohorts

        response = client.get(f"/courses/{course_id}/cohorts")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_cohorts
        mock_get_cohorts.assert_called_with(course_id)


@pytest.mark.asyncio
async def test_get_tasks_for_course(client, mock_db):
    """
    Test getting tasks for a course
    """
    with patch("api.routes.course.get_tasks_for_course_from_db") as mock_get_tasks:
        course_id = 1
        expected_tasks = [
            {"id": 1, "name": "Task 1", "type": "learning_material"},
            {"id": 2, "name": "Task 2", "type": "quiz"},
        ]

        # Test successful retrieval
        mock_get_tasks.return_value = expected_tasks

        response = client.get(f"/courses/{course_id}/tasks")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == expected_tasks
        mock_get_tasks.assert_called_with(course_id)


@pytest.mark.asyncio
async def test_update_course_name(client, mock_db):
    """
    Test updating the name of a course
    """
    with patch("api.routes.course.update_course_name_in_db") as mock_update_name:
        course_id = 1
        request_body = {"name": "Updated Course Name"}

        response = client.put(f"/courses/{course_id}", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_update_name.assert_called_with(course_id, request_body["name"])


@pytest.mark.asyncio
async def test_swap_milestone_ordering(client, mock_db):
    """
    Test swapping milestone ordering
    """
    with patch(
        "api.routes.course.swap_milestone_ordering_for_course_in_db"
    ) as mock_swap:
        course_id = 1
        request_body = {"milestone_1_id": 2, "milestone_2_id": 3}

        response = client.put(
            f"/courses/{course_id}/milestones/swap", json=request_body
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_swap.assert_called_with(
            course_id, request_body["milestone_1_id"], request_body["milestone_2_id"]
        )


@pytest.mark.asyncio
async def test_swap_task_ordering(client, mock_db):
    """
    Test swapping task ordering
    """
    with patch("api.routes.course.swap_task_ordering_for_course_in_db") as mock_swap:
        course_id = 1
        request_body = {"task_1_id": 2, "task_2_id": 3}

        response = client.put(f"/courses/{course_id}/tasks/swap", json=request_body)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"success": True}
        mock_swap.assert_called_with(
            course_id, request_body["task_1_id"], request_body["task_2_id"]
        )
