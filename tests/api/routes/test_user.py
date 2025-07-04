import pytest
from fastapi import status


# Test for the get_user_by_id endpoint
@pytest.mark.asyncio
async def test_get_user_by_id_endpoints(client, mock_db):
    """
    Test all scenarios for the get_user_by_id endpoint
    """
    # Test scenario 1: User exists
    user_id = 1
    expected_user = {
        "id": user_id,
        "first_name": "Test",
        "last_name": "User",
        "email": "test@example.com",
    }
    mock_db["get_user"].return_value = expected_user

    response = client.get(f"/users/{user_id}")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == expected_user
    mock_db["get_user"].assert_called_with(user_id)

    # Test scenario 2: User does not exist
    nonexistent_user_id = 999
    mock_db["get_user"].reset_mock()
    mock_db["get_user"].return_value = None

    response = client.get(f"/users/{nonexistent_user_id}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json() == {"detail": "User not found"}
    mock_db["get_user"].assert_called_with(nonexistent_user_id)


# Test for the update_user endpoint
@pytest.mark.asyncio
async def test_update_user_endpoint(client, mock_db):
    """
    Test all scenarios for the update_user endpoint
    """
    # Test scenario 1: Successful update
    user_id = 1
    updated_user = {
        "id": user_id,
        "first_name": "Updated",
        "middle_name": "Test",
        "last_name": "User",
        "default_dp_color": "blue",
    }
    mock_db["update_user"].return_value = updated_user

    response = client.put(
        f"/users/{user_id}?first_name=Updated&middle_name=Test&last_name=User&default_dp_color=blue"
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == updated_user
    mock_db["update_user"].assert_called_with(
        mock_db["cursor"], user_id, "Updated", "Test", "User", "blue"
    )

    # Test scenario 2: User not found during update
    nonexistent_user_id = 999
    mock_db["update_user"].reset_mock()
    mock_db["update_user"].return_value = None

    response = client.put(
        f"/users/{nonexistent_user_id}?first_name=Updated&middle_name=Test&last_name=User&default_dp_color=blue"
    )

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json() == {"detail": "User not found"}
    mock_db["update_user"].assert_called_with(
        mock_db["cursor"], nonexistent_user_id, "Updated", "Test", "User", "blue"
    )


# Test for the get_user_cohorts endpoint
@pytest.mark.asyncio
async def test_get_user_cohorts_endpoint(client, mock_db):
    """
    Test all scenarios for the get_user_cohorts endpoint
    """
    # Test scenario 1: User has cohorts
    user_id = 1
    expected_cohorts = [{"id": 1, "name": "Cohort 1"}, {"id": 2, "name": "Cohort 2"}]
    mock_db["get_cohorts"].return_value = expected_cohorts

    response = client.get(f"/users/{user_id}/cohorts")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == expected_cohorts
    mock_db["get_cohorts"].assert_called_with(user_id)

    # Test scenario 2: User has no cohorts
    user_id_no_cohorts = 2
    mock_db["get_cohorts"].reset_mock()
    mock_db["get_cohorts"].return_value = []

    response = client.get(f"/users/{user_id_no_cohorts}/cohorts")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []
    mock_db["get_cohorts"].assert_called_with(user_id_no_cohorts)


# Test for the get_user_activity_for_year endpoint
@pytest.mark.asyncio
async def test_get_user_activity_for_year_endpoint(client, mock_db):
    """
    Test all scenarios for the get_user_activity_for_year endpoint
    """
    # Test scenario 1: User has activity data
    user_id = 1
    year = 2023
    expected_activity = [1, 0, 1, 1, 0, 1, 1]  # Activity data for the year
    mock_db["get_activity"].return_value = expected_activity

    response = client.get(f"/users/{user_id}/activity/{year}")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == expected_activity
    mock_db["get_activity"].assert_called_with(user_id, year)

    # Test scenario 2: User has no activity data
    user_id_no_activity = 2
    mock_db["get_activity"].reset_mock()
    mock_db["get_activity"].return_value = []

    response = client.get(f"/users/{user_id_no_activity}/activity/{year}")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []
    mock_db["get_activity"].assert_called_with(user_id_no_activity, year)


# Test for the get_user_active_days endpoint
@pytest.mark.asyncio
async def test_get_user_active_days_endpoint(client, mock_db):
    """
    Test all scenarios for the get_user_active_days endpoint
    """
    # Test scenario 1: User has active days
    user_id = 1
    days = 7
    cohort_id = 1
    expected_active_days = ["2023-04-01", "2023-04-02", "2023-04-03"]
    mock_db["get_active_days"].return_value = expected_active_days

    response = client.get(
        f"/users/{user_id}/active_days?days={days}&cohort_id={cohort_id}"
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == expected_active_days
    mock_db["get_active_days"].assert_called_with(user_id, days, cohort_id)

    # Test scenario 2: User has no active days
    user_id_no_days = 2
    mock_db["get_active_days"].reset_mock()
    mock_db["get_active_days"].return_value = []

    response = client.get(
        f"/users/{user_id_no_days}/active_days?days={days}&cohort_id={cohort_id}"
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []
    mock_db["get_active_days"].assert_called_with(user_id_no_days, days, cohort_id)


# Test for the is_user_present_in_cohort endpoint
@pytest.mark.asyncio
async def test_is_user_in_cohort_endpoint(client, mock_db):
    """
    Test all scenarios for the is_user_present_in_cohort endpoint
    """
    # Test scenario 1: User is in cohort
    user_id = 1
    cohort_id = 1
    mock_db["is_user_in_cohort"].return_value = True

    response = client.get(f"/users/{user_id}/cohort/{cohort_id}/present")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() is True
    mock_db["is_user_in_cohort"].assert_called_with(user_id, cohort_id)

    # Test scenario 2: User is not in cohort
    user_id = 1
    cohort_id = 2
    mock_db["is_user_in_cohort"].reset_mock()
    mock_db["is_user_in_cohort"].return_value = False

    response = client.get(f"/users/{user_id}/cohort/{cohort_id}/present")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() is False
    mock_db["is_user_in_cohort"].assert_called_with(user_id, cohort_id)


# Test for the get_user_streak endpoint
@pytest.mark.asyncio
async def test_get_user_streak_endpoint(client, mock_db):
    """
    Test all scenarios for the get_user_streak endpoint
    """
    # Test scenario 1: User has streak and active days
    user_id = 1
    cohort_id = 1
    streak_days = ["2023-04-01", "2023-04-02", "2023-04-03"]
    active_days = ["2023-04-01", "2023-04-02", "2023-04-03"]

    mock_db["get_streak"].return_value = streak_days
    mock_db["get_active_days"].return_value = active_days

    response = client.get(f"/users/{user_id}/streak?cohort_id={cohort_id}")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "streak_count": len(streak_days),
        "active_days": active_days,
    }
    mock_db["get_streak"].assert_called_with(user_id, cohort_id)
    mock_db["get_active_days"].assert_called_with(user_id, 3, cohort_id)

    # Test scenario 2: User has no streak
    mock_db["get_streak"].reset_mock()
    mock_db["get_active_days"].reset_mock()
    mock_db["get_streak"].return_value = []
    mock_db["get_active_days"].return_value = []

    response = client.get(f"/users/{user_id}/streak?cohort_id={cohort_id}")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"streak_count": 0, "active_days": []}
    mock_db["get_streak"].assert_called_with(user_id, cohort_id)
    mock_db["get_active_days"].assert_called_with(user_id, 3, cohort_id)


# Test for the get_user_courses endpoint
@pytest.mark.asyncio
async def test_get_user_courses_endpoint(client, mock_db):
    """
    Test all scenarios for the get_user_courses endpoint
    """
    # Test scenario 1: User has courses
    user_id = 1
    expected_courses = [
        {
            "id": 1,
            "name": "Course 1",
            "role": "learner",
            "org": {"id": 1, "name": "Test Org", "slug": "test-org"},
            "cohort_id": 1,
        },
        {
            "id": 2,
            "name": "Course 2",
            "role": "admin",
            "org": {"id": 1, "name": "Test Org", "slug": "test-org"},
            "cohort_id": None,
        },
    ]
    mock_db["get_user_courses"].return_value = expected_courses

    response = client.get(f"/users/{user_id}/courses")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == expected_courses
    mock_db["get_user_courses"].assert_called_with(user_id)

    # Test scenario 2: User has no courses
    user_id_no_courses = 2
    mock_db["get_user_courses"].reset_mock()
    mock_db["get_user_courses"].return_value = []

    response = client.get(f"/users/{user_id_no_courses}/courses")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []
    mock_db["get_user_courses"].assert_called_with(user_id_no_courses)


# Test for the get_user_org_cohorts endpoint
@pytest.mark.asyncio
async def test_get_user_org_cohorts_endpoint(client, mock_db):
    """
    Test all scenarios for the get_user_org_cohorts endpoint
    """
    # Test scenario 1: User has cohorts in organization
    user_id = 1
    org_id = 1
    expected_cohorts = [
        {"id": 1, "name": "Org Cohort 1", "role": "learner", "joined_at": None},
        {"id": 2, "name": "Org Cohort 2", "role": "mentor", "joined_at": None},
    ]
    mock_db["get_user_org_cohorts"].return_value = expected_cohorts

    response = client.get(f"/users/{user_id}/org/{org_id}/cohorts")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == expected_cohorts
    mock_db["get_user_org_cohorts"].assert_called_with(user_id, org_id)

    # Test scenario 2: User has no cohorts in organization
    user_id_no_cohorts = 2
    mock_db["get_user_org_cohorts"].reset_mock()
    mock_db["get_user_org_cohorts"].return_value = []

    response = client.get(f"/users/{user_id_no_cohorts}/org/{org_id}/cohorts")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []
    mock_db["get_user_org_cohorts"].assert_called_with(user_id_no_cohorts, org_id)


# Test for the get_user_orgs endpoint
@pytest.mark.asyncio
async def test_get_user_orgs_endpoint(client, mock_db):
    """
    Test all scenarios for the get_user_orgs endpoint
    """
    # Test scenario 1: User belongs to organizations
    user_id = 1
    expected_orgs = [
        {"id": 1, "name": "Org 1", "slug": "org-1"},
        {"id": 2, "name": "Org 2", "slug": "org-2"},
    ]
    mock_db["get_user_organizations"].return_value = expected_orgs

    response = client.get(f"/users/{user_id}/orgs")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == expected_orgs
    mock_db["get_user_organizations"].assert_called_with(user_id)

    # Test scenario 2: User belongs to no organizations
    user_id_no_orgs = 2
    mock_db["get_user_organizations"].reset_mock()
    mock_db["get_user_organizations"].return_value = []

    response = client.get(f"/users/{user_id_no_orgs}/orgs")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []
    mock_db["get_user_organizations"].assert_called_with(user_id_no_orgs)
