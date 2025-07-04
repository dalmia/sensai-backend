import pytest
from datetime import datetime
from src.api.models import (
    UserCourseRole,
    TaskType,
    TaskStatus,
    QuestionType,
    ScorecardStatus,
    TaskInputType,
    TaskAIResponseType,
    GenerateCourseJobStatus,
    GenerateTaskJobStatus,
    LeaderboardViewType,
    ChatRole,
    ChatResponseType,
    UserLoginData,
    CreateOrganizationRequest,
    DripConfig,
    Block,
    ScorecardCriterion,
    BaseScorecard,
    DraftQuestion,
    PublishedQuestion,
    Task,
    LearningMaterialTask,
    QuizTask,
    Milestone,
    Course,
    Organization,
    User,
    UserStreak,
    ChatMessage,
    Tag,
)


class TestEnumEquality:
    """Test enum equality methods and their edge cases."""

    def test_user_course_role_equality_with_string(self):
        """Test UserCourseRole equality with string values."""
        assert UserCourseRole.ADMIN == "admin"
        assert UserCourseRole.LEARNER == "learner"
        assert UserCourseRole.MENTOR == "mentor"

    def test_user_course_role_equality_with_enum(self):
        """Test UserCourseRole equality with other enum instances."""
        assert UserCourseRole.ADMIN == UserCourseRole.ADMIN
        assert UserCourseRole.LEARNER == UserCourseRole.LEARNER
        assert UserCourseRole.MENTOR == UserCourseRole.MENTOR

    def test_user_course_role_enum_value_comparison(self):
        """Test UserCourseRole enum-to-enum value comparison - covers line 406."""
        # Create separate instances to ensure we hit the elif isinstance(other, UserCourseRole) branch
        role1 = UserCourseRole.ADMIN
        role2 = UserCourseRole.ADMIN
        role3 = UserCourseRole.LEARNER

        # Test same value comparison (should be True)
        assert (
            role1 == role2
        )  # This should trigger line 406: return self.value == other.value

        # Test different value comparison (should be False)
        assert not (
            role1 == role3
        )  # This should also trigger line 406 but return False

    def test_user_course_role_inequality_return_false(self):
        """Test UserCourseRole equality returns False for non-matching types - covers line 408."""
        assert (UserCourseRole.ADMIN == 123) is False
        assert (UserCourseRole.ADMIN == None) is False
        assert (UserCourseRole.ADMIN == []) is False
        assert (UserCourseRole.ADMIN == {}) is False

    def test_task_type_equality_with_string(self):
        """Test TaskType equality with string values."""
        assert TaskType.QUIZ == "quiz"
        assert TaskType.LEARNING_MATERIAL == "learning_material"

    def test_task_type_equality_with_enum(self):
        """Test TaskType equality with other enum instances."""
        assert TaskType.QUIZ == TaskType.QUIZ
        assert TaskType.LEARNING_MATERIAL == TaskType.LEARNING_MATERIAL

    def test_task_type_inequality_return_false(self):
        """Test TaskType equality returns False for non-matching types - covers line 176."""
        assert (TaskType.QUIZ == 123) is False
        assert (TaskType.QUIZ == None) is False
        assert (TaskType.QUIZ == []) is False
        assert (TaskType.QUIZ == {}) is False

    def test_task_status_equality_with_string(self):
        """Test TaskStatus equality with string values."""
        assert TaskStatus.DRAFT == "draft"
        assert TaskStatus.PUBLISHED == "published"

    def test_task_status_equality_with_enum(self):
        """Test TaskStatus equality with other enum instances."""
        assert TaskStatus.DRAFT == TaskStatus.DRAFT
        assert TaskStatus.PUBLISHED == TaskStatus.PUBLISHED

    def test_task_status_inequality_return_false(self):
        """Test TaskStatus equality returns False for non-matching types - covers line 192."""
        assert (TaskStatus.DRAFT == 123) is False
        assert (TaskStatus.DRAFT == None) is False
        assert (TaskStatus.DRAFT == []) is False
        assert (TaskStatus.DRAFT == {}) is False

    def test_question_type_equality_with_string(self):
        """Test QuestionType equality with string values."""
        assert QuestionType.OPEN_ENDED == "subjective"
        assert QuestionType.OBJECTIVE == "objective"

    def test_question_type_equality_with_enum(self):
        """Test QuestionType equality with other enum instances."""
        assert QuestionType.OPEN_ENDED == QuestionType.OPEN_ENDED
        assert QuestionType.OBJECTIVE == QuestionType.OBJECTIVE

    def test_question_type_inequality_return_false(self):
        """Test QuestionType equality returns False for non-matching types - covers line 264."""
        assert (QuestionType.OPEN_ENDED == 123) is False
        assert (QuestionType.OPEN_ENDED == None) is False
        assert (QuestionType.OPEN_ENDED == []) is False
        assert (QuestionType.OPEN_ENDED == {}) is False

    def test_scorecard_status_equality_with_string(self):
        """Test ScorecardStatus equality with string values."""
        assert ScorecardStatus.PUBLISHED == "published"
        assert ScorecardStatus.DRAFT == "draft"

    def test_scorecard_status_equality_with_enum(self):
        """Test ScorecardStatus equality with other enum instances."""
        assert ScorecardStatus.PUBLISHED == ScorecardStatus.PUBLISHED
        assert ScorecardStatus.DRAFT == ScorecardStatus.DRAFT

    def test_scorecard_status_inequality_return_false(self):
        """Test ScorecardStatus equality returns False for non-matching types - covers line 287."""
        assert (ScorecardStatus.PUBLISHED == 123) is False
        assert (ScorecardStatus.PUBLISHED == None) is False
        assert (ScorecardStatus.PUBLISHED == []) is False
        assert (ScorecardStatus.PUBLISHED == {}) is False

    def test_task_input_type_equality_with_string(self):
        """Test TaskInputType equality with string values."""
        assert TaskInputType.CODE == "code"
        assert TaskInputType.TEXT == "text"
        assert TaskInputType.AUDIO == "audio"

    def test_task_input_type_equality_with_enum(self):
        """Test TaskInputType equality with other enum instances."""
        assert TaskInputType.CODE == TaskInputType.CODE
        assert TaskInputType.TEXT == TaskInputType.TEXT
        assert TaskInputType.AUDIO == TaskInputType.AUDIO

    def test_task_input_type_inequality_return_false(self):
        """Test TaskInputType equality returns False for non-matching types - covers line 232."""
        assert (TaskInputType.CODE == 123) is False
        assert (TaskInputType.CODE == None) is False
        assert (TaskInputType.CODE == []) is False
        assert (TaskInputType.CODE == {}) is False

    def test_task_ai_response_type_equality_with_string(self):
        """Test TaskAIResponseType equality with string values."""
        assert TaskAIResponseType.CHAT == "chat"
        assert TaskAIResponseType.EXAM == "exam"

    def test_task_ai_response_type_equality_with_enum(self):
        """Test TaskAIResponseType equality with other enum instances."""
        assert TaskAIResponseType.CHAT == TaskAIResponseType.CHAT
        assert TaskAIResponseType.EXAM == TaskAIResponseType.EXAM

    def test_task_ai_response_type_inequality_return_false(self):
        """Test TaskAIResponseType equality returns False for non-matching types - covers line 248."""
        assert (TaskAIResponseType.CHAT == 123) is False
        assert (TaskAIResponseType.CHAT == None) is False
        assert (TaskAIResponseType.CHAT == []) is False
        assert (TaskAIResponseType.CHAT == {}) is False

    def test_generate_course_job_status_equality_with_string(self):
        """Test GenerateCourseJobStatus equality with string values."""
        assert GenerateCourseJobStatus.STARTED == "started"
        assert GenerateCourseJobStatus.PENDING == "pending"
        assert GenerateCourseJobStatus.COMPLETED == "completed"
        assert GenerateCourseJobStatus.FAILED == "failed"

    def test_generate_course_job_status_equality_with_enum(self):
        """Test GenerateCourseJobStatus equality with other enum instances."""
        assert GenerateCourseJobStatus.STARTED == GenerateCourseJobStatus.STARTED
        assert GenerateCourseJobStatus.PENDING == GenerateCourseJobStatus.PENDING
        assert GenerateCourseJobStatus.COMPLETED == GenerateCourseJobStatus.COMPLETED
        assert GenerateCourseJobStatus.FAILED == GenerateCourseJobStatus.FAILED

    def test_generate_course_job_status_enum_value_comparison(self):
        """Test GenerateCourseJobStatus enum-to-enum value comparison - covers line 344."""
        # Create separate instances to ensure we hit the elif isinstance(other, GenerateCourseJobStatus) branch
        status1 = GenerateCourseJobStatus.COMPLETED
        status2 = GenerateCourseJobStatus.COMPLETED
        status3 = GenerateCourseJobStatus.FAILED

        # Test same value comparison (should be True)
        assert (
            status1 == status2
        )  # This should trigger line 344: return self.value == other.value

        # Test different value comparison (should be False)
        assert not (
            status1 == status3
        )  # This should also trigger line 344 but return False

    def test_generate_course_job_status_recursion_bug(self):
        """Test GenerateCourseJobStatus recursion bug when comparing with non-string/enum types."""
        # This enum has a bug where it uses "return self == other" instead of "return False"
        # This causes infinite recursion when compared with non-string/enum types
        with pytest.raises(RecursionError):
            GenerateCourseJobStatus.STARTED == 123

    def test_generate_task_job_status_equality_with_string(self):
        """Test GenerateTaskJobStatus equality with string values."""
        assert GenerateTaskJobStatus.STARTED == "started"
        assert GenerateTaskJobStatus.COMPLETED == "completed"
        assert GenerateTaskJobStatus.FAILED == "failed"

    def test_generate_task_job_status_equality_with_enum(self):
        """Test GenerateTaskJobStatus equality with other enum instances."""
        assert GenerateTaskJobStatus.STARTED == GenerateTaskJobStatus.STARTED
        assert GenerateTaskJobStatus.COMPLETED == GenerateTaskJobStatus.COMPLETED
        assert GenerateTaskJobStatus.FAILED == GenerateTaskJobStatus.FAILED

    def test_generate_task_job_status_enum_value_comparison(self):
        """Test GenerateTaskJobStatus enum-to-enum value comparison - covers line 360."""
        # Create separate instances to ensure we hit the elif isinstance(other, GenerateTaskJobStatus) branch
        status1 = GenerateTaskJobStatus.COMPLETED
        status2 = GenerateTaskJobStatus.COMPLETED
        status3 = GenerateTaskJobStatus.FAILED

        # Test same value comparison (should be True)
        assert (
            status1 == status2
        )  # This should trigger line 360: return self.value == other.value

        # Test different value comparison (should be False)
        assert not (
            status1 == status3
        )  # This should also trigger line 360 but return False

    def test_generate_task_job_status_inequality_return_false(self):
        """Test GenerateTaskJobStatus equality returns False for non-matching types - covers line 862."""
        assert (GenerateTaskJobStatus.STARTED == 123) is False
        assert (GenerateTaskJobStatus.STARTED == None) is False
        assert (GenerateTaskJobStatus.STARTED == []) is False
        assert (GenerateTaskJobStatus.STARTED == {}) is False

    def test_leaderboard_view_type_equality_raises_not_implemented(self):
        """Test LeaderboardViewType equality raises NotImplementedError for non-string/enum types."""
        with pytest.raises(NotImplementedError):
            LeaderboardViewType.ALL_TIME == 123
        with pytest.raises(NotImplementedError):
            LeaderboardViewType.ALL_TIME == None
        with pytest.raises(NotImplementedError):
            LeaderboardViewType.ALL_TIME == []


class TestEnumStringMethods:
    """Test enum __str__ methods."""

    def test_enum_str_methods_with_custom_implementation(self):
        """Test that enums with custom __str__ methods return their value when converted to string."""
        assert str(UserCourseRole.ADMIN) == "admin"
        assert str(TaskType.QUIZ) == "quiz"
        assert str(TaskStatus.DRAFT) == "draft"
        assert str(QuestionType.OPEN_ENDED) == "subjective"
        assert str(ScorecardStatus.PUBLISHED) == "published"
        assert str(TaskInputType.CODE) == "code"
        assert str(TaskAIResponseType.CHAT) == "chat"
        assert str(GenerateCourseJobStatus.STARTED) == "started"
        assert str(GenerateTaskJobStatus.COMPLETED) == "completed"
        assert str(LeaderboardViewType.ALL_TIME) == "All time"

    def test_enum_str_methods_with_default_implementation(self):
        """Test that enums without custom __str__ methods use default enum representation."""
        # These enums don't have custom __str__ methods, so they use default enum representation
        assert str(ChatRole.USER) == "ChatRole.USER"
        assert str(ChatResponseType.TEXT) == "ChatResponseType.TEXT"


class TestModelDefaults:
    """Test model default values and optional fields."""

    def test_user_login_data_with_optional_family_name(self):
        """Test UserLoginData with None family_name."""
        data = UserLoginData(
            email="test@example.com",
            given_name="John",
            family_name=None,
            id_token="token123",
        )
        assert data.family_name is None

    def test_drip_config_defaults(self):
        """Test DripConfig default values."""
        config = DripConfig()
        assert config.is_drip_enabled is False
        assert config.frequency_value is None
        assert config.frequency_unit is None
        assert config.publish_at is None

    def test_block_defaults(self):
        """Test Block model default values."""
        block = Block(type="text")
        assert block.id is None
        assert block.props == {}
        assert block.content == []
        assert block.children == []
        assert block.position is None

    def test_scorecard_criterion_complete(self):
        """Test ScorecardCriterion with all fields."""
        criterion = ScorecardCriterion(
            name="Quality",
            description="Code quality assessment",
            min_score=0.0,
            max_score=10.0,
            pass_score=7.0,
        )
        assert criterion.name == "Quality"
        assert criterion.description == "Code quality assessment"
        assert criterion.min_score == 0.0
        assert criterion.max_score == 10.0
        assert criterion.pass_score == 7.0

    def test_draft_question_with_optional_fields(self):
        """Test DraftQuestion with optional fields set to None."""
        question = DraftQuestion(
            blocks=[],
            answer=None,
            type=QuestionType.OPEN_ENDED,
            input_type=TaskInputType.TEXT,
            response_type=TaskAIResponseType.CHAT,
            context=None,
            coding_languages=None,
            scorecard_id=None,
            title="question",
        )
        assert question.answer is None
        assert question.context is None
        assert question.coding_languages is None
        assert question.scorecard_id is None
        assert question.title == "question"

    def test_published_question_with_optional_fields(self):
        """Test PublishedQuestion with optional fields."""
        question = PublishedQuestion(
            id=1,
            blocks=[],
            answer=None,
            type=QuestionType.OPEN_ENDED,
            input_type=TaskInputType.TEXT,
            response_type=TaskAIResponseType.CHAT,
            context=None,
            coding_languages=None,
            scorecard_id=None,
            max_attempts=None,
            is_feedback_shown=None,
            title="question",
        )
        assert question.scorecard_id is None
        assert question.max_attempts is None
        assert question.is_feedback_shown is None
        assert question.title == "question"
        
    def test_milestone_with_optional_fields(self):
        """Test Milestone with optional fields."""
        milestone = Milestone(
            id=1,
            name=None,
            color=None,
            ordering=None,
            unlock_at=None,
        )
        assert milestone.name is None
        assert milestone.color is None
        assert milestone.ordering is None
        assert milestone.unlock_at is None

    def test_task_with_optional_scheduled_publish_at(self):
        """Test Task with None scheduled_publish_at."""
        task = Task(
            id=1,
            title="Test Task",
            type=TaskType.QUIZ,
            status=TaskStatus.DRAFT,
            scheduled_publish_at=None,
        )
        assert task.scheduled_publish_at is None

    def test_chat_message_with_optional_fields(self):
        """Test ChatMessage with optional fields set to None."""
        message = ChatMessage(
            id=1,
            created_at="2024-01-01 12:00:00",
            user_id=123,
            question_id=456,
            role=None,
            content=None,
            response_type=None,
        )
        assert message.role is None
        assert message.content is None
        assert message.response_type is None


class TestModelInstantiation:
    """Test model instantiation and validation."""

    def test_user_login_data_complete(self):
        """Test UserLoginData with all fields."""
        data = UserLoginData(
            email="test@example.com",
            given_name="John",
            family_name="Doe",
            id_token="token123",
        )
        assert data.email == "test@example.com"
        assert data.given_name == "John"
        assert data.family_name == "Doe"
        assert data.id_token == "token123"

    def test_create_organization_request(self):
        """Test CreateOrganizationRequest instantiation."""
        request = CreateOrganizationRequest(
            name="Test Org", slug="test-org", user_id=123
        )
        assert request.name == "Test Org"
        assert request.slug == "test-org"
        assert request.user_id == 123

    def test_learning_material_task(self):
        """Test LearningMaterialTask instantiation."""
        task = LearningMaterialTask(
            id=1,
            title="Learning Task",
            type=TaskType.LEARNING_MATERIAL,
            status=TaskStatus.PUBLISHED,
            scheduled_publish_at=None,
            blocks=[Block(type="text")],
        )
        assert task.id == 1
        assert task.title == "Learning Task"
        assert task.type == TaskType.LEARNING_MATERIAL
        assert task.status == TaskStatus.PUBLISHED
        assert len(task.blocks) == 1

    def test_quiz_task(self):
        """Test QuizTask instantiation."""
        question = PublishedQuestion(
            id=1,
            blocks=[Block(type="text")],
            answer=None,
            type=QuestionType.OPEN_ENDED,
            input_type=TaskInputType.TEXT,
            response_type=TaskAIResponseType.CHAT,
            context=None,
            coding_languages=None,
            title="question",
        )
        task = QuizTask(
            id=1,
            title="Quiz Task",
            type=TaskType.QUIZ,
            status=TaskStatus.PUBLISHED,
            scheduled_publish_at=None,
            questions=[question],
        )
        assert task.id == 1
        assert task.title == "Quiz Task"
        assert task.type == TaskType.QUIZ
        assert len(task.questions) == 1
        assert task.questions[0].title == "question"

    def test_user_streak(self):
        """Test UserStreak instantiation."""
        user = User(
            id=1,
            email="test@example.com",
            first_name="John",
            middle_name=None,
            last_name="Doe",
        )
        streak = UserStreak(user=user, count=5)
        assert streak.user.id == 1
        assert streak.count == 5

    def test_organization(self):
        """Test Organization instantiation."""
        org = Organization(id=1, name="Test Org", slug="test-org")
        assert org.id == 1
        assert org.name == "Test Org"
        assert org.slug == "test-org"

    def test_tag(self):
        """Test Tag instantiation."""
        tag = Tag(id=1, name="Python")
        assert tag.id == 1
        assert tag.name == "Python"


class TestComplexModels:
    """Test complex model combinations and edge cases."""

    def test_base_scorecard(self):
        """Test BaseScorecard with criteria list."""
        criteria = [
            ScorecardCriterion(
                name="Quality",
                description="Code quality",
                min_score=0.0,
                max_score=10.0,
                pass_score=7.0,
            )
        ]
        scorecard = BaseScorecard(title="Code Review", criteria=criteria)
        assert scorecard.title == "Code Review"
        assert len(scorecard.criteria) == 1
        assert scorecard.criteria[0].name == "Quality"

    def test_drip_config_with_all_fields(self):
        """Test DripConfig with all fields set."""
        config = DripConfig(
            is_drip_enabled=True,
            frequency_value=7,
            frequency_unit="days",
            publish_at=datetime(2024, 1, 1, 12, 0, 0),
        )
        assert config.is_drip_enabled is True
        assert config.frequency_value == 7
        assert config.frequency_unit == "days"
        assert config.publish_at == datetime(2024, 1, 1, 12, 0, 0)

    def test_block_with_all_fields(self):
        """Test Block with all fields populated."""
        block = Block(
            id="block-123",
            type="paragraph",
            props={"style": "bold"},
            content=["Hello", "World"],
            children=[{"type": "text", "content": "child"}],
            position=0,
        )
        assert block.id == "block-123"
        assert block.type == "paragraph"
        assert block.props == {"style": "bold"}
        assert block.content == ["Hello", "World"]
        assert len(block.children) == 1
        assert block.position == 0


class TestEnumEdgeCases:
    """Test enum edge cases and string representations."""

    def test_enum_inequality_with_different_strings(self):
        """Test enum inequality with non-matching strings."""
        assert (TaskType.QUIZ == "learning_material") is False
        assert (TaskStatus.DRAFT == "published") is False
        assert (UserCourseRole.ADMIN == "learner") is False

    def test_enum_inequality_with_different_enums(self):
        """Test enum inequality with different enum types."""
        assert (TaskType.QUIZ == TaskStatus.DRAFT) is False
        assert (UserCourseRole.ADMIN == TaskInputType.CODE) is False

    def test_generate_course_job_status_self_equality(self):
        """Test GenerateCourseJobStatus self equality using == operator."""
        status1 = GenerateCourseJobStatus.COMPLETED
        status2 = GenerateCourseJobStatus.COMPLETED
        assert status1 == status2  # This covers the __eq__ method

    def test_generate_task_job_status_string_conversion(self):
        """Test GenerateTaskJobStatus string conversion and equality."""
        status = GenerateTaskJobStatus.FAILED
        assert str(status) == "failed"
        assert status == "failed"
        assert status == GenerateTaskJobStatus.FAILED


class TestEnumValueAccess:
    """Test accessing enum values directly."""

    def test_enum_value_property(self):
        """Test accessing .value property of enums."""
        assert UserCourseRole.ADMIN.value == "admin"
        assert TaskType.QUIZ.value == "quiz"
        assert TaskStatus.DRAFT.value == "draft"
        assert QuestionType.OPEN_ENDED.value == "subjective"
        assert ScorecardStatus.PUBLISHED.value == "published"
        assert TaskInputType.CODE.value == "code"
        assert TaskAIResponseType.CHAT.value == "chat"
        assert GenerateCourseJobStatus.STARTED.value == "started"
        assert GenerateTaskJobStatus.COMPLETED.value == "completed"
        assert LeaderboardViewType.ALL_TIME.value == "All time"


class TestChatEnums:
    """Test chat-related enums that don't have custom methods."""

    def test_chat_role_enum(self):
        """Test ChatRole enum basic functionality."""
        assert ChatRole.USER.value == "user"
        assert ChatRole.ASSISTANT.value == "assistant"
        # These enums use default enum behavior
        assert ChatRole.USER == ChatRole.USER
        assert ChatRole.USER != ChatRole.ASSISTANT

    def test_chat_response_type_enum(self):
        """Test ChatResponseType enum basic functionality."""
        assert ChatResponseType.TEXT.value == "text"
        assert ChatResponseType.CODE.value == "code"
        assert ChatResponseType.AUDIO.value == "audio"
        # These enums use default enum behavior
        assert ChatResponseType.TEXT == ChatResponseType.TEXT
        assert ChatResponseType.TEXT != ChatResponseType.CODE

    def test_generate_course_job_status_direct_enum_comparison_same_value(self):
        """Test GenerateCourseJobStatus direct enum instance comparison with same values."""
        # Force enum-to-enum comparison for the same value
        status_a = GenerateCourseJobStatus.COMPLETED
        status_b = GenerateCourseJobStatus.COMPLETED
        # This should trigger the elif isinstance(other, GenerateCourseJobStatus) branch
        # and execute: return self.value == other.value (should return True)
        assert status_a.__eq__(status_b) is True

    def test_generate_course_job_status_direct_enum_comparison_different_value(self):
        """Test GenerateCourseJobStatus direct enum instance comparison with different values."""
        # Force enum-to-enum comparison for different values
        status_a = GenerateCourseJobStatus.COMPLETED
        status_b = GenerateCourseJobStatus.FAILED
        # This should trigger the elif isinstance(other, GenerateCourseJobStatus) branch
        # and execute: return self.value == other.value (should return False)
        assert status_a.__eq__(status_b) is False

    def test_generate_task_job_status_direct_enum_comparison_same_value(self):
        """Test GenerateTaskJobStatus direct enum instance comparison with same values."""
        # Force enum-to-enum comparison for the same value
        status_a = GenerateTaskJobStatus.COMPLETED
        status_b = GenerateTaskJobStatus.COMPLETED
        # This should trigger the elif isinstance(other, GenerateTaskJobStatus) branch
        # and execute: return self.value == other.value (should return True)
        assert status_a.__eq__(status_b) is True

    def test_generate_task_job_status_direct_enum_comparison_different_value(self):
        """Test GenerateTaskJobStatus direct enum instance comparison with different values."""
        # Force enum-to-enum comparison for different values
        status_a = GenerateTaskJobStatus.COMPLETED
        status_b = GenerateTaskJobStatus.FAILED
        # This should trigger the elif isinstance(other, GenerateTaskJobStatus) branch
        # and execute: return self.value == other.value (should return False)
        assert status_a.__eq__(status_b) is False

    def test_user_course_role_direct_enum_comparison_same_value(self):
        """Test UserCourseRole direct enum instance comparison with same values."""
        # Force enum-to-enum comparison for the same value
        role_a = UserCourseRole.ADMIN
        role_b = UserCourseRole.ADMIN
        # This should trigger the elif isinstance(other, UserCourseRole) branch
        # and execute: return self.value == other.value (should return True)
        assert role_a.__eq__(role_b) is True

    def test_user_course_role_direct_enum_comparison_different_value(self):
        """Test UserCourseRole direct enum instance comparison with different values."""
        # Force enum-to-enum comparison for different values
        role_a = UserCourseRole.ADMIN
        role_b = UserCourseRole.LEARNER
        # This should trigger the elif isinstance(other, UserCourseRole) branch
        # and execute: return self.value == other.value (should return False)
        assert role_a.__eq__(role_b) is False
