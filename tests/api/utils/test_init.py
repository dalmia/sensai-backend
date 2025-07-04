import pytest
import datetime
from src.api.utils import generate_random_color, get_date_from_str, convert_utc_to_ist


class TestGenerateRandomColor:
    def test_generate_random_color_format(self):
        """Test that generate_random_color returns a valid hex color format."""
        color = generate_random_color()

        # Check that the result is a string
        assert isinstance(color, str)

        # Check that it starts with #
        assert color.startswith("#")

        # Check that it's 7 characters long (#RRGGBB)
        assert len(color) == 7

        # Check that the characters after # are valid hex
        hex_part = color[1:]
        assert all(c in "0123456789abcdef" for c in hex_part.lower())


class TestDateUtils:
    def test_get_date_from_str_ist(self):
        """Test converting a date string in IST to a date object."""
        date_str = "2023-05-15 14:30:00"
        result = get_date_from_str(date_str, "IST")

        assert isinstance(result, datetime.date)
        assert result.year == 2023
        assert result.month == 5
        assert result.day == 15

    def test_get_date_from_str_utc(self):
        """Test converting a date string in UTC to a date object in IST."""
        date_str = "2023-05-15 09:00:00"  # 9 AM UTC = 2:30 PM IST
        result = get_date_from_str(date_str, "UTC")

        assert isinstance(result, datetime.date)
        assert result.year == 2023
        assert result.month == 5
        assert result.day == 15  # Same day in this case

    def test_convert_utc_to_ist(self):
        """Test converting a UTC datetime to IST."""
        # Create a UTC datetime
        utc_dt = datetime.datetime(
            2023, 5, 15, 9, 0, 0, tzinfo=datetime.timezone.utc
        )  # 9 AM UTC

        # Convert to IST
        ist_dt = convert_utc_to_ist(utc_dt)

        # Check timezone offset (IST is UTC+5:30)
        assert ist_dt.utcoffset() == datetime.timedelta(hours=5, minutes=30)

        # Check that the time is correctly converted (9 AM UTC = 2:30 PM IST)
        assert ist_dt.hour == 14
        assert ist_dt.minute == 30

    def test_convert_utc_to_ist_naive(self):
        """Test converting a naive UTC datetime to IST."""
        # Create a naive UTC datetime (no timezone info)
        naive_utc_dt = datetime.datetime(2023, 5, 15, 9, 0, 0)  # 9 AM UTC

        # Convert to IST
        ist_dt = convert_utc_to_ist(naive_utc_dt)

        # Check timezone offset (IST is UTC+5:30)
        assert ist_dt.utcoffset() == datetime.timedelta(hours=5, minutes=30)

        # Check that the time is correctly converted (9 AM UTC = 2:30 PM IST)
        assert ist_dt.hour == 14
        assert ist_dt.minute == 30
