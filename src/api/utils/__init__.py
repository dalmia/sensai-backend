from datetime import datetime, timezone, timedelta
import random
import colorsys


def generate_random_color() -> str:
    # Generate a random hue
    hue = random.random()

    # Create two colors with the same hue but different lightness
    saturation = random.uniform(0.3, 0.9)
    value = random.uniform(0.4, 1.0)
    color = colorsys.hsv_to_rgb(hue, saturation, value)

    # Convert RGB values to hex
    return "#{:02x}{:02x}{:02x}".format(
        int(color[0] * 255), int(color[1] * 255), int(color[2] * 255)
    )


def get_date_from_str(date_str: str, source_timezone: str) -> datetime.date:
    """source_timezone: which timezone the date_str is in. Can be IST or UTC"""
    if source_timezone == "IST":
        # return the date as is
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").date()

    return (
        datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=timezone.utc)  # first convert from utc to ist
        .astimezone(timezone(timedelta(hours=5, minutes=30)))
        .date()  # then get the date
    )


def convert_utc_to_ist(utc_dt: datetime) -> datetime:
    # First ensure the datetime is UTC aware if it isn't already
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)

    # Create IST timezone
    ist = timezone(timedelta(hours=5, minutes=30))

    # Convert to IST
    ist_dt = utc_dt.astimezone(ist)

    return ist_dt
