import base64


def prepare_audio_input_for_ai(audio_data: bytes):
    return base64.b64encode(audio_data).decode("utf-8")
