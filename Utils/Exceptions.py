class DeviceParametersException(Exception):
    def __init__(self, text):
        self.txt = text

    def get_text(self):
        return self.txt


class SequenceErrorException(Exception):
    def __init__(self, text):
        self.txt = text

    def get_text(self):
        return self.txt


class CountersException(Exception):
    def __init__(self, text):
        self.txt = text

    def get_text(self):
        return self.txt
