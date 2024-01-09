from structures import file
class IORequest:
    def __init__(self, file, timestamp, requestType, offsetStart, offsetEnd):
        self.file = file
        self.timestamp = timestamp
        self.requestType = requestType
        self.offsetStart = offsetStart
        self.offsetEnd = offsetEnd
        self.execution_start_time = 0
        self.execution_end_time = None

    @property
    def waiting_time(self):
        if self.execution_start_time is not None:
            return self.execution_start_time - self.timestamp
        return 0
