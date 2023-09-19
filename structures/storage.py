class SSD:
    def __init__(self, size: int):
        self.size = size  # Size of the SSD in blocks
        self.read_speed = 500  # SSD read speed in MB/s
        self.write_speed = 400  # SSD write speed in MB/s
        self.latency = 0.001  # SSD latency in seconds

class HDD:
    def __init__(self, size: int):
        self.size = size  # Size of the SSD in blocks
        self.read_speed = 200  # HDD read speed in MB/s
        self.write_speed = 100  # HDD write speed in MB/s
        self.latency = 0.005  # HDD latency in seconds