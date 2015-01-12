class SonarStatus:
    def __init__(self):
        self.__count = 0
        self.output = None
    
    def get_count(self):
        return self.__count

    def __increment_count(self):
        self.__count += 1
