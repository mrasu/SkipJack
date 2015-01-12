from model.sonar_status import SonarStatus


class GridStatus(SonarStatus):
    def __init__(self):
        SonarStatus.__init__(self)
        self.condition = None
