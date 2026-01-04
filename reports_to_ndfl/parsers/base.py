from abc import ABC, abstractmethod


class BaseBrokerParser(ABC):
    def __init__(self, request, user, target_year):
        self.request = request
        self.user = user
        self.target_year = target_year

    @abstractmethod
    def process(self):
        """Return unified output tuple for display."""
        raise NotImplementedError
