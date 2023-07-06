class BaseEngine:
    def get_engine(self):
        raise NotImplemented("must implement get_engine() method")

    def get_session(self):
        raise NotImplemented("must implement get_session() method")
