from src.repositories.historical_dpm_repository import HistoricalDpmRepository
from src.util.logger_provider import attach_logger


@attach_logger
class HistoricalDPMManager:
    def run(self):
        try:
            HistoricalDpmRepository().save_historical_dpms()
            self.logger.info("DPM values saved in historical table")
        except Exception as e:
            # self.__trigger_slack_notification_alert('Saving prices to historical table failed')
            self.logger.warning(f"Saving prices to historical table failed: {e}")


if __name__ == "__main__":
    HistoricalDPMManager().run()
