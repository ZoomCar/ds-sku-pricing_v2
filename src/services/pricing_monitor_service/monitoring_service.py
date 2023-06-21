from src.services.pricing_monitor_service.price_level_monitoring import CityCarGroupLevelMonitoring
from src.services.pricing_monitor_service.service_level_monitoring import ServiceMonitoringService
from src.services.pricing_monitor_service.car_id_level_monitoring import CarLevelMonitoringService
from src.util.error_handlers import db_error_handler
from src.util.logger_provider import attach_logger


@attach_logger
class MonitoringService:

    @db_error_handler
    def run(self):
        ServiceMonitoringService().run()
        CityCarGroupLevelMonitoring().run()
        CarLevelMonitoringService().run()



if __name__ == '__main__':
    MonitoringService().run()
