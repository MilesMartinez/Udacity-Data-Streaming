"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("processing weather message")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        self.temperature = message.value()['temperature']
        self.status = message.value()['status']