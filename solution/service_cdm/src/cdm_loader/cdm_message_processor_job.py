from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer

from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger
                 ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        for _ in range(self._batch_size):
            self._logger.info("TRY CONSUME")
            msg = self._consumer.consume()
            self._logger.info(f"CONSUMED msg={msg}")
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: No more messages (msg=None). STOP")
                break

            try:
                self._cdm_repository.load_cdm(msg)
                self._logger.info(f"{datetime.utcnow()}: CDM loaded object_id={msg}")

            except Exception as e:
                self._logger.exception(
                    f"{datetime.utcnow()}: ERROR processing message object_id={msg} error={e}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
