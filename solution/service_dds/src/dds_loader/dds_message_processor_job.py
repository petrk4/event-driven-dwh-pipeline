from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer

from dds_loader.repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def _hash(self, value: str):
        return self._dds_repository._hash_key(value)

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START batch_size={self._batch_size}")
        self._logger.info(f"{datetime.utcnow()}: START")
        for _ in range(self._batch_size):
            self._logger.info("TRY CONSUME")
            msg = self._consumer.consume()
            self._logger.info(f"CONSUMED msg={msg}")
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: No more messages (msg=None). STOP")
                break

            try:
                self._dds_repository.load_dds(msg)
                self._logger.info(f"{datetime.utcnow()}: DDS loaded object_id={msg['object_id']}")

                user_id = self._hash(msg['payload']["user"]["id"])

                for product in msg['payload']["products"]:
                    product_id = self._hash(product["id"])
                    category_id = self._hash(product["category"])

                    cdm_product_event = {
                        "user_id": str(user_id),
                        "type": "user_product",
                        "product_id": str(product_id),
                        "product_name": product["name"],
                        "order_cnt": product["quantity"]
                    }

                    self._producer.produce(cdm_product_event)

                    cdm_category_event = {
                        "user_id": str(user_id),
                        "type": "user_category",
                        "category_id": str(category_id),
                        "category_name": product["category"],
                        "order_cnt": product["quantity"]
                    }

                    self._producer.produce(cdm_category_event)

            except Exception as e:
                self._logger.exception(
                    f"{datetime.utcnow()}: ERROR processing message object_id={msg.get('object_id')} error={e}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
