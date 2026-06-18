import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    @staticmethod
    def _hash_key(value: str) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_OID, str(value))

    def load_dds(self, msg):
        payload = msg['payload']
        load_dt = datetime.utcnow()
        src = 'kafka'

        h_user_pk = self._hash_key(payload["user"]["id"])
        self.h_user_insert(h_user_pk, payload["user"]["id"], load_dt, src)

        h_restaurant_pk = self._hash_key(payload["restaurant"]["id"])
        self.h_restaurant_insert(h_restaurant_pk, payload["restaurant"]["id"], load_dt, src)

        order_dt = datetime.strptime(payload["date"],"%Y-%m-%d %H:%M:%S")
        h_order_pk = self._hash_key(str(payload["id"]))
        self.h_order_insert(h_order_pk, payload["id"], load_dt, order_dt, src)

        hk_order_user_pk = self._hash_key(f"{payload['id']}|{payload['user']['id']}")
        self.l_order_user_insert(hk_order_user_pk, h_order_pk, h_user_pk, load_dt, src)

        for item in payload["products"]:
            h_category_pk = self._hash_key(item["category"])
            self.h_category_insert(h_category_pk, item["category"], load_dt, src)

            h_product_pk = self._hash_key(item["id"])
            self.h_product_insert(h_product_pk, item["id"], load_dt, src)

            hk_order_product_pk = self._hash_key(f"{payload['id']}|{item['id']}")
            self.l_order_product_insert(hk_order_product_pk, h_order_pk, h_product_pk, load_dt, src)

            hk_product_restaurant_pk = self._hash_key(f"{payload['restaurant']['id']}|{item['id']}")
            self.l_product_restaurant_insert(hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, src)

            hk_product_category_pk = self._hash_key(f"{item['category']}|{item['id']}")
            self.l_product_category_insert(hk_product_category_pk, h_category_pk, h_product_pk, load_dt, src)

            hk_product_names_hashdiff = self._hash_key(f"{item['id']}|{item['name']}")
            self.s_product_names_insert(hk_product_names_hashdiff, h_product_pk, item['name'], load_dt,src)

        hk_user_names_hashdiff = self._hash_key(f"{payload['user']['id']}|{payload['user']['name']}|{payload['user']['login']}")
        self.s_user_names_insert(hk_user_names_hashdiff, h_user_pk, payload['user']['name'], payload['user']['login'], load_dt, src)

        hk_restaurant_names_hashdiff = self._hash_key(f"{payload['restaurant']['id']}|{payload['restaurant']['name']}")
        self.s_restaurant_names_insert(hk_restaurant_names_hashdiff, h_restaurant_pk, payload['restaurant']['name'], load_dt, src)

        hk_order_status_hashdiff = self._hash_key(f"{payload['id']}|{payload['status']}")
        self.s_order_status_insert(hk_order_status_hashdiff, h_order_pk, payload['status'],load_dt, src)

        hk_order_cost_hashdiff = self._hash_key(f"{payload['id']}|{payload['cost']}|{payload['payment']}")
        self.s_order_cost_insert(hk_order_cost_hashdiff, h_order_pk, payload['cost'], payload['payment'], load_dt, src)


    def h_user_insert(self,
                            h_user_pk: uuid.UUID,
                            user_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id) DO NOTHING
                    """,
                    (
                        h_user_pk,
                        user_id,
                        load_dt,
                        load_src
                    )
                )

    def h_restaurant_insert(self,
                            h_restaurant_pk: uuid.UUID,
                            restaurant_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (restaurant_id) DO NOTHING
                    """,
                    (
                        h_restaurant_pk,
                        restaurant_id,
                        load_dt,
                        load_src
                    )
                )

    def h_order_insert(self,
                            h_order_pk: uuid.UUID,
                            order_id: int,
                            load_dt: datetime,
                            order_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order (h_order_pk, order_id, load_dt, order_dt, load_src) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (order_id) DO NOTHING
                    """,
                    (
                        h_order_pk,
                        order_id,
                        load_dt,
                        order_dt,
                        load_src
                    )
                )

    def h_category_insert(self,
                            h_category_pk: uuid.UUID,
                            category_name: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (category_name) DO NOTHING
                    """,
                    (
                        h_category_pk,
                        category_name,
                        load_dt,
                        load_src
                    )
                )

    def h_product_insert(self,
                            h_product_pk: uuid.UUID,
                            product_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (product_id) DO NOTHING
                    """,
                    (
                        h_product_pk,
                        product_id,
                        load_dt,
                        load_src
                    )
                )

    def l_order_product_insert(self,
                            hk_order_product_pk: uuid.UUID,
                            h_order_pk: uuid.UUID,
                            h_product_pk: uuid.UUID,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                )

    def l_product_restaurant_insert(self,
                            hk_product_restaurant_pk: uuid.UUID,
                            h_restaurant_pk: uuid.UUID,
                            h_product_pk: uuid.UUID,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        hk_product_restaurant_pk,
                        h_restaurant_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                )

    def l_order_user_insert(self,
                            hk_order_user_pk: uuid.UUID,
                            h_order_pk: uuid.UUID,
                            h_user_pk: uuid.UUID,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        hk_order_user_pk,
                        h_order_pk,
                        h_user_pk,
                        load_dt,
                        load_src
                    )
                )

    def l_product_category_insert(self,
                            hk_product_category_pk: uuid.UUID,
                            h_category_pk: uuid.UUID,
                            h_product_pk: uuid.UUID,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        hk_product_category_pk,
                        h_category_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                )

    def s_order_cost_insert(self,
                            hk_order_cost_hashdiff: uuid.UUID,
                            h_order_pk: uuid.UUID,
                            cost: float,
                            payment: float,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost (hk_order_cost_hashdiff, h_order_pk, cost, payment, load_dt, load_src) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        hk_order_cost_hashdiff,
                        h_order_pk,
                        cost,
                        payment,
                        load_dt,
                        load_src
                    )
                )

    def s_order_status_insert(self,
                            hk_order_status_hashdiff: uuid.UUID,
                            h_order_pk: uuid.UUID,
                            status: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status (hk_order_status_hashdiff, h_order_pk, status, load_dt, load_src) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        hk_order_status_hashdiff,
                        h_order_pk,
                        status,
                        load_dt,
                        load_src
                    )
                )

    def s_product_names_insert(self,
                            hk_product_names_hashdiff: uuid.UUID,
                            h_product_pk: uuid.UUID,
                            name: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names (hk_product_names_hashdiff, h_product_pk, name, load_dt, load_src) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        hk_product_names_hashdiff,
                        h_product_pk,
                        name,
                        load_dt,
                        load_src
                    )
                )

    def s_restaurant_names_insert(self,
                            hk_restaurant_names_hashdiff: uuid.UUID,
                            h_restaurant_pk: uuid.UUID,
                            name: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names (hk_restaurant_names_hashdiff, h_restaurant_pk, name, load_dt, load_src) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        hk_restaurant_names_hashdiff,
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src
                    )
                )

    def s_user_names_insert(self,
                            hk_user_names_hashdiff: uuid.UUID,
                            h_user_pk: uuid.UUID,
                            username: str,
                            userlogin: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names (hk_user_names_hashdiff, h_user_pk, username, userlogin, load_dt, load_src) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """,
                    (
                        hk_user_names_hashdiff,
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src
                    )
                )
