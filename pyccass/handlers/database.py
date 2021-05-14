from datetime import datetime
from typing import List, Optional
import sqlite3

import pandas as pd


class DBHandler:
    def __init__(self, address: str):
        self.con: sqlite3.Connection = sqlite3.connect(address)
        try:
            self._create_table()
        except:
            pass

    def __del__(self):
        self.con.close()

    def _create_table(self):
        con = self.con
        con.execute("""
        CREATE TABLE ccass_holding
        (date integer, code text, custodian_code text, custodian text, holding integer, pct_holding real);
        """)

        con.execute("""
        CREATE INDEX date_idx
        ON ccass_holding (date)
        """)

        con.commit()

    def insert_many(self, data: List[tuple]):
        con = self.con
        con.executemany(
            """INSERT INTO ccass_holding VALUES (?,?,?,?,?,?)""",
            data
        )
        con.commit()

    def query_by_date(self, date: datetime) -> pd.DataFrame:
        timestamp = date.timestamp()
        query_string = f"""
        SELECT *
        FROM ccass_holding
        WHERE date={timestamp}
        """

        df = pd.read_sql(query_string, self.con)
        # Format data
        timezone = datetime.now() - datetime.utcnow()
        df['date'] = pd.to_datetime(df['date'], unit='s') + timezone

        return df

    def query_max_date(self) -> Optional[datetime]:
        cur = self.con.execute("""
        SELECT MAX(date)
        FROM ccass_holding
        """)
        timestamp = list(cur)[0][0]
        try:
            date = datetime.fromtimestamp(timestamp)
            return date
        except:
            return None

    def query_min_date(self) -> Optional[datetime]:
        cur = self.con.execute("""
        SELECT Min(date)
        FROM ccass_holding
        """)
        timestamp = list(cur)[0][0]
        try:
            date = datetime.fromtimestamp(timestamp)
            return date
        except:
            return None
