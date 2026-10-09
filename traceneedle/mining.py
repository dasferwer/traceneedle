import re
from typing import cast

from drain3 import TemplateMiner
from drain3.persistence_handler import PersistenceHandler
from drain3.template_miner_config import TemplateMinerConfig
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import HashingVectorizer

VECTORIZER = HashingVectorizer(
    n_features=64, analyzer="char_wb", ngram_range=(3, 5), alternate_sign=False
)


def normalized(message):
    text = re.sub(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F-]{27,}\b", "<id>", message)
    text = re.sub(r"\b((?:request|user|job|connection)(?:_id)?[=:])\d+\b", r"\1<n>", text)
    return re.sub(r"\b\d+(?:\.\d+)?(?:ms|s)\b", "<duration>", text)


def embedding(template):
    return str(cast(csr_matrix, VECTORIZER.transform([template])).toarray()[0].tolist())


class State(PersistenceHandler):
    def __init__(self, conn):
        self.conn = conn

    # Drain3 выводит None из пустого метода базы; фактический контракт — bytes | None.
    def load_state(self):  # pyright: ignore[reportIncompatibleMethodOverride]
        row = self.conn.execute("SELECT state FROM miner_state WHERE id=1").fetchone()
        return bytes(row["state"]) if row else None

    def save_state(self, state):
        self.conn.execute(
            "INSERT INTO miner_state VALUES (1,%s) ON CONFLICT(id) DO UPDATE SET state=EXCLUDED.state",
            (state,),
        )


def miner(conn):
    config = TemplateMinerConfig()
    config.drain_sim_th = 0.85
    config.drain_max_clusters = 1000
    config.profiling_enabled = False
    return TemplateMiner(State(conn), config=config)
