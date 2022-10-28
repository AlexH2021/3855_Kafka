from sqlalchemy import Column, Float, Integer, String
from base import Base


class Trade_Stats(Base):
    """ Account """
    __tablename__ = "trade_stats"

    id = Column(Integer, primary_key=True)
    total_trade_num = Column(Integer)
    total_share = Column(Float)
    total_price = Column(Float)
    traceID = Column(String(250), nullable=True)

    def __init__(self, total_trade_num, share, price, traceID=0):
        """ Initializes an account """
        self.total_trade_num = total_trade_num
        self.total_share = share
        self.total_price = price
        self.traceID = traceID