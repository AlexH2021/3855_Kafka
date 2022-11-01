from sqlalchemy import Column, Integer, String, DateTime
from base import Base
from datetime import datetime


class Trade(Base):
    """ Trade """
    __tablename__ = "trade"
    __table_args__ = {'mysql_engine':'InnoDB'}

    tradeID = Column(Integer, primary_key=True)
    tradeType = Column(String(250))
    symbol = Column(String(250))
    shares = Column(Integer)
    price = Column(Integer)
    createdAt = Column(DateTime)
    accountID = Column(Integer)
    traceID = Column(String(250))

    def __init__(self, tradeID, tradeType, symbol, shares, price, createdAt, accountID, traceID):
        """ Initializes a trading session """
        self.tradeID = tradeID
        self.tradeType = tradeType
        self.symbol = symbol
        self.shares = shares
        self.price = price
        self.createdAt =  createdAt
        self.accountID = accountID
        self.traceID = traceID

    def to_dict(self):
        """ Dictionary Representation of a trading session """
        dict = {}
        dict['tradeID'] = self.tradeID
        dict['tradeType'] = self.tradeType
        dict['symbol'] = self.symbol
        dict['shares'] = self.shares
        dict['price'] = self.price
        dict['createdAt'] = self.createdAt
        dict['accountID'] = self.accountID
        dict['traceID'] = self.traceID

        return dict
